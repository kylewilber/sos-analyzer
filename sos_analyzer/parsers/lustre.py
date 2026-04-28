"""
sos_analyzer/parsers/lustre.py — Lustre OST/MDT status and devices
"""
from __future__ import annotations
from pathlib import Path
from ..common import read_lines, write_json, flag_disk


def parse(sos_root: Path, out_dir: Path) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)

    lustre_dir = sos_root / "sos_commands" / "lustre"

    # ── Device list from lctl ──
    dev_up = dev_down = 0
    ost_names: list[str] = []
    mdt_names: list[str] = []
    devices: list[dict]  = []

    device_file = lustre_dir / "lctl_device_list"
    if device_file.exists() and device_file.stat().st_size > 0:
        for line in read_lines(device_file):
            parts = line.split()
            if len(parts) < 4:
                continue
            state = parts[1]
            dtype = parts[2]
            name  = parts[3]
            devices.append({"name": name, "type": dtype, "state": state})
            if state == "UP":
                dev_up += 1
            else:
                dev_down += 1
            if dtype == "obdfilter":
                ost_names.append(name)
            elif dtype == "mdt":
                mdt_names.append(name)

    # ── Usage from lfs df ──
    usage: dict[str, dict] = {}
    lfs_df = lustre_dir / "lfs_df"
    if lfs_df.exists() and lfs_df.stat().st_size > 0:
        for line in read_lines(lfs_df):
            if line.startswith("UUID") or not line:
                continue
            parts = line.split()
            if len(parts) < 5:
                continue
            uuid     = parts[0]
            blocks   = parts[1]
            used     = parts[2]
            avail    = parts[3]
            pct_str  = parts[4].replace("%", "")
            try:
                pct      = int(pct_str)
                used_tb  = round(int(used)  / 1073741824, 2)
                avail_tb = round(int(avail) / 1073741824, 2)
                total_tb = round(int(blocks) / 1073741824, 2)
            except (ValueError, ZeroDivisionError):
                continue
            base = uuid.replace("_UUID", "")
            usage[base] = {
                "used_pct":  pct,
                "used_tb":   used_tb,
                "avail_tb":  avail_tb,
                "total_tb":  total_tb,
                "flag":      flag_disk(pct),
            }

    # ── Build OST array ──
    osts: list[dict] = []
    ost_crit = ost_warn = 0
    for name in ost_names:
        u = usage.get(name, {"used_pct": 0, "used_tb": 0.0, "avail_tb": 0.0, "total_tb": 0.0, "flag": "OK"})
        osts.append({
            "uuid":     f"{name}_UUID",
            "total_tb": u["total_tb"],
            "used_tb":  u["used_tb"],
            "avail_tb": u["avail_tb"],
            "used_pct": u["used_pct"],
            "flag":     u["flag"],
        })
        if u["flag"] == "CRITICAL": ost_crit += 1
        elif u["flag"] == "WARNING": ost_warn += 1

    # ── Build MDT array ──
    mdts: list[dict] = []
    for name in mdt_names:
        u = usage.get(name, {"used_pct": 0, "used_tb": 0.0, "avail_tb": 0.0, "total_tb": 0.0, "flag": "OK"})
        mdts.append({
            "uuid":     f"{name}_UUID",
            "total_tb": u["total_tb"],
            "used_tb":  u["used_tb"],
            "avail_tb": u["avail_tb"],
            "used_pct": u["used_pct"],
            "flag":     u["flag"],
        })

    # ── Overall flag ──
    if dev_down > 0 or ost_crit > 0:
        lustre_flag = "CRITICAL"
    elif ost_warn > 0:
        lustre_flag = "WARNING"
    else:
        lustre_flag = "OK"

    result = {
        "flag":        lustre_flag,
        "ost_count":   len(osts),
        "mdt_count":   len(mdts),
        "ost_critical": ost_crit,
        "ost_warning":  ost_warn,
        "devices_up":   dev_up,
        "devices_down": dev_down,
        "osts":         osts,
        "mdts":         mdts,
        "devices":      devices,
    }

    write_json(out_dir / "lustre.json", result)

    ost_txt = "\n".join(
        f"  {o['uuid']:<40} {o['used_pct']:>4}% ({o['used_tb']}/{o['total_tb']} TB) [{o['flag']}]"
        for o in osts
    ) or "  (no OST data)"
    mdt_txt = "\n".join(
        f"  {m['uuid']:<40} {m['used_pct']:>4}% ({m['used_tb']}/{m['total_tb']} TB) [{m['flag']}]"
        for m in mdts
    ) or "  (no MDT data)"

    (out_dir / "lustre.txt").write_text(
        f"=== LUSTRE ===\n"
        f"  OSTs:    {len(osts)}  (Critical: {ost_crit}  Warning: {ost_warn})\n"
        f"  MDTs:    {len(mdts)}\n"
        f"  Devices: UP:{dev_up}  DOWN:{dev_down}\n"
        f"  Flag:    {lustre_flag}\n\n"
        f"-- OST Usage --\n{ost_txt}\n\n"
        f"-- MDT Usage --\n{mdt_txt}\n"
    )

    return result
