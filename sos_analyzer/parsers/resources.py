"""
sos_analyzer/parsers/resources.py — CPU, memory, disk usage
"""
from __future__ import annotations
from pathlib import Path
from ..common import read_file, read_lines, write_json, flag_disk, flag_mem


def parse(sos_root: Path, out_dir: Path) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)

    # ── Memory ──
    mem_total_kb = mem_free_kb = mem_avail_kb = 0
    swap_total_kb = swap_free_kb = 0
    for line in read_lines(sos_root / "proc" / "meminfo"):
        k, _, v = line.partition(":")
        v = v.strip().split()[0] if v.strip() else "0"
        try:
            val = int(v)
        except ValueError:
            continue
        if k == "MemTotal":     mem_total_kb  = val
        elif k == "MemFree":    mem_free_kb   = val
        elif k == "MemAvailable": mem_avail_kb = val
        elif k == "SwapTotal":  swap_total_kb = val
        elif k == "SwapFree":   swap_free_kb  = val

    mem_used_kb   = mem_total_kb - mem_avail_kb if mem_avail_kb else mem_total_kb - mem_free_kb
    mem_used_pct  = round(mem_used_kb * 100 / mem_total_kb) if mem_total_kb else 0
    mem_total_gb  = round(mem_total_kb / 1048576, 1)
    mem_used_gb   = round(mem_used_kb  / 1048576, 1)
    swap_used_kb  = swap_total_kb - swap_free_kb
    swap_used_pct = round(swap_used_kb * 100 / swap_total_kb) if swap_total_kb else 0
    mem_flag      = flag_mem(mem_used_pct)

    # ── CPU from SAR ──
    cpu_usr = cpu_sys = cpu_iowait = cpu_idle = "N/A"
    sar_dir = sos_root / "sos_commands" / "sar"
    if sar_dir.exists():
        sar_files = sorted(
            [f for f in sar_dir.iterdir()
             if f.name.startswith("sar") and not f.name.endswith(".xml")],
            key=lambda f: f.name
        )
        if sar_files:
            for line in read_lines(sar_files[-1]):
                if line.startswith("Average:") and " all " in line:
                    parts = line.split()
                    if len(parts) >= 8:
                        cpu_usr    = parts[2]
                        cpu_sys    = parts[4]
                        cpu_iowait = parts[5]
                        cpu_idle   = parts[-1]
                    break

    # ── Disk ──
    # Read cpu_count for load normalization (best effort)
    cpu_count_val = sum(
        1 for l in read_lines(sos_root / "proc" / "cpuinfo")
        if l.startswith("processor")
    ) or 1

    # Normalize SAR values if summed across cores
    def maybe_normalize(val: str) -> str:
        try:
            v = float(val)
            if v > 100:
                return f"{v / cpu_count_val:.2f}"
        except (ValueError, TypeError):
            pass
        return val

    if cpu_usr != "N/A":
        cpu_usr    = maybe_normalize(cpu_usr)
        cpu_sys    = maybe_normalize(cpu_sys)
        cpu_iowait = maybe_normalize(cpu_iowait)
        cpu_idle   = maybe_normalize(cpu_idle)

    # ── Disk from df ──
    disks = []
    disk_txt = ""
    for line in read_lines(sos_root / "df"):
        if line.startswith("Filesystem") or line.startswith("tmpfs") or line.startswith("devtmpfs"):
            continue
        parts = line.split()
        if len(parts) < 6:
            continue
        fs     = parts[0]
        blocks = parts[1]
        used   = parts[2]
        avail  = parts[3]
        pct_s  = parts[4].replace("%", "")
        mount  = parts[5]

        # Skip pseudo / very small filesystems
        try:
            b = int(blocks)
        except ValueError:
            continue
        if b < 100000:
            continue

        try:
            pct = int(pct_s)
        except ValueError:
            pct = 0

        # Skip Lustre mounts — covered by lustre parser
        if mount.startswith("/lustre/"):
            # Keep them — they're useful for disk usage display
            pass

        try:
            used_gb  = round(int(used)  / 1048576, 1)
            avail_gb = round(int(avail) / 1048576, 1)
            total_gb = round((int(used) + int(avail)) / 1048576, 1)
        except ValueError:
            used_gb = avail_gb = total_gb = 0.0

        d_flag = flag_disk(pct)
        disks.append({
            "filesystem": fs,
            "mount":      mount,
            "total_gb":   total_gb,
            "used_gb":    used_gb,
            "avail_gb":   avail_gb,
            "used_pct":   pct,
            "flag":       d_flag,
        })
        disk_txt += f"  {mount:<45} {pct:>4}% ({used_gb}/{total_gb} GB) [{d_flag}]\n"

    result = {
        "memory": {
            "total_gb":      mem_total_gb,
            "used_gb":       mem_used_gb,
            "used_pct":      mem_used_pct,
            "swap_total_kb": swap_total_kb,
            "swap_used_kb":  swap_used_kb,
            "swap_used_pct": swap_used_pct,
            "flag":          mem_flag,
        },
        "cpu": {
            "count":      str(cpu_count_val),
            "usr_pct":    cpu_usr,
            "sys_pct":    cpu_sys,
            "iowait_pct": cpu_iowait,
            "idle_pct":   cpu_idle,
        },
        "disk": disks,
    }

    write_json(out_dir / "resources.json", result)

    (out_dir / "resources.txt").write_text(
        f"=== RESOURCES ===\n\n"
        f"-- Memory --\n"
        f"  Total:     {mem_total_gb} GB\n"
        f"  Used:      {mem_used_gb} GB ({mem_used_pct}%) [{mem_flag}]\n"
        f"  Swap Used: {swap_used_pct}%\n\n"
        f"-- CPU ({cpu_count_val} cores, SAR daily avg) --\n"
        f"  User:     {cpu_usr}%\n"
        f"  System:   {cpu_sys}%\n"
        f"  I/O Wait: {cpu_iowait}%\n"
        f"  Idle:     {cpu_idle}%\n\n"
        f"-- Disk --\n{disk_txt or '  (no data)'}\n"
    )

    return result
