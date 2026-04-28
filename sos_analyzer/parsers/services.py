"""
sos_analyzer/parsers/services.py — systemd service status
"""
from __future__ import annotations
import re
from pathlib import Path
from ..common import read_lines, write_json


def _load_ignore_patterns(conf_dir: Path) -> list[str]:
    ignore_file = conf_dir / "ignore_services.txt"
    patterns = []
    if ignore_file.exists():
        for line in read_lines(ignore_file):
            if not line.startswith("#"):
                patterns.append(line.strip())
    return patterns


def _is_ignored(unit: str, patterns: list[str]) -> bool:
    return any(pat in unit for pat in patterns)


def parse(sos_root: Path, out_dir: Path, conf_dir: Path | None = None) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)

    ignore_patterns = _load_ignore_patterns(conf_dir) if conf_dir else []

    systemd_dir = sos_root / "sos_commands" / "systemd"

    # ── Failed units ──
    failed_units  = []
    ignored_units = []

    failed_file = systemd_dir / "systemctl_list-units_--failed"
    if failed_file.exists():
        for line in read_lines(failed_file):
            # Lines start with ● (or just the unit name after filtering)
            if not (line.startswith("●") or re.match(r'\s*\S+\.service', line)):
                continue
            # Normalize: remove leading ●
            line = line.lstrip("●").strip()
            parts = line.split()
            if len(parts) < 4:
                continue
            unit  = parts[0]
            load  = parts[1]
            active = parts[2]
            sub   = parts[3]
            desc  = " ".join(parts[4:])
            entry = {
                "unit":        unit,
                "load":        load,
                "active":      active,
                "sub":         sub,
                "description": desc,
            }
            if _is_ignored(unit, ignore_patterns):
                ignored_units.append(entry)
            else:
                failed_units.append(entry)

    # ── Unit file counts ──
    unit_files_path = systemd_dir / "systemctl_list-unit-files"
    enabled_count  = 0
    disabled_count = 0
    total_unit_files = 0
    if unit_files_path.exists():
        for line in read_lines(unit_files_path):
            if ".service" in line:
                total_unit_files += 1
                if " enabled" in line:
                    enabled_count += 1
                elif " disabled" in line:
                    disabled_count += 1

    svc_flag = "WARNING" if failed_units else "OK"

    result = {
        "failed_count":    len(failed_units),
        "ignored_count":   len(ignored_units),
        "enabled_count":   enabled_count,
        "disabled_count":  disabled_count,
        "total_unit_files": total_unit_files,
        "flag":            svc_flag,
        "failed_units":    failed_units,
        "ignored_units":   ignored_units,
    }

    write_json(out_dir / "services.json", result)

    failed_txt  = "\n".join(f"  {u['unit']:<55} {u['active']}/{u['sub']}" for u in failed_units)  or "  (none)"
    ignored_txt = "\n".join(f"  {u['unit']:<55} {u['active']}/{u['sub']}" for u in ignored_units) or "  (none)"

    (out_dir / "services.txt").write_text(
        f"=== SERVICES ===\n"
        f"  Failed Units:    {len(failed_units)}  [{svc_flag}]\n"
        f"  Ignored Units:   {len(ignored_units)}\n"
        f"  Enabled:         {enabled_count}\n"
        f"  Disabled:        {disabled_count}\n"
        f"  Total .service:  {total_unit_files}\n\n"
        f"-- Failed Units --\n{failed_txt}\n\n"
        f"-- Ignored Failed Units --\n{ignored_txt}\n"
    )

    return result
