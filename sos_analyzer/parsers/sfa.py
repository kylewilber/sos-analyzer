"""
sos_analyzer/parsers/sfa.py — DDN SFA enclosure health parser
"""
from __future__ import annotations
import re
from pathlib import Path
from ..common import read_lines, write_json


def _sfa_rows(path: Path) -> list[str]:
    """Return data rows from a Jira-style table (skip header/separator rows)."""
    rows = []
    for line in read_lines(path):
        if line.startswith("|") and not line.startswith("||") and "---" not in line:
            rows.append(line)
    return rows


def _col(line: str, idx: int) -> str:
    """Extract column idx (1-based) from a pipe-delimited table row."""
    parts = [p.strip() for p in line.split("|")]
    # parts[0] is empty (before first |), parts[1] is col 1, etc.
    if idx + 1 < len(parts):
        return parts[idx]
    return ""


def parse(sos_root: Path, out_dir: Path) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)

    sfa_dir = sos_root / "sos_commands" / "sfa"
    if not sfa_dir.exists():
        result = {"available": False}
        write_json(out_dir / "sfa.json", result)
        (out_dir / "sfa.txt").write_text("=== SFA HARDWARE ===\n\nNot available.\n")
        return result

    sfa_flag = "OK"

    # ── Subsystem list ──
    subsystems: list[dict] = []
    tz_flag = 0
    sub_file = sfa_dir / "emf_sfa_subsystem_list_--table-style_jira"
    if sub_file.exists():
        for line in _sfa_rows(sub_file):
            name     = _col(line, 3)
            platform = _col(line, 4)
            health   = _col(line, 5)
            tz       = _col(line, 8)
            if not name:
                continue
            subsystems.append({
                "name":     name,
                "platform": platform,
                "health":   health,
                "timezone": tz,
            })
        tzs = [s["timezone"] for s in subsystems if s["timezone"]]
        if len(set(tzs)) > 1:
            tz_flag  = 1
            sfa_flag = "WARNING"

    # ── Pool list ──
    pool_not_optimal = 0
    pool_issues      = ""
    pool_file = sfa_dir / "emf_sfa_pool_list_--table-style_jira"
    if pool_file.exists():
        for line in _sfa_rows(pool_file):
            name  = _col(line, 3)
            state = _col(line, 4)
            if not name:
                continue
            if state and state != "Optimal":
                pool_not_optimal += 1
                pool_issues += f" {name}:{state}"
                sfa_flag = "CRITICAL"

    # ── IOC list — IB HCA firmware ──
    part_fw:       dict[str, str]  = {}
    part_mismatch: dict[str, str]  = {}
    ib_fw_flag = 0
    ib_summary = ""
    ioc_file = sfa_dir / "emf_sfa_ioc_list_--table-style_jira"
    if ioc_file.exists():
        for line in _sfa_rows(ioc_file):
            port_type = _col(line, 9)
            if port_type != "Infiniband":
                continue
            part = _col(line, 7)
            fw   = _col(line, 8)
            if not part or not fw:
                continue
            if part not in part_fw:
                part_fw[part] = fw
            elif part_fw[part] != fw:
                part_mismatch[part] = f"expected:{part_fw[part]} got:{fw}"
                ib_fw_flag = 1
                if sfa_flag == "OK":
                    sfa_flag = "WARNING"

        ib_summary = " ".join(
            f"{part}={fw}" + (f"[MISMATCH:{part_mismatch[part]}]" if part in part_mismatch else "")
            for part, fw in sorted(part_fw.items())
        )

    result = {
        "available":        True,
        "flag":             sfa_flag,
        "tz_inconsistent":  tz_flag,
        "pool_not_optimal": pool_not_optimal,
        "pool_issues":      pool_issues.strip(),
        "ib_fw_flag":       ib_fw_flag,
        "ib_fw_summary":    ib_summary,
        "subsystems":       subsystems,
    }

    write_json(out_dir / "sfa.json", result)

    tz_str   = "consistent" if not tz_flag else "INCONSISTENT"
    pool_str = "all Optimal" if not pool_not_optimal else f"{pool_not_optimal} not optimal ({pool_issues.strip()})"
    fw_str   = "consistent" if not ib_fw_flag else "MISMATCH detected"

    (out_dir / "sfa.txt").write_text(
        f"=== SFA HARDWARE ===\n"
        f"Flag:              {sfa_flag}\n"
        f"Timezone:          {tz_str}\n"
        f"Pools:             {pool_str}\n"
        f"IB FW:             {fw_str}\n"
        f"IB FW summary:     {ib_summary}\n"
    )

    return result
