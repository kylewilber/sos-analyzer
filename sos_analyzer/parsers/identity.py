"""
sos_analyzer/parsers/identity.py — node identity extraction
"""
from __future__ import annotations
import re
from pathlib import Path
from ..common import read_file, read_lines, write_json, hostname_from_sos


def parse(sos_root: Path, out_dir: Path) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)

    # ── Hostname ──
    hostname = hostname_from_sos(sos_root)

    # ── OS release ──
    os_name = os_version = os_id = ""
    for line in read_lines(sos_root / "etc" / "os-release"):
        if line.startswith("NAME="):
            os_name = line.split("=", 1)[1].strip('"')
        elif line.startswith("VERSION="):
            os_version = line.split("=", 1)[1].strip('"')
        elif line.startswith("ID="):
            os_id = line.split("=", 1)[1].strip('"')
    if not os_name:
        os_name = read_file(sos_root / "etc" / "redhat-release") or "unknown"

    # ── Kernel / arch ──
    uname_str = read_file(sos_root / "uname")
    parts  = uname_str.split()
    kernel = parts[2] if len(parts) > 2 else ""
    arch   = parts[-1] if parts else ""

    # ── CPU count ──
    cpu_count: str | int = sum(
        1 for l in read_lines(sos_root / "proc" / "cpuinfo")
        if l.startswith("processor")
    )
    if not cpu_count:
        dmi = sos_root / "sos_commands" / "hardware" / "dmidecode"
        if dmi.exists():
            dmi_text = read_file(dmi)
            m_threads = re.search(r'Thread Count:\s*(\d+)', dmi_text)
            m_sockets = len(re.findall(r'Socket Designation:.*CPU', dmi_text))
            if m_threads:
                cpu_count = int(m_threads.group(1)) * max(m_sockets, 1)
    if not cpu_count:
        cpu_count = "unknown"

    # ── Uptime / load ──
    uptime_raw  = read_file(sos_root / "uptime")
    uptime_days = 0
    load_avg    = ""
    if uptime_raw:
        m = re.search(r'(\d+)\s+days?', uptime_raw)
        if m:
            uptime_days = int(m.group(1))
        m = re.search(r'load average[s]?:\s*([\d.,\s]+)', uptime_raw)
        if m:
            load_avg = m.group(1).strip().rstrip(",")

    # ── Collection date ──
    date_text = read_file(sos_root / "date")
    collect_date = ""
    for line in date_text.splitlines():
        if "Universal time:" in line:
            parts = line.split(":", 1)
            collect_date = parts[1].strip() if len(parts) > 1 else ""
            break
    if not collect_date:
        collect_date = date_text.splitlines()[0] if date_text else ""

    # ── SOS version ──
    sos_version = ""
    ver_text = read_file(sos_root / "version.txt")
    m = re.search(r'([\d.]+)', ver_text)
    if m:
        sos_version = m.group(1)

    result = {
        "hostname":        hostname,
        "os_name":         os_name,
        "os_version":      os_version,
        "os_id":           os_id,
        "kernel":          kernel,
        "arch":            arch,
        "cpu_count":       str(cpu_count),
        "uptime_days":     uptime_days,
        "load_average":    load_avg,
        "collection_date": collect_date,
        "sos_version":     sos_version,
    }

    write_json(out_dir / "identity.json", result)

    (out_dir / "identity.txt").write_text(
        f"=== NODE IDENTITY: {hostname} ===\n"
        f"OS:              {os_name} {os_version}\n"
        f"Kernel:          {kernel} ({arch})\n"
        f"CPUs:            {cpu_count}\n"
        f"Uptime:          {uptime_days} days\n"
        f"Load Average:    {load_avg}\n"
        f"Collected:       {collect_date}\n"
        f"SOS Version:     {sos_version}\n"
    )

    return result
