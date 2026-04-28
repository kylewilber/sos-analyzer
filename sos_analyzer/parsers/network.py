"""
sos_analyzer/parsers/network.py — IP interfaces, routes, InfiniBand
"""
from __future__ import annotations
import re
from pathlib import Path
from ..common import read_file, read_lines, write_json, hostname_from_sos


IB_ERROR_COUNTERS = {
    "SymbolErrorCounter", "LinkErrorRecoveryCounter", "LinkDownedCounter",
    "PortRcvErrors", "PortRcvRemotePhysicalErrors", "PortXmitDiscards",
    "LocalLinkIntegrityErrors", "ExcessiveBufferOverrunErrors",
    "PortRcvConstraintErrors", "PortXmitConstraintErrors",
    "VL15Dropped", "QP1Dropped",
}


def _parse_interfaces(sos_root: Path) -> list[dict]:
    ifaces = []
    current_iface = None
    for line in read_lines(sos_root / "ip_addr"):
        # New interface block: "2: eth0: <flags>"
        m = re.match(r'^\d+:\s+(\S+):', line)
        if m:
            current_iface = m.group(1).rstrip("@").split("@")[0]
            continue
        # inet line within current interface
        m = re.match(r'\s+inet\s+(\d+\.\d+\.\d+\.\d+/\d+).*scope\s+(\S+)', line)
        if m and current_iface and current_iface != "lo":
            ifaces.append({
                "interface": current_iface,
                "ip_cidr":   m.group(1),
                "scope":     m.group(2),
            })
    return ifaces


def _parse_ib_errors(sos_root: Path) -> dict[str, tuple[int, str]]:
    """Returns {ca: (total_errors, error_detail_string)}"""
    ib_dir = sos_root / "sos_commands" / "infiniband"
    errors: dict[str, tuple[int, str]] = {}

    if not ib_dir.exists():
        return errors

    for pq_file in ib_dir.glob("perfquery_-C_*_-P_*"):
        # Extract CA name: perfquery_-C_mlx5_0_-P_1 → mlx5_0
        m = re.match(r'perfquery_-C_(.+)_-P_\d+', pq_file.name)
        if not m:
            continue
        ca = m.group(1)
        total = 0
        details = []
        for line in read_lines(pq_file):
            m2 = re.match(r'^(\w+):\.*\s*(\d+)', line)
            if m2 and m2.group(1) in IB_ERROR_COUNTERS:
                val = int(m2.group(2))
                if val > 0:
                    total += val
                    details.append(f"{m2.group(1)}:{val}")
        errors[ca] = (total, ",".join(details))

    return errors


def _parse_ibstat(sos_root: Path) -> dict[str, dict]:
    """Parse ibstat for state, rate, firmware per CA."""
    result: dict[str, dict] = {}
    ibstat = sos_root / "sos_commands" / "infiniband" / "ibstat"
    if not ibstat.exists():
        return result

    current_ca = None
    data: dict = {}
    for line in read_lines(ibstat):
        m = re.match(r"^CA '(.+)'", line)
        if m:
            if current_ca and data:
                result[current_ca] = data
            current_ca = m.group(1)
            data = {}
            continue
        if current_ca:
            m = re.search(r'State:\s+(.+)', line)
            if m: data["state"] = m.group(1).strip()
            m = re.search(r'Rate:\s+(.+)', line)
            if m: data["rate"] = m.group(1).strip()
            m = re.search(r'Firmware version:\s+(.+)', line)
            if m: data["firmware"] = m.group(1).strip()

    if current_ca and data:
        result[current_ca] = data

    return result


def _parse_ibstatus(sos_root: Path) -> dict[str, str]:
    """Parse ibstatus for human-readable rate strings."""
    result: dict[str, str] = {}
    ibstatus = sos_root / "sos_commands" / "infiniband" / "ibstatus"
    if not ibstatus.exists():
        return result

    current_dev = None
    for line in read_lines(ibstatus):
        m = re.search(r"Infiniband device '(.+)'", line)
        if m:
            current_dev = m.group(1)
            continue
        if current_dev:
            m = re.search(r'rate:\s+(.+)', line)
            if m:
                result[current_dev] = m.group(1).strip().replace(" ", "")

    return result


def parse(sos_root: Path, out_dir: Path) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)

    hostname = hostname_from_sos(sos_root)

    # Interfaces
    interfaces = _parse_interfaces(sos_root)

    # Default gateway
    default_gw = ""
    for line in read_lines(sos_root / "ip_route"):
        if line.startswith("default") and "table" not in line:
            parts = line.split()
            idx = parts.index("via") + 1 if "via" in parts else -1
            if idx > 0:
                default_gw = parts[idx]
            break

    # DNS
    dns_servers = ",".join(
        line.split()[1] for line in read_lines(sos_root / "etc" / "resolv.conf")
        if line.startswith("nameserver")
    )

    # IB data
    ib_errors   = _parse_ib_errors(sos_root)
    ibstat_data = _parse_ibstat(sos_root)
    ibstatus    = _parse_ibstatus(sos_root)

    ib_ports = []
    ib_flag  = "OK"

    for ca, data in sorted(ibstat_data.items()):
        state    = data.get("state", "")
        firmware = data.get("firmware", "")
        rate_raw = data.get("rate", "")

        # Prefer ibstatus rate string (includes lane width)
        rate = ibstatus.get(ca, f"{rate_raw}Gb/sec" if rate_raw else "")
        # Normalize: "200Gb/sec(4XHDR)" style
        rate = rate.replace(" ", "")

        err_count, err_detail = ib_errors.get(ca, (0, ""))

        port_flag = "OK"
        if state != "Active":
            port_flag = "CRITICAL"
            ib_flag   = "CRITICAL"
        elif err_count > 0:
            port_flag = "WARNING"
            if ib_flag == "OK":
                ib_flag = "WARNING"

        ib_ports.append({
            "ca":           ca,
            "state":        state,
            "rate":         rate,
            "firmware":     firmware,
            "error_count":  err_count,
            "error_detail": err_detail,
            "flag":         port_flag,
        })

    result = {
        "hostname":        hostname,
        "default_gateway": default_gw,
        "dns_servers":     dns_servers,
        "ib_flag":         ib_flag,
        "interfaces":      interfaces,
        "infiniband":      ib_ports,
    }

    write_json(out_dir / "network.json", result)

    iface_txt = "\n".join(f"  {i['interface']:<12} {i['ip_cidr']}" for i in interfaces) or "  (none)"
    ib_txt = "\n".join(
        f"  {p['ca']:<10} State:{p['state']:<8} Rate:{p['rate']:<22} "
        f"FW:{p['firmware']:<14} Errors:{p['error_count']} [{p['flag']}]"
        + (f"\n             {p['error_detail']}" if p['error_detail'] else "")
        for p in ib_ports
    ) or "  (no ibstat data)"

    (out_dir / "network.txt").write_text(
        f"=== NETWORK ===\n"
        f"  Default GW:  {default_gw or 'N/A'}\n"
        f"  DNS:         {dns_servers or 'N/A'}\n"
        f"  IB Flag:     {ib_flag}\n\n"
        f"-- IP Interfaces --\n{iface_txt}\n\n"
        f"-- InfiniBand --\n{ib_txt}\n"
    )

    return result
