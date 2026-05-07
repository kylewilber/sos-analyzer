"""
sos_analyzer/aggregate.py — merge per-node JSON into cluster_diff.json
"""
from __future__ import annotations
from datetime import datetime, timezone
from pathlib import Path
from .common import load_json, write_json, worst_flag


def aggregate(nodes_dir: Path, out_dir: Path) -> dict:
    """
    Read all per-node JSON files from nodes_dir and produce:
      - cluster_diff.json
      - cluster_summary.txt
    Returns the cluster_diff dict.
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    node_dirs = sorted(
        [d for d in nodes_dir.iterdir() if d.is_dir() and (d / "identity.json").exists()]
    )

    nodes = []
    for d in node_dirs:
        ident   = load_json(d / "identity.json")
        res     = load_json(d / "resources.json")
        svc     = load_json(d / "services.json")
        net     = load_json(d / "network.json")
        log     = load_json(d / "logs.json")
        lus     = load_json(d / "lustre.json")
        sfa     = load_json(d / "sfa.json")
        sysctl  = load_json(d / "sysctl.json")
        exa     = load_json(d / "exascaler.json")

        mem = res.get("memory", {})
        cpu = res.get("cpu", {})

        # Overall flag
        flags = [
            mem.get("flag", "OK"),
            svc.get("flag", "OK"),
            log.get("flag", "OK"),
            lus.get("flag", "OK"),
            sfa.get("flag", "OK") if sfa.get("available") else "OK",
            sysctl.get("flag", "OK") if sysctl.get("available") else "OK",
            exa.get("flag", "OK") if exa.get("available") else "OK",
        ]
        overall = worst_flag(*flags)

        node = {
            "hostname":        ident.get("hostname", d.name),
            "os":              f"{ident.get('os_name','')} {ident.get('os_version','')}".strip(),
            "kernel":          ident.get("kernel", ""),
            "cpu_count":       ident.get("cpu_count", ""),
            "uptime_days":     ident.get("uptime_days", 0),
            "collection_date": ident.get("collection_date", ""),

            "mem_used_pct":    mem.get("used_pct", 0),
            "mem_flag":        mem.get("flag", "OK"),
            "cpu_idle_pct":    cpu.get("idle_pct", "0.00"),

            "failed_services": svc.get("failed_count", 0),
            "services_flag":   svc.get("flag", "OK"),

            "log_critical":    log.get("critical_count", log.get("log_critical", 0)),
            "log_warnings":    log.get("warning_count",  log.get("log_warnings", 0)),
            "log_flag":        log.get("flag", "OK"),

            "lustre_flag":     lus.get("flag", "OK"),
            "ost_count":       lus.get("ost_count", 0),
            "ost_critical":    lus.get("ost_critical", 0),
            "devices_down":    lus.get("devices_down", 0),

            "sfa_flag":            sfa.get("flag", "OK") if sfa.get("available") else "N/A",
            "sfa_pool_not_optimal": sfa.get("pool_not_optimal", 0),
            "sfa_tz_inconsistent":  sfa.get("tz_inconsistent", 0),
            "sfa_ib_fw_flag":       sfa.get("ib_fw_flag", 0),

            "sysctl_flag":         sysctl.get("flag", "N/A") if sysctl.get("available") else "N/A",
            "sysctl_drift_count":  sysctl.get("drift_count", 0),
            "exa_flag":            exa.get("flag", "N/A") if exa.get("available") else "N/A",
            "exa_drift_count":     exa.get("drift_count", 0),
            "exa_version":         exa.get("version"),
            "exa_filesystems":     exa.get("filesystems", []),
            "exa_param_drift":     exa.get("param_drift", []),
            "exa_ha_groups":      exa.get("ha_groups", []),
            "exa_sfa_names":      exa.get("sfa_names", []),

            "overall_flag":    overall,
        }
        nodes.append(node)

    cluster_diff = {
        "cluster_summary": {
            "node_count": len(nodes),
            "generated":  datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
        "nodes": nodes,
    }

    write_json(out_dir / "cluster_diff.json", cluster_diff)
    _write_summary(nodes, out_dir / "cluster_summary.txt")

    return cluster_diff


def _write_summary(nodes: list[dict], path: Path) -> None:
    w = 76
    lines = [
        "╔" + "═" * w + "╗\n",
        f"║{'CLUSTER SOS REPORT SUMMARY':^{w}}║\n",
        "╠" + "═" * w + "╣\n",
        f"║  Nodes: {len(nodes):<5}  Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC'):<{w-20}}║\n",
        "╠═══════════════╦════════╦═══════════╦══════════╦══════════╦══════════════╣\n",
        f"║ {'HOSTNAME':<13} ║ {'MEM%':<6} ║ {'FAIL SVC':<9} ║ {'LOG':<8} ║ {'LUSTRE':<8} ║ {'STATUS':<12} ║\n",
        "╠═══════════════╬════════╬═══════════╬══════════╬══════════╬══════════════╣\n",
    ]
    for n in nodes:
        lines.append(
            f"║ {n['hostname'][:13]:<13} ║ {str(n['mem_used_pct'])+'%':<6} ║ "
            f"{str(n['failed_services']):<9} ║ {n['log_flag']:<8} ║ "
            f"{n['lustre_flag']:<8} ║ {n['overall_flag']:<12} ║\n"
        )
    lines.append("╚═══════════════╩════════╩═══════════╩══════════╩══════════╩══════════════╝\n\n")

    crit = [n["hostname"] for n in nodes if n["overall_flag"] == "CRITICAL"]
    warn = [n["hostname"] for n in nodes if n["overall_flag"] == "WARNING"]
    if crit:
        lines.append(f"!! CRITICAL NODES: {', '.join(crit)}\n")
    if warn:
        lines.append(f"!  WARNING NODES:  {', '.join(warn)}\n")
    if not crit and not warn:
        lines.append("   All nodes: OK\n")

    path.write_text("".join(lines))
