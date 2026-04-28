"""
sos_analyzer/parsers/sysctl.py — kernel sysctl parameter extraction and drift detection
"""
from __future__ import annotations
from pathlib import Path
from ..common import read_lines, write_json

# ─── Parameters to extract ────────────────────────────────────────────────────

PARAMS = [
    # VM / memory management
    "vm.dirty_ratio", "vm.dirty_background_ratio",
    "vm.dirty_bytes", "vm.dirty_background_bytes",
    "vm.dirty_expire_centisecs", "vm.dirty_writeback_centisecs",
    "vm.swappiness", "vm.overcommit_memory", "vm.overcommit_ratio",
    "vm.min_free_kbytes", "vm.vfs_cache_pressure",
    "vm.zone_reclaim_mode", "vm.numa_stat",
    "vm.nr_hugepages", "vm.max_map_count", "vm.panic_on_oom",
    # Network core
    "net.core.rmem_max", "net.core.wmem_max",
    "net.core.rmem_default", "net.core.wmem_default",
    "net.core.netdev_max_backlog", "net.core.somaxconn", "net.core.optmem_max",
    # TCP
    "net.ipv4.tcp_rmem", "net.ipv4.tcp_wmem", "net.ipv4.tcp_mem",
    "net.ipv4.tcp_congestion_control", "net.ipv4.tcp_timestamps",
    "net.ipv4.tcp_sack", "net.ipv4.tcp_low_latency",
    "net.ipv4.tcp_slow_start_after_idle", "net.ipv4.tcp_syncookies",
    "net.ipv4.tcp_fin_timeout", "net.ipv4.tcp_keepalive_time",
    "net.ipv4.tcp_keepalive_intvl", "net.ipv4.tcp_keepalive_probes",
    "net.ipv4.tcp_max_syn_backlog", "net.ipv4.udp_mem",
    # Interface security
    "net.ipv4.conf.all.accept_redirects", "net.ipv4.conf.all.secure_redirects",
    "net.ipv4.conf.all.send_redirects",
    "net.ipv4.conf.default.accept_redirects", "net.ipv4.conf.default.secure_redirects",
    "net.ipv4.conf.mgmt0.accept_redirects", "net.ipv4.conf.mgmt0.secure_redirects",
    "net.ipv4.conf.mlxib0.accept_redirects", "net.ipv4.conf.mlxib0.secure_redirects",
    "net.ipv4.conf.mlxib1.accept_redirects", "net.ipv4.conf.mlxib1.secure_redirects",
    # Kernel
    "kernel.hung_task_timeout_secs", "kernel.hung_task_warnings",
    "kernel.numa_balancing", "kernel.nmi_watchdog",
    "kernel.panic", "kernel.panic_on_oops",
    "kernel.pid_max", "kernel.threads-max",
    "kernel.sysrq", "kernel.perf_event_paranoid",
]

# ─── Recommended values for Lustre OSS/MDS nodes ─────────────────────────────

RECOMMENDED: dict[str, str] = {
    "vm.swappiness":                          "10",
    "vm.zone_reclaim_mode":                   "0",
    "vm.numa_stat":                           "1",
    "kernel.numa_balancing":                  "0",
    "net.ipv4.tcp_timestamps":                "0",
    "net.ipv4.tcp_low_latency":               "1",
    "net.ipv4.tcp_slow_start_after_idle":     "0",
    "net.ipv4.conf.mgmt0.accept_redirects":   "0",
    "net.ipv4.conf.mgmt0.secure_redirects":   "0",
    "net.ipv4.conf.mlxib0.accept_redirects":  "0",
    "net.ipv4.conf.mlxib0.secure_redirects":  "0",
    "net.ipv4.conf.mlxib1.accept_redirects":  "0",
    "net.ipv4.conf.mlxib1.secure_redirects":  "0",
    "kernel.hung_task_warnings":              "10",
}

SECURITY_PARAMS = {k for k in RECOMMENDED if "redirect" in k}


def parse(sos_root: Path, out_dir: Path) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)

    sysctl_file = sos_root / "sos_commands" / "kernel" / "sysctl_-a"
    if not sysctl_file.exists():
        result = {"available": False, "flag": "UNKNOWN", "params": {}, "drift_flags": [], "drift_count": 0}
        write_json(out_dir / "sysctl.json", result)
        (out_dir / "sysctl.txt").write_text("=== SYSCTL ===\n\nNot available.\n")
        return result

    # ── Extract values ──
    raw: dict[str, str] = {}
    for line in read_lines(sysctl_file):
        if " = " in line:
            key, _, val = line.partition(" = ")
            raw[key.strip()] = val.strip()

    values = {p: raw.get(p, "NOT_FOUND") for p in PARAMS}

    # ── Flag drift ──
    drift_flags = []
    for param, rec in RECOMMENDED.items():
        actual = values.get(param, "NOT_FOUND")
        if actual == "NOT_FOUND":
            continue
        if actual != rec:
            drift_flags.append({
                "param":       param,
                "actual":      actual,
                "recommended": rec,
            })

    # ── Overall flag ──
    overall = "OK"
    if drift_flags:
        has_security = any(d["param"] in SECURITY_PARAMS for d in drift_flags)
        overall = "WARNING" if has_security else "INFO"

    result = {
        "available":    True,
        "flag":         overall,
        "drift_count":  len(drift_flags),
        "params":       values,
        "drift_flags":  drift_flags,
    }

    write_json(out_dir / "sysctl.json", result)

    # ── Text summary ──
    lines = [
        "=== SYSCTL ===\n",
        f"Overall flag: {overall}  |  Drift findings: {len(drift_flags)}\n",
        "\n-- Memory Management --\n",
    ]
    mem_params = [
        "vm.dirty_ratio", "vm.dirty_background_ratio", "vm.dirty_expire_centisecs",
        "vm.dirty_writeback_centisecs", "vm.swappiness", "vm.overcommit_memory",
        "vm.min_free_kbytes", "vm.vfs_cache_pressure", "vm.zone_reclaim_mode",
        "vm.nr_hugepages", "vm.panic_on_oom",
    ]
    for p in mem_params:
        val = values.get(p, "NOT_FOUND")
        rec = RECOMMENDED.get(p, "")
        marker = f"  ← recommended: {rec}" if rec and val != rec else ""
        lines.append(f"  {p:<45} = {val}{marker}\n")

    lines.append("\n-- Network Buffers --\n")
    for p in ["net.core.rmem_max", "net.core.wmem_max", "net.core.rmem_default",
              "net.core.wmem_default", "net.core.netdev_max_backlog", "net.core.somaxconn",
              "net.ipv4.tcp_rmem", "net.ipv4.tcp_wmem", "net.ipv4.tcp_congestion_control"]:
        lines.append(f"  {p:<45} = {values.get(p, 'NOT_FOUND')}\n")

    lines.append("\n-- TCP Tuning --\n")
    for p in ["net.ipv4.tcp_timestamps", "net.ipv4.tcp_low_latency",
              "net.ipv4.tcp_slow_start_after_idle", "net.ipv4.tcp_sack",
              "net.ipv4.tcp_fin_timeout", "net.ipv4.tcp_keepalive_time",
              "net.ipv4.tcp_syncookies"]:
        val = values.get(p, "NOT_FOUND")
        rec = RECOMMENDED.get(p, "")
        marker = f"  ← recommended: {rec}" if rec and val != rec else ""
        lines.append(f"  {p:<45} = {val}{marker}\n")

    lines.append("\n-- Kernel --\n")
    for p in ["kernel.hung_task_timeout_secs", "kernel.hung_task_warnings",
              "kernel.numa_balancing", "kernel.nmi_watchdog",
              "kernel.panic", "kernel.panic_on_oops",
              "kernel.pid_max", "kernel.threads-max"]:
        val = values.get(p, "NOT_FOUND")
        rec = RECOMMENDED.get(p, "")
        marker = f"  ← recommended: {rec}" if rec and val != rec else ""
        lines.append(f"  {p:<45} = {val}{marker}\n")

    if drift_flags:
        lines.append("\n-- Drift Findings --\n")
        for d in drift_flags:
            lines.append(f"  {d['param']:<45} actual={d['actual']:<15} recommended={d['recommended']}\n")

    (out_dir / "sysctl.txt").write_text("".join(lines))

    return result
