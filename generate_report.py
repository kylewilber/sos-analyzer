#!/usr/bin/env python3
"""
generate_report.py — Python-native HTML dashboard generator for SOS cluster reports.

Replaces: analyze_cluster.py + patch_visuals.py + patch_layout.py + patch_report.py

Usage:
    python3 generate_report.py <report_output_dir> [--no-llm] [--debug]

Example:
    python3 generate_report.py ~/work/sos-analyzer/reports/test99

Reads:
    <dir>/cluster/cluster_diff.json
    <dir>/nodes/<hostname>/*.json

Writes:
    <dir>/cluster/cluster_report_ai.html

Optional LLM narrative summary requires Ollama at http://172.16.0.252:11434
"""

import json
import math
import re
import sys
import urllib.request
import urllib.error
from collections import defaultdict
from datetime import datetime, timezone
from html import escape
from pathlib import Path
from statistics import median, stdev

# ─── Config ───────────────────────────────────────────────────────────────────

OLLAMA_URL  = "http://172.16.0.252:11434/api/generate"
MODEL       = "qwen3-coder:30b"
OUTPUT_FILE = "cluster_report_ai.html"

MAX_CRIT_EVENTS   = 5
MAX_CLIENT_EVENTS = 5
MAX_EVENT_LEN     = 150

# ─── Colors ───────────────────────────────────────────────────────────────────

C = {
    "bg":       "#0f1117",
    "card":     "#1a2236",
    "border":   "#253048",
    "text":     "#e2e8f0",
    "muted":    "#64748b",
    "critical": "#f87171",
    "warning":  "#fbbf24",
    "ok":       "#34d399",
    "info":     "#60a5fa",
    "track":    "#1e2d45",
}

def flag_color(flag: str) -> str:
    return {
        "CRITICAL": C["critical"],
        "WARNING":  C["warning"],
        "OK":       C["ok"],
        "INFO":     C["info"],
        "CLIENT_ISSUES": C["info"],
    }.get(str(flag).upper(), C["muted"])

# ─── Data loading ─────────────────────────────────────────────────────────────

def load_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def load_node(node_dir: Path) -> dict:
    ident   = load_json(node_dir / "identity.json")
    res     = load_json(node_dir / "resources.json")
    net     = load_json(node_dir / "network.json")
    logs    = load_json(node_dir / "logs.json")
    sfa     = load_json(node_dir / "sfa.json")
    sysctl  = load_json(node_dir / "sysctl.json")
    lustre  = load_json(node_dir / "lustre.json")

    mem   = res.get("memory", {})
    cpu   = res.get("cpu", {})

    disks = []
    for d in res.get("disk", []):
        fs = d.get("filesystem", "")
        if any(x in fs for x in ("tmpfs", "devtmpfs")):
            continue
        disks.append({
            "mount":    d.get("mount", ""),
            "used_pct": d.get("used_pct", 0),
            "used_gb":  round(d.get("used_gb", 0), 1),
            "total_gb": round(d.get("total_gb", 0), 1),
            "flag":     d.get("flag", "OK"),
        })

    ib_ports = net.get("infiniband", [])

    crit_events   = logs.get("critical_events", logs.get("critical", []))
    client_events = logs.get("client_events", [])

    sysctl_drift = sysctl.get("drift_flags", []) if sysctl.get("available") else []

    return {
        "hostname":          ident.get("hostname", node_dir.name),
        "os":                f"{ident.get('os_name','')} {ident.get('os_version','')}".strip(),
        "kernel":            ident.get("kernel", ""),
        "cpu_count":         ident.get("cpu_count", "?"),
        "uptime_days":       ident.get("uptime_days", 0),
        "load_average":      ident.get("load_average", ""),
        "collection_date":   ident.get("collection_date", ""),

        "mem_used_pct":      mem.get("used_pct", 0),
        "mem_used_gb":       mem.get("used_gb", 0),
        "mem_total_gb":      mem.get("total_gb", 0),
        "swap_used_pct":     mem.get("swap_used_pct", 0),
        "mem_flag":          mem.get("flag", "OK"),

        "cpu_usr_pct":       cpu.get("usr_pct", "0.00"),
        "cpu_sys_pct":       cpu.get("sys_pct", "0.00"),
        "cpu_iowait_pct":    cpu.get("iowait_pct", "0.00"),
        "cpu_idle_pct":      cpu.get("idle_pct", "0.00"),

        "disks":             disks,

        "ib_flag":           net.get("ib_flag", "OK"),
        "ib_ports":          ib_ports,

        "log_flag":          logs.get("flag", "OK"),
        "log_critical":      logs.get("critical_count", logs.get("log_critical", 0)),
        "log_warnings":      logs.get("warning_count",  logs.get("log_warnings", 0)),
        "log_client_events": logs.get("client_event_count", 0),
        "critical_events":   [str(e)[:MAX_EVENT_LEN] for e in
                              (crit_events[:MAX_CRIT_EVENTS] if isinstance(crit_events, list) else [])],
        "client_events":     [str(e)[:MAX_EVENT_LEN] for e in
                              (client_events[:MAX_CLIENT_EVENTS] if isinstance(client_events, list) else [])],

        "lustre_flag":       lustre.get("flag", "OK"),
        "ost_count":         lustre.get("ost_count", 0),
        "mdt_count":         lustre.get("mdt_count", 0),
        "ost_critical":      lustre.get("ost_critical", 0),
        "devices_down":      lustre.get("devices_down", 0),

        "sfa_flag":             sfa.get("flag", "OK"),
        "sfa_tz_inconsistent":  sfa.get("tz_inconsistent", 0),
        "sfa_pool_not_optimal": sfa.get("pool_not_optimal", 0),
        "sfa_ib_fw_summary":    sfa.get("ib_fw_summary", ""),
        "sfa_subsystems":       sfa.get("subsystems", []),

        "sysctl_available":   sysctl.get("available", False),
        "sysctl_flag":        sysctl.get("flag", "N/A") if sysctl.get("available") else "N/A",
        "sysctl_drift_count": sysctl.get("drift_count", 0),
        "sysctl_drift":       sysctl_drift,

        "overall_flag":      "OK",
    }


def node_role(node: dict) -> str:
    """Derive node role from lustre device counts."""
    ost = node.get("ost_count", 0)
    mdt = node.get("mdt_count", 0)
    if ost > 0 and mdt > 0: return "OSS+MDS"
    if ost > 0:              return "OSS"
    if mdt > 0:              return "MDS"
    return "MGS"


def load_cluster(report_dir: Path) -> tuple[dict, list[dict]]:
    cluster_diff = load_json(report_dir / "cluster" / "cluster_diff.json")
    nodes_dir    = report_dir / "nodes"

    flag_lookup = {n["hostname"]: n.get("overall_flag", "OK")
                   for n in cluster_diff.get("nodes", [])}

    nodes = []
    if nodes_dir.exists():
        for node_dir in sorted(nodes_dir.iterdir()):
            if node_dir.is_dir():
                node = load_node(node_dir)
                node["overall_flag"] = flag_lookup.get(node["hostname"], "OK")
                nodes.append(node)

    return cluster_diff, nodes


def group_by_appliance(nodes: list[dict]) -> dict[str, list[dict]]:
    groups = defaultdict(list)
    for node in nodes:
        m = re.match(r'^(.*-ddn\d+)', node["hostname"])
        key = m.group(1) if m else "unknown"
        groups[key].append(node)
    return dict(sorted(groups.items()))


# ─── Correlations ─────────────────────────────────────────────────────────────

def compute_correlations(nodes: list[dict]) -> dict:
    findings = {}

    # Uptime split
    uptime_groups = defaultdict(list)
    for n in nodes:
        uptime_groups[n["uptime_days"]].append(n["hostname"])
    if len(uptime_groups) > 1:
        findings["uptime_split"] = {str(k): v for k, v in sorted(uptime_groups.items())}

    # Log outliers > 2σ
    for label, key in [("log_critical_outliers", "log_critical"),
                        ("log_warning_outliers",  "log_warnings")]:
        counts = {n["hostname"]: n[key] for n in nodes}
        vals   = list(counts.values())
        if len(vals) > 2 and stdev(vals) > 0:
            med = median(vals)
            sd  = stdev(vals)
            threshold = med + 2 * sd
            outliers  = {h: v for h, v in counts.items() if v > threshold}
            if outliers:
                findings[label] = {
                    "median": round(med, 1), "stdev": round(sd, 1),
                    "threshold": round(threshold, 1), "outliers": outliers,
                }

    # LNet client IPs on 3+ nodes
    ip_to_nodes = defaultdict(list)
    for n in nodes:
        seen = set()
        for ev in n.get("client_events", []):
            for ip in re.findall(r'\b(\d{1,3}(?:\.\d{1,3}){3})@o2ib\b', str(ev)):
                if ip not in seen:
                    ip_to_nodes[ip].append(n["hostname"])
                    seen.add(ip)
    multi = {ip: hosts for ip, hosts in ip_to_nodes.items() if len(hosts) >= 3}
    if multi:
        findings["lnet_multi_node_clients"] = multi

    # IB port errors
    ib_errors = {}
    for n in nodes:
        for p in n.get("ib_ports", []):
            if p.get("error_count", 0) > 0:
                ib_errors.setdefault(n["hostname"], []).append({
                    "port":   p.get("ca"),
                    "errors": p.get("error_count"),
                    "detail": p.get("error_detail", ""),
                })
    if ib_errors:
        findings["ib_port_errors"] = ib_errors

    # IB firmware variants
    fw_variants = set()
    for n in nodes:
        if n.get("sfa_ib_fw_summary"):
            for fw in n["sfa_ib_fw_summary"].split():
                fw_variants.add(fw)
    if len(fw_variants) > 1:
        findings["ib_fw_variants"] = sorted(fw_variants)

    # OST fill stats
    ost_pcts = [
        d["used_pct"] for n in nodes for d in n.get("disks", [])
        if "ost" in str(d.get("mount", "")).lower()
    ]
    if ost_pcts:
        findings["ost_fill_stats"] = {
            "count": len(ost_pcts), "min": min(ost_pcts),
            "max": max(ost_pcts), "median": round(median(ost_pcts), 1),
        }

    # Kernel call traces
    call_trace_nodes = [
        n["hostname"] for n in nodes
        if any("Call Trace" in str(ev) for ev in n.get("critical_events", []))
    ]
    if call_trace_nodes:
        findings["kernel_call_traces"] = call_trace_nodes

    # Load outliers
    load_vals = {}
    for n in nodes:
        la = n.get("load_average", "")
        if la:
            try:
                load_vals[n["hostname"]] = float(str(la).split(",")[0].strip())
            except Exception:
                pass
    if len(load_vals) > 2:
        lvals = list(load_vals.values())
        med   = median(lvals)
        sd    = stdev(lvals) if stdev(lvals) > 0 else 1
        outliers = {h: round(v, 2) for h, v in load_vals.items() if v > med + 2 * sd}
        if outliers:
            findings["load_outliers"] = {"median": round(med, 2), "outliers": outliers}

    # Sysctl drift
    sysctl_warn  = [n["hostname"] for n in nodes if n.get("sysctl_flag") == "WARNING"]
    sysctl_info  = [n["hostname"] for n in nodes if n.get("sysctl_flag") == "INFO"]
    if sysctl_warn or sysctl_info:
        param_map = defaultdict(list)
        for n in nodes:
            for d in n.get("sysctl_drift", []):
                p = d.get("param", "")
                if p:
                    param_map[p].append({
                        "hostname":    n["hostname"],
                        "actual":      d.get("actual", ""),
                        "recommended": d.get("recommended", ""),
                    })
        findings["sysctl_drift"] = {
            "warning_nodes": sysctl_warn,
            "info_nodes":    sysctl_info,
            "params":        dict(param_map),
        }

    # SFA timezone mismatch
    tz_map = {}
    for n in nodes:
        for sub in n.get("sfa_subsystems", []):
            name = sub.get("name")
            tz   = sub.get("timezone")
            if name and tz and name not in tz_map:
                tz_map[name] = tz
    if tz_map and len(set(tz_map.values())) > 1:
        findings["sfa_tz_mismatch"] = tz_map

    return findings


# ─── SVG components ───────────────────────────────────────────────────────────

def gauge_color(pct, warn=60, crit=80):
    if pct >= crit: return C["critical"]
    if pct >= warn: return C["warning"]
    return C["ok"]


def semicircle_gauge(label, value_str, pct, warn=60, crit=80,
                     sublabel="", width=180, height=120):
    cx, cy, r = width / 2, height - 20, (width / 2) - 15
    color     = gauge_color(pct, warn, crit)

    def arc(deg_start, deg_end, col, sw=14):
        def pt(d):
            rad = math.radians(d)
            return cx + r * math.cos(rad), cy + r * math.sin(rad)
        x1, y1 = pt(deg_start)
        x2, y2 = pt(deg_end)
        large  = 1 if abs(deg_end - deg_start) > 180 else 0
        return (f'<path d="M {x1:.1f},{y1:.1f} A {r},{r} 0 {large},1 {x2:.1f},{y2:.1f}" '
                f'fill="none" stroke="{col}" stroke-width="{sw}" stroke-linecap="round"/>')

    fill_deg   = 180 + (min(pct, 100) / 100) * 180
    needle_rad = math.radians(fill_deg)
    nx         = cx + (r - 8) * math.cos(needle_rad)
    ny         = cy + (r - 8) * math.sin(needle_rad)

    sub_tag = f'<text x="{cx}" y="{cy+26}" text-anchor="middle" font-size="10" fill="{C["muted"]}">{escape(sublabel)}</text>' if sublabel else ""

    return f"""<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}">
  {arc(180, 360, C["track"], 14)}
  {arc(180, min(fill_deg, 359.9), color, 14)}
  <line x1="{cx}" y1="{cy}" x2="{nx:.1f}" y2="{ny:.1f}" stroke="{color}" stroke-width="2" stroke-linecap="round"/>
  <circle cx="{cx}" cy="{cy}" r="4" fill="{color}"/>
  <text x="{cx}" y="{cy-8}" text-anchor="middle" font-size="18" font-weight="bold" fill="{color}">{escape(value_str)}</text>
  <text x="{cx}" y="{cy+14}" text-anchor="middle" font-size="11" fill="#94a3b8">{escape(label)}</text>
  {sub_tag}
</svg>"""


def donut_chart(n_crit, n_warn, n_ok, total, width=160, height=160):
    cx, cy = width / 2, height / 2
    r_out, r_in = 60, 38
    segments = [(n_crit, C["critical"], "Critical"),
                (n_warn, C["warning"],  "Warning"),
                (n_ok,   C["ok"],       "OK")]
    svgs  = []
    angle = -90.0

    for count, color, label in segments:
        if count == 0:
            continue
        sweep = (count / total) * 360
        end   = angle + sweep

        def pt_o(d):
            rad = math.radians(d)
            return cx + r_out * math.cos(rad), cy + r_out * math.sin(rad)
        def pt_i(d):
            rad = math.radians(d)
            return cx + r_in * math.cos(rad), cy + r_in * math.sin(rad)

        x1o, y1o = pt_o(angle); x2o, y2o = pt_o(end)
        x1i, y1i = pt_i(angle); x2i, y2i = pt_i(end)
        large = 1 if sweep > 180 else 0
        path = (f'M {x1o:.1f},{y1o:.1f} A {r_out},{r_out} 0 {large},1 {x2o:.1f},{y2o:.1f} '
                f'L {x2i:.1f},{y2i:.1f} A {r_in},{r_in} 0 {large},0 {x1i:.1f},{y1i:.1f} Z')
        svgs.append(f'<path d="{path}" fill="{color}" opacity="0.9"/>')
        angle = end

    svgs.append(f'<text x="{cx}" y="{cy-6}" text-anchor="middle" font-size="22" font-weight="bold" fill="{C["text"]}">{total}</text>')
    svgs.append(f'<text x="{cx}" y="{cy+12}" text-anchor="middle" font-size="11" fill="{C["muted"]}">nodes</text>')

    lx, ly = 8, height - 18
    for count, color, label in segments:
        svgs.append(f'<rect x="{lx}" y="{ly}" width="10" height="10" fill="{color}" rx="2"/>')
        svgs.append(f'<text x="{lx+13}" y="{ly+9}" font-size="10" fill="{C["muted"]}">{count} {label}</text>')
        lx += 70

    return f'<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}">{"".join(svgs)}</svg>'


def load_heatmap(load_data: dict, nodes: list[dict]) -> str:
    # Try DDN appliance grouping (sc1-ddn1, sc1-ddn2, etc.)
    groups = defaultdict(list)
    for node in nodes:
        m = re.match(r'^(.*-ddn\d+)', node["hostname"])
        key = m.group(1) if m else None
        if key:
            groups[key].append(node["hostname"])

    # Fallback: group by role suffix (-oss, -mds, -mgs, etc.)
    if not groups:
        import re as _re
        for node in nodes:
            # Try to extract role suffix: alcyone-oss -> oss, maia-mgs -> mgs
            m = _re.search(r'-(oss|mds|mgs|client|nid|\w+)$', node["hostname"])
            key = m.group(1) if m else "unknown"
            groups[key].append(node["hostname"])
        # If everything still ends up in one group, chunk by 4
        if len(groups) == 1 and list(groups.keys())[0] == "unknown":
            groups.clear()
            hostnames = sorted(n["hostname"] for n in nodes)
            chunk = 4
            for i in range(0, len(hostnames), chunk):
                first = hostnames[i].split("-")[-1]
                last  = hostnames[min(i+chunk-1, len(hostnames)-1)].split("-")[-1]
                groups[f"{first}-{last}"] = hostnames[i:i+chunk]

    appliances = sorted(groups.keys())
    cols = max(len(v) for v in groups.values())
    rows = len(appliances)
    # Scale cell size for wide grids
    if cols > 16:   cell_w, cell_h = 32, 28
    elif cols > 8:  cell_w, cell_h = 42, 32
    else:           cell_w, cell_h = 52, 36
    pad_x, pad_y = 8, 28
    label_w = 60
    width  = pad_x * 2 + cols * cell_w + (cols - 1) * 3 + label_w
    height = pad_y + rows * cell_h + (rows - 1) * 4 + 20

    def load_color(load):
        if load >= 40: return C["critical"]
        if load >= 20: return C["warning"]
        if load >= 5:  return C["ok"]
        return "#1e3a5f"

    flag_lookup = {n["hostname"]: n["overall_flag"] for n in nodes}
    svgs = []

    for ci in range(cols):
        x = pad_x + 56 + ci * (cell_w + 4) + cell_w // 2
        svgs.append(f'<text x="{x}" y="16" text-anchor="middle" font-size="9" fill="{C["muted"]}">vm{ci}</text>')

    for ri, appl in enumerate(appliances):
        y     = pad_y + ri * (cell_h + 4)
        short = appl.split("-")[-1]
        svgs.append(f'<text x="{pad_x+50}" y="{y + cell_h//2 + 4}" text-anchor="end" font-size="10" fill="#94a3b8">{short}</text>')

        for ci, hostname in enumerate(sorted(groups[appl])):
            x     = pad_x + 56 + ci * (cell_w + 4)
            load  = load_data.get(hostname, 0)
            color = load_color(load)
            flag  = flag_lookup.get(hostname, "OK")
            load_str = f"{load:.0f}" if load >= 10 else f"{load:.1f}"
            dot_color = flag_color(flag)

            svgs.append(
                f'<rect x="{x}" y="{y}" width="{cell_w}" height="{cell_h}" rx="4" '
                f'fill="{color}" opacity="0.85" style="cursor:pointer" '
                f'onclick="scrollToNode(\'{hostname}\')" />'
            )
            svgs.append(
                f'<text x="{x + cell_w//2}" y="{y + cell_h//2 + 3}" '
                f'text-anchor="middle" font-size="12" font-weight="bold" fill="#0f1117">{load_str}</text>'
            )
            svgs.append(f'<circle cx="{x + cell_w - 6}" cy="{y + 6}" r="3" fill="{dot_color}"/>')

    return (f'<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}" '
            f'style="overflow:visible">{"".join(svgs)}</svg>')


# ─── HTML helpers ─────────────────────────────────────────────────────────────

def badge(flag: str) -> str:
    color = flag_color(flag)
    return f'<span style="background:{color};color:#0f1117;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:700;">{escape(flag)}</span>'


def collapsible(title: str, content: str, flag: str = "") -> str:
    flag_indicator = f' {badge(flag)}' if flag and flag not in ("OK", "N/A") else ""
    return f"""<div class="collapsible" onclick="toggleSection(this)">
  <span>{escape(title)}{flag_indicator}</span>
  <span class="chevron">▶</span>
</div>
<div class="collapsible-content">
{content}
</div>"""


def color_val(val, warn_thresh=None, crit_thresh=None, lower_is_bad=False) -> str:
    try:
        v = float(str(val).split()[0])
    except Exception:
        return f'<span style="color:{C["muted"]}">{escape(str(val))}</span>'
    if warn_thresh is None:
        return escape(str(val))
    if lower_is_bad:
        color = C["critical"] if v <= crit_thresh else C["warning"] if v <= warn_thresh else C["ok"]
    else:
        color = C["critical"] if v >= crit_thresh else C["warning"] if v >= warn_thresh else C["ok"]
    return f'<span style="color:{color};font-weight:600">{escape(str(val))}</span>'


# ─── Section renderers ────────────────────────────────────────────────────────

def render_disk_section(node: dict) -> str:
    disks = node.get("disks", [])
    if not disks:
        return "<p style='color:#64748b'>No disk data</p>"

    rows = ""
    for d in disks:
        pct   = d["used_pct"]
        color = flag_color(d["flag"])
        bar_color = C["critical"] if pct >= 85 else C["warning"] if pct >= 70 else C["ok"]
        rows += f"""<tr>
      <td style="font-family:monospace;font-size:12px">{escape(d['mount'])}</td>
      <td style="color:{color};font-weight:600">{pct}%</td>
      <td>{d['used_gb']} GB</td>
      <td>{d['total_gb']} GB</td>
      <td style="width:80px">
        <div style="background:#253048;border-radius:3px;height:6px">
          <div style="background:{bar_color};width:{min(pct,100)}%;height:6px;border-radius:3px"></div>
        </div>
      </td>
    </tr>"""
    return f"""<table style="width:100%;border-collapse:collapse;font-size:12px">
  <thead><tr style="color:{C['muted']}">
    <th style="text-align:left;padding:4px">Mount</th>
    <th>Used%</th><th>Used</th><th>Total</th><th>Fill</th>
  </tr></thead>
  <tbody>{rows}</tbody>
</table>"""


def render_ib_section(node: dict) -> str:
    ports = node.get("ib_ports", [])
    if not ports:
        return f"<p style='color:{C['muted']}'>No IB data</p>"

    rows = ""
    for p in ports:
        err_count = p.get("error_count", 0)
        err_color = C["critical"] if err_count > 0 else C["ok"]
        fw        = p.get("firmware", "")
        rows += f"""<tr>
      <td style="font-family:monospace">{escape(str(p.get('ca','')))} </td>
      <td style="color:{flag_color(p.get('state',''))}">
        {escape(str(p.get('state','')))}
      </td>
      <td>{escape(str(p.get('rate','')))} </td>
      <td style="font-family:monospace;font-size:11px">{escape(fw)}</td>
      <td style="color:{err_color};font-weight:600">{err_count}</td>
      <td style="font-size:11px;color:{C['muted']}">{escape(str(p.get('error_detail','') or ''))}</td>
    </tr>"""
    return f"""<table style="width:100%;border-collapse:collapse;font-size:12px">
  <thead><tr style="color:{C['muted']}">
    <th style="text-align:left;padding:4px">Port</th>
    <th>State</th><th>Rate</th><th>Firmware</th><th>Errors</th><th>Detail</th>
  </tr></thead>
  <tbody>{rows}</tbody>
</table>"""


def render_log_section(node: dict) -> str:
    events = node.get("critical_events", [])
    crit   = node.get("log_critical", 0)
    warn   = node.get("log_warnings", 0)
    client = node.get("log_client_events", 0)

    summary = f"<p style='font-size:12px;color:{C['muted']}'>{crit} critical &nbsp;·&nbsp; {warn} warnings &nbsp;·&nbsp; {client} client events</p>"
    if not events:
        return summary + f"<p style='color:{C['muted']}'>No critical event samples</p>"

    lines = "\n".join(escape(str(e)) for e in events)
    return summary + f'<pre style="font-size:11px;overflow-x:auto;white-space:pre-wrap;word-break:break-all;color:{C["text"]};margin:4px 0">{lines}</pre>'


def render_client_ni_section(node: dict) -> str:
    events = node.get("client_events", [])
    count  = node.get("log_client_events", 0)
    if not events:
        return f"<p style='color:{C['muted']}'>No client NI events ({count} total)</p>"
    lines = "\n".join(escape(str(e)) for e in events)
    return f'<pre style="font-size:11px;overflow-x:auto;white-space:pre-wrap;word-break:break-all;color:{C["text"]};margin:4px 0">{lines}</pre>'


def render_sfa_section(node: dict) -> str:
    if not node.get("sfa_flag"):
        return f"<p style='color:{C['muted']}'>No SFA data</p>"

    lines = [
        f"<p style='font-size:12px'><b>Flag:</b> {badge(node['sfa_flag'])}</p>",
        f"<p style='font-size:12px'><b>Timezone inconsistent:</b> {node.get('sfa_tz_inconsistent', 0)}</p>",
        f"<p style='font-size:12px'><b>Pool not optimal:</b> {node.get('sfa_pool_not_optimal', 0)}</p>",
    ]
    if node.get("sfa_ib_fw_summary"):
        lines.append(f"<p style='font-size:12px'><b>IB FW:</b> <code style='font-size:11px'>{escape(node['sfa_ib_fw_summary'])}</code></p>")

    subsystems = node.get("sfa_subsystems", [])
    if subsystems:
        rows = ""
        tzs  = [s.get("timezone", "") for s in subsystems]
        mode_tz = max(set(tzs), key=tzs.count) if tzs else ""
        for s in subsystems:
            tz    = s.get("timezone", "")
            tz_color = C["warning"] if tz != mode_tz else C["text"]
            rows += f"<tr><td>{escape(s.get('name',''))}</td><td>{escape(s.get('platform',''))}</td><td style='color:{flag_color(s.get('health','ok'))}'>{escape(s.get('health',''))}</td><td style='color:{tz_color}'>{escape(tz)}</td></tr>"
        lines.append(f"""<table style="width:100%;border-collapse:collapse;font-size:12px;margin-top:6px">
  <thead><tr style="color:{C['muted']}"><th style="text-align:left;padding:3px">Subsystem</th><th>Platform</th><th>Health</th><th>Timezone</th></tr></thead>
  <tbody>{rows}</tbody>
</table>""")

    return "\n".join(lines)


def render_sysctl_section(node: dict) -> str:
    if not node.get("sysctl_available"):
        return f"<p style='color:{C['muted']}'>Sysctl data not available</p>"

    drift = node.get("sysctl_drift", [])
    count = node.get("sysctl_drift_count", 0)

    header = f"<p style='font-size:12px'><b>Flag:</b> {badge(node['sysctl_flag'])} &nbsp; <b>Drift findings:</b> {count}</p>"

    if not drift:
        return header + f"<p style='color:{C['ok']}'>✓ All monitored parameters at recommended values</p>"

    rows = ""
    for d in drift:
        rows += f"""<tr>
      <td style="font-family:monospace;font-size:12px">{escape(d.get('param',''))}</td>
      <td style="color:{C['warning']};font-weight:600">{escape(str(d.get('actual','')))}</td>
      <td style="color:{C['ok']}">{escape(str(d.get('recommended','')))}</td>
    </tr>"""

    return header + f"""<table style="width:100%;border-collapse:collapse;font-size:12px">
  <thead><tr style="color:{C['muted']}">
    <th style="text-align:left;padding:4px">Parameter</th>
    <th>Actual</th><th>Recommended</th>
  </tr></thead>
  <tbody>{rows}</tbody>
</table>"""


# ─── Node card ────────────────────────────────────────────────────────────────

def render_node_card(node: dict) -> str:
    h        = node["hostname"]
    flag     = node["overall_flag"]
    flag_c   = flag_color(flag)
    mem_pct  = node["mem_used_pct"]
    bar_color = C["critical"] if mem_pct >= 90 else C["warning"] if mem_pct >= 80 else C["ok"]

    sysctl_summary = ""
    if node.get("sysctl_available"):
        sc  = node.get("sysctl_flag", "N/A")
        sdc = node.get("sysctl_drift_count", 0)
        sc_color = flag_color(sc)
        sysctl_summary = f'<span style="color:{sc_color}">Sysctl: {escape(sc)}</span>'
        if sdc > 0:
            sysctl_summary += f' <span style="color:{C["muted"]}">({sdc} drift)</span>'

    load_1m = ""
    la = node.get("load_average", "")
    if la:
        try:
            load_1m = str(float(str(la).split(",")[0].strip()))
        except Exception:
            load_1m = str(la).split(",")[0].strip()

    summary = f"""<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:6px;font-size:12px;margin-bottom:8px">
  <div>Uptime: <b>{node['uptime_days']}d</b> &nbsp;·&nbsp; Load: <b>{escape(load_1m)}</b></div>
  <div>Mem: {color_val(mem_pct, 80, 90)}% ({node['mem_used_gb']} / {node['mem_total_gb']} GB) &nbsp;·&nbsp; Swap: {node['swap_used_pct']}%</div>
  <div>CPU: usr={escape(str(node['cpu_usr_pct']))}% &nbsp; iowait={escape(str(node['cpu_iowait_pct']))}% &nbsp; idle={color_val(node['cpu_idle_pct'], 10, 2, lower_is_bad=True)}%</div>
  <div>IB: {badge(node['ib_flag'])} &nbsp;·&nbsp; Logs: <span style="color:{flag_color(node['log_flag'])}">{node['log_critical']} crit</span> / {node['log_warnings']} warn / {node['log_client_events']} client</div>
  <div>{sysctl_summary}</div>
</div>
<div style="background:{C['border']};border-radius:3px;height:6px;margin-bottom:8px">
  <div style="background:{bar_color};width:{min(mem_pct,100)}%;height:6px;border-radius:3px"></div>
</div>"""

    sections = ""
    sections += collapsible("Disk Usage",       render_disk_section(node),     node.get("mem_flag",""))
    sections += collapsible("InfiniBand",        render_ib_section(node),       node.get("ib_flag",""))
    sections += collapsible("Log Events",        render_log_section(node),      node.get("log_flag",""))
    sections += collapsible("Client NI Events",  render_client_ni_section(node))
    sections += collapsible("SFA",               render_sfa_section(node),      node.get("sfa_flag",""))
    sections += collapsible("Sysctl Tuning",     render_sysctl_section(node),   node.get("sysctl_flag",""))

    role = node_role(node)
    role_color = {
        "OSS": C["info"], "MDS": C["ok"], "OSS+MDS": C["warning"],
        "MGS": C["muted"]
    }.get(role, C["muted"])

    return f"""<div class="node-card" id="node-{escape(h)}" data-status="{escape(flag)}" data-hostname="{escape(h)}">
  <div class="node-header">
    <h3 style="margin:0;font-size:14px">{escape(h)}</h3>
    <span style="font-size:11px;color:{role_color};font-weight:600;margin-right:6px">{escape(role)}</span>
    {badge(flag)}
  </div>
  <div style="padding:10px 14px">
    {summary}
    {sections}
  </div>
</div>"""


# ─── Appliance section ────────────────────────────────────────────────────────

def render_appliance(appliance: str, nodes: list[dict]) -> str:
    flags    = [n["overall_flag"] for n in nodes]
    n_crit   = flags.count("CRITICAL")
    n_warn   = flags.count("WARNING")
    cards    = "\n".join(render_node_card(n) for n in nodes)
    summary  = f"{len(nodes)} nodes"
    if n_crit: summary += f" &nbsp;·&nbsp; <span style='color:{C['critical']}'>{n_crit} critical</span>"
    if n_warn: summary += f" &nbsp;·&nbsp; <span style='color:{C['warning']}'>{n_warn} warning</span>"

    return f"""<div class="appliance-group" id="appliance-{escape(appliance)}">
  <div style="display:flex;align-items:center;gap:12px;padding:12px 16px;background:{C['border']};border-radius:8px;margin-bottom:8px">
    <h2 style="margin:0;font-size:16px">{escape(appliance)}</h2>
    <span style="font-size:12px;color:{C['muted']}">{summary}</span>
  </div>
  {cards}
</div>"""


# ─── Anomaly panel ────────────────────────────────────────────────────────────

def render_anomaly_card(title: str, severity: str, content: str) -> str:
    color     = flag_color(severity)
    border    = f"border-left:4px solid {color}"
    return f"""<div style="background:{C['card']};{border};border-radius:6px;padding:12px 16px;margin-bottom:10px">
  <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px">
    <b style="font-size:13px">{escape(title)}</b>
    {badge(severity)}
  </div>
  <div style="font-size:12px;color:{C['text']}">{content}</div>
</div>"""


def render_anomaly_panel(correlations: dict) -> str:
    cards = ""

    if "log_critical_outliers" in correlations:
        c = correlations["log_critical_outliers"]
        items = " &nbsp;·&nbsp; ".join(f"<b>{h}</b>: {v}" for h, v in c["outliers"].items())
        cards += render_anomaly_card(
            "Log Critical Outliers", "CRITICAL",
            f"Cluster median: {c['median']} &nbsp;·&nbsp; Threshold: {c['threshold']}<br>{items}"
        )

    if "kernel_call_traces" in correlations:
        nodes_str = ", ".join(f"<b>{h}</b>" for h in correlations["kernel_call_traces"])
        cards += render_anomaly_card("Kernel Call Traces", "CRITICAL",
            f"Call traces detected on: {nodes_str}")

    if "ib_port_errors" in correlations:
        items = []
        for hostname, ports in correlations["ib_port_errors"].items():
            for p in ports:
                items.append(f"<b>{hostname}</b> {p['port']}: {p['errors']} errors ({p.get('detail','')})")
        cards += render_anomaly_card("IB Port Errors", "CRITICAL", "<br>".join(items))

    if "uptime_split" in correlations:
        groups = correlations["uptime_split"]
        items  = " &nbsp;·&nbsp; ".join(f"<b>{k}d</b>: {', '.join(v)}" for k, v in groups.items())
        cards += render_anomaly_card("Uptime Split — Partial Reboot Event", "WARNING", items)

    if "log_warning_outliers" in correlations:
        c = correlations["log_warning_outliers"]
        items = " &nbsp;·&nbsp; ".join(f"<b>{h}</b>: {v}" for h, v in c["outliers"].items())
        cards += render_anomaly_card(
            "Log Warning Outliers", "WARNING",
            f"Cluster median: {c['median']} &nbsp;·&nbsp; Threshold: {c['threshold']}<br>{items}"
        )

    if "lnet_multi_node_clients" in correlations:
        items = []
        for ip, hosts in correlations["lnet_multi_node_clients"].items():
            items.append(f"<b>{ip}@o2ib</b> → {', '.join(hosts)}")
        cards += render_anomaly_card(
            "LNet Client NI Recovery — Multi-Node", "WARNING",
            "Same client IPs in recovery on 3+ nodes simultaneously:<br>" + "<br>".join(items)
        )

    if "load_outliers" in correlations:
        c = correlations["load_outliers"]
        items = " &nbsp;·&nbsp; ".join(f"<b>{h}</b>: {v}" for h, v in c["outliers"].items())
        cards += render_anomaly_card(
            "Load Average Outliers", "WARNING",
            f"Cluster median: {c['median']}<br>{items}"
        )

    if "sysctl_drift" in correlations:
        c       = correlations["sysctl_drift"]
        w_nodes = c.get("warning_nodes", [])
        i_nodes = c.get("info_nodes", [])
        params  = c.get("params", {})
        severity = "WARNING" if w_nodes else "INFO"
        content = ""
        if w_nodes:
            content += f"<b>WARNING nodes:</b> {', '.join(w_nodes)}<br>"
        if i_nodes:
            content += f"<b>INFO nodes:</b> {', '.join(i_nodes)}<br>"
        for param, node_list in params.items():
            rec = node_list[0].get("recommended", "") if node_list else ""
            affected = ", ".join(f"<b>{x['hostname']}</b> ({x['actual']})" for x in node_list)
            content += f"<br><code style='font-size:11px'>{escape(param)}</code> recommended={escape(rec)} — {affected}"
        cards += render_anomaly_card("Sysctl Tuning Drift", severity, content)

    if "sfa_tz_mismatch" in correlations:
        items = " &nbsp;·&nbsp; ".join(f"<b>{k}</b>: {v}" for k, v in correlations["sfa_tz_mismatch"].items())
        cards += render_anomaly_card("SFA Timezone Mismatch", "WARNING", items)

    if "ib_fw_variants" in correlations:
        variants = " &nbsp;·&nbsp; ".join(f"<code style='font-size:11px'>{escape(v)}</code>"
                                           for v in correlations["ib_fw_variants"])
        cards += render_anomaly_card("IB Firmware Variants", "INFO",
            f"Two firmware strings present: {variants} — verify these are different card models, not version drift")

    if "ost_fill_stats" in correlations:
        c = correlations["ost_fill_stats"]
        cards += render_anomaly_card("OST Fill Level", "INFO",
            f"{c['count']} OSTs &nbsp;·&nbsp; median <b>{c['median']}%</b> &nbsp;·&nbsp; range {c['min']}–{c['max']}%")

    if not cards:
        cards = f"<p style='color:{C['ok']}'>✓ No significant anomalies detected</p>"

    return f"""<div style="padding:16px 20px">
  <h2 style="margin:0 0 12px 0;font-size:15px;color:{C['muted']};text-transform:uppercase;letter-spacing:0.06em">Anomalies &amp; Correlations</h2>
  {cards}
</div>"""


# ─── Overview table ───────────────────────────────────────────────────────────

def render_overview_table(nodes: list[dict]) -> str:
    rows = ""
    for n in nodes:
        flag     = n["overall_flag"]
        flag_c   = flag_color(flag)
        la       = str(n.get("load_average", "")).split(",")[0].strip()
        role     = node_role(n)
        sysctl_f = n.get("sysctl_flag", "N/A")
        sdc      = n.get("sysctl_drift_count", 0)
        sysctl_cell = f'<span style="color:{flag_color(sysctl_f)}">{escape(sysctl_f)}</span>'
        if sdc > 0:
            sysctl_cell += f' <span style="color:{C["muted"]};font-size:10px">({sdc}d)</span>'

        rows += f"""<tr data-status="{escape(flag)}" data-hostname="{escape(n['hostname'])}" onclick="scrollToNode('{escape(n['hostname'])}')">
  <td style="font-family:monospace;font-size:12px">{escape(n['hostname'])}</td>
  <td><span style="font-size:11px;color:{C['muted']}">{escape(role)}</span></td>
  <td>{badge(flag)}</td>
  <td>{n['uptime_days']}</td>
  <td>{color_val(la, 20, 40)}</td>
  <td>{color_val(n['mem_used_pct'], 80, 90)}</td>
  <td>{color_val(n['cpu_idle_pct'], 10, 2, lower_is_bad=True)}</td>
  <td style="color:{flag_color('CRITICAL') if n['log_critical'] >= 10 else flag_color('WARNING') if n['log_critical'] >= 5 else C['muted'] if n['log_critical'] == 0 else C['text']}">{n['log_critical']}</td>
  <td style="color:{flag_color('CRITICAL') if n['log_warnings'] >= 200 else flag_color('WARNING') if n['log_warnings'] >= 50 else C['muted'] if n['log_warnings'] == 0 else C['text']}">{n['log_warnings']}</td>
  <td style="color:{flag_color(n['ib_flag'])}">{escape(n['ib_flag'])}</td>
  <td style="color:{flag_color(n['sfa_flag'])}">{escape(n['sfa_flag'])}</td>
  <td>{sysctl_cell}</td>
</tr>"""

    return f"""<div style="padding:0 20px 16px 20px">
  <h2 style="margin:0 0 10px 0;font-size:15px;color:{C['muted']};text-transform:uppercase;letter-spacing:0.06em">Cluster Overview</h2>
  <div style="display:flex;gap:8px;margin-bottom:10px;flex-wrap:wrap">
    <input id="node-search" type="text" placeholder="Search nodes..." oninput="filterTable()"
      style="background:{C['card']};border:1px solid {C['border']};color:{C['text']};padding:6px 10px;border-radius:4px;font-size:12px;width:200px">
    <button onclick="filterStatus('all')"      class="filter-btn active" data-filter="all">All</button>
    <button onclick="filterStatus('CRITICAL')" class="filter-btn" data-filter="CRITICAL">Critical</button>
    <button onclick="filterStatus('WARNING')"  class="filter-btn" data-filter="WARNING">Warning</button>
    <button onclick="filterStatus('OK')"       class="filter-btn" data-filter="OK">OK</button>
  </div>
  <div style="overflow-x:auto">
    <table id="overview-table" style="width:100%;border-collapse:collapse;font-size:12px">
      <thead>
        <tr style="color:{C['muted']};border-bottom:1px solid {C['border']}">
          <th onclick="sortTable(0)" style="text-align:left;padding:6px 8px;cursor:pointer">Hostname ↕</th>
          <th onclick="sortTable(2)" style="padding:6px 4px;cursor:pointer">Role ↕</th>
          <th onclick="sortTable(2)" style="padding:6px 4px;cursor:pointer">Status ↕</th>
          <th onclick="sortTable(3)" style="padding:6px 4px;cursor:pointer">Uptime(d) ↕</th>
          <th onclick="sortTable(4)" style="padding:6px 4px;cursor:pointer">Load ↕</th>
          <th onclick="sortTable(5)" style="padding:6px 4px;cursor:pointer">Mem% ↕</th>
          <th onclick="sortTable(6)" style="padding:6px 4px;cursor:pointer">CPU Idle% ↕</th>
          <th onclick="sortTable(7)" style="padding:6px 4px;cursor:pointer">Log Crit ↕</th>
          <th onclick="sortTable(8)" style="padding:6px 4px;cursor:pointer">Log Warn ↕</th>
          <th onclick="sortTable(9)" style="padding:6px 4px;cursor:pointer">IB ↕</th>
          <th onclick="sortTable(10)" style="padding:6px 4px;cursor:pointer">SFA ↕</th>
          <th onclick="sortTable(11)" style="padding:6px 4px;cursor:pointer">Sysctl ↕</th>
        </tr>
      </thead>
      <tbody id="overview-tbody">
        {rows}
      </tbody>
    </table>
  </div>
</div>"""


# ─── Visual summary header ────────────────────────────────────────────────────

def render_visual_header(nodes: list[dict], report_dir: Path) -> str:
    # Metrics
    n_total = len(nodes)
    n_crit  = sum(1 for n in nodes if n["overall_flag"] == "CRITICAL")
    n_warn  = sum(1 for n in nodes if n["overall_flag"] == "WARNING")
    n_ok    = sum(1 for n in nodes if n["overall_flag"] == "OK")

    mem_pcts  = [n["mem_used_pct"] for n in nodes]
    avg_mem   = round(sum(mem_pcts) / len(mem_pcts), 1) if mem_pcts else 0
    max_mem   = max(mem_pcts) if mem_pcts else 0
    max_mem_h = next((n["hostname"] for n in nodes if n["mem_used_pct"] == max_mem), "")

    load_data = {}
    for n in nodes:
        la = n.get("load_average", "")
        if la:
            try:
                load_data[n["hostname"]] = float(str(la).split(",")[0].strip())
            except Exception:
                pass

    avg_load   = round(sum(load_data.values()) / len(load_data), 1) if load_data else 0
    max_load   = max(load_data.values(), default=0)
    max_load_h = max(load_data, key=load_data.get) if load_data else ""
    cpu_count  = next((int(str(n.get("cpu_count", 24))) for n in nodes), 24)
    # Normalize load: use 4x cpu_count as 100% ceiling (storage nodes run high load normally)
    load_pct   = min(round((avg_load / max(cpu_count * 4, 1)) * 100, 1), 100)

    # OST fill from resources.json
    ost_fills = []
    nodes_dir = report_dir / "nodes"
    for node_dir in sorted(nodes_dir.iterdir()):
        if node_dir.is_dir():
            res = load_json(node_dir / "resources.json")
            for d in res.get("disk", []):
                mount = d.get("mount", "")
                if "ost" in mount.lower():
                    ost_fills.append({
                        "used_pct": d.get("used_pct", 0),
                        "used_tb":  round(d.get("used_gb", 0) / 1024, 1),
                        "total_tb": round(d.get("total_gb", 0) / 1024, 1),
                    })

    avg_ost   = round(median([x["used_pct"] for x in ost_fills]), 1) if ost_fills else 0
    used_pb   = round(sum(x["used_tb"] for x in ost_fills) / 1024, 2)
    total_pb  = round(sum(x["total_tb"] for x in ost_fills) / 1024, 2)

    total_crit_ev = sum(n["log_critical"] for n in nodes)
    total_warn_ev = sum(n["log_warnings"] for n in nodes)

    # SVGs
    donut   = donut_chart(n_crit, n_warn, n_ok, n_total)
    mem_g   = semicircle_gauge("Avg Memory", f"{avg_mem}%", avg_mem, 70, 85,
                                sublabel=f"max {max_mem}% ({max_mem_h})")
    load_g  = semicircle_gauge("Avg Load", f"{avg_load}", load_pct, 40, 70,
                                sublabel=f"max {max_load:.0f} ({max_load_h})")
    ost_g   = semicircle_gauge("OST Fill", f"{avg_ost}%", avg_ost, 70, 85,
                                sublabel=f"{used_pb} / {total_pb} PB")
    heatmap = load_heatmap(load_data, nodes)

    panel = lambda title, content: f"""<div class="vsummary-panel">
  <div class="vsummary-title">{title}</div>
  {content}
</div>"""

    return f"""<div class="vsummary">
  {panel("Node Status", donut)}
  {panel("Memory Pressure", mem_g)}
  {panel("CPU Load", load_g)}
  {panel("OST Capacity", ost_g)}
  {panel("Log Events", f'''<div style="display:flex;gap:24px;align-items:center;padding:16px 0">
    <div style="text-align:center">
      <div style="font-size:36px;font-weight:700;color:{C['critical']};line-height:1">{total_crit_ev}</div>
      <div style="font-size:11px;color:{C['muted']};margin-top:4px">Critical Events</div>
    </div>
    <div style="text-align:center">
      <div style="font-size:36px;font-weight:700;color:{C['warning']};line-height:1">{total_warn_ev}</div>
      <div style="font-size:11px;color:{C['muted']};margin-top:4px">Warnings</div>
    </div>
  </div>''')}
  <div class="vsummary-panel wide">
    <div class="vsummary-title">Load Heatmap <span style="font-size:10px;color:{C['muted']}">(click → node card)</span></div>
    {heatmap}
    <div style="font-size:10px;color:{C['muted']};margin-top:6px">Cell = 1-min load avg &nbsp;·&nbsp; Dot = node status</div>
  </div>
</div>"""


# ─── CSS ──────────────────────────────────────────────────────────────────────

CSS = f"""
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{ background: {C['bg']}; color: {C['text']}; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; line-height: 1.5; }}
h1, h2, h3 {{ font-weight: 600; }}
table {{ width: 100%; border-collapse: collapse; }}
th, td {{ padding: 5px 8px; text-align: left; border-bottom: 1px solid {C['border']}; }}
th {{ color: {C['muted']}; font-weight: 600; font-size: 11px; text-transform: uppercase; letter-spacing: 0.04em; }}
code {{ font-family: monospace; background: {C['border']}; padding: 1px 4px; border-radius: 3px; }}
pre {{ background: #0a0e17; padding: 8px; border-radius: 4px; overflow-x: auto; }}

/* Visual summary */
.vsummary {{
  display: flex; flex-wrap: wrap; gap: 12px;
  padding: 16px 20px; background: #111827;
  border-bottom: 1px solid {C['border']}; align-items: stretch;
}}
.vsummary-panel {{
  background: {C['card']}; border: 1px solid {C['border']}; border-radius: 8px;
  padding: 12px 16px; display: flex; flex-direction: column; align-items: center;
  flex: 1 1 160px; max-width: 210px; min-width: 150px;
}}
.vsummary-panel.wide {{ flex: 1 1 280px; max-width: 340px; }}
.vsummary-title {{
  font-size: 10px; text-transform: uppercase; letter-spacing: 0.08em;
  color: {C['muted']}; font-weight: 600; margin-bottom: 8px; text-align: center;
}}

/* Node cards */
.node-card {{
  background: {C['card']}; border: 1px solid {C['border']}; border-radius: 8px;
  margin-bottom: 10px; overflow: hidden;
  animation: cardPulse 4s ease-in-out infinite;
}}
.node-card[data-status="OK"], .node-card[data-status="WARNING"] {{ animation: none; }}
@keyframes cardPulse {{
  0%   {{ border-color: {C['border']}; }}
  50%  {{ border-color: rgba(248,113,113,0.5); }}
  100% {{ border-color: {C['border']}; }}
}}
@keyframes rowPulse {{
  0%   {{ box-shadow: inset 0 0 0 0px rgba(248,113,113,0); }}
  50%  {{ box-shadow: inset 0 0 0 2px rgba(248,113,113,0.5); }}
  100% {{ box-shadow: inset 0 0 0 0px rgba(248,113,113,0); }}
}}
.node-header {{
  display: flex; justify-content: space-between; align-items: center;
  padding: 10px 14px; background: {C['border']}; border-bottom: 1px solid {C['border']};
}}
.appliance-group {{ margin-bottom: 24px; }}

/* Collapsible */
.collapsible {{
  display: flex; justify-content: space-between; align-items: center;
  padding: 7px 14px; cursor: pointer; font-size: 12px; font-weight: 600;
  border-top: 1px solid {C['border']}; user-select: none;
  color: {C['muted']};
}}
.collapsible:hover {{ color: {C['text']}; background: rgba(255,255,255,0.02); }}
.collapsible-content {{ display: none; padding: 10px 14px; }}
.chevron {{ display: inline-block; transition: transform 0.2s ease; font-size: 10px; }}

/* Filter buttons */
.filter-btn {{
  background: {C['card']}; border: 1px solid {C['border']}; color: {C['muted']};
  padding: 5px 12px; border-radius: 4px; cursor: pointer; font-size: 12px;
}}
.filter-btn:hover, .filter-btn.active {{ background: {C['border']}; color: {C['text']}; }}

/* Flash animation for row click */
@keyframes flash {{
  0%   {{ background: rgba(96,165,250,0.2); }}
  100% {{ background: transparent; }}
}}
.flash {{ animation: flash 0.8s ease-out; }}

/* Table rows */
#overview-table tbody tr {{ cursor: pointer; border-bottom: 1px solid {C['border']}; }}
#overview-table tbody tr:hover {{ background: rgba(255,255,255,0.03); }}
#overview-table tbody tr[data-status="CRITICAL"] {{ animation: rowPulse 3s ease-in-out infinite; }}

/* Scrollbar */
::-webkit-scrollbar {{ width: 6px; height: 6px; }}
::-webkit-scrollbar-track {{ background: {C['bg']}; }}
::-webkit-scrollbar-thumb {{ background: {C['border']}; border-radius: 3px; }}
"""

# ─── JavaScript ───────────────────────────────────────────────────────────────

JS = """
function toggleNodeDetail() {
  const body    = document.getElementById('node-detail-body');
  const chevron = document.getElementById('node-detail-chevron');
  const isOpen  = body.style.display !== 'none';
  body.style.display    = isOpen ? 'none' : 'block';
  chevron.style.transform = isOpen ? 'rotate(0deg)' : 'rotate(90deg)';
}

function toggleSection(el) {
  const content = el.nextElementSibling;
  const chevron = el.querySelector('.chevron');
  if (!content) return;
  const isOpen = content.classList.contains('open');
  content.classList.toggle('open', !isOpen);
  content.style.display = isOpen ? 'none' : 'block';
  if (chevron) chevron.style.transform = isOpen ? 'rotate(0deg)' : 'rotate(90deg)';
}

function scrollToNode(hostname) {
  const el = document.getElementById('node-' + hostname);
  if (!el) return;
  // Expand node detail section if collapsed
  const body    = document.getElementById('node-detail-body');
  const chevron = document.getElementById('node-detail-chevron');
  if (body && body.style.display === 'none') {
    body.style.display = 'block';
    if (chevron) chevron.style.transform = 'rotate(90deg)';
  }
  setTimeout(() => {
    el.scrollIntoView({ behavior: 'smooth', block: 'start' });
    el.classList.add('flash');
    setTimeout(() => el.classList.remove('flash'), 800);
  }, 50);
}

function filterTable() {
  const q = document.getElementById('node-search').value.toLowerCase();
  document.querySelectorAll('#overview-tbody tr').forEach(row => {
    const h = row.getAttribute('data-hostname') || '';
    row.style.display = h.toLowerCase().includes(q) ? '' : 'none';
  });
}

let _activeFilter = 'all';
function filterStatus(status) {
  _activeFilter = status;
  document.querySelectorAll('.filter-btn').forEach(b => {
    b.classList.toggle('active', b.getAttribute('data-filter') === status);
  });
  document.querySelectorAll('#overview-tbody tr').forEach(row => {
    const s = row.getAttribute('data-status') || '';
    row.style.display = (status === 'all' || s === status) ? '' : 'none';
  });
}

let _sortDir = {};
function sortTable(col) {
  const tbody = document.getElementById('overview-tbody');
  const rows  = Array.from(tbody.querySelectorAll('tr'));
  const dir   = (_sortDir[col] === 'asc') ? 'desc' : 'asc';
  _sortDir[col] = dir;
  rows.sort((a, b) => {
    const av = a.cells[col] ? a.cells[col].textContent.trim() : '';
    const bv = b.cells[col] ? b.cells[col].textContent.trim() : '';
    const an = parseFloat(av), bn = parseFloat(bv);
    let cmp = (!isNaN(an) && !isNaN(bn)) ? an - bn : av.localeCompare(bv);
    return dir === 'asc' ? cmp : -cmp;
  });
  rows.forEach(r => tbody.appendChild(r));
}
"""

# ─── LLM narrative summary ────────────────────────────────────────────────────

def generate_narrative(correlations: dict, nodes: list[dict]) -> str:
    """Optional: one small focused LLM call for a plain-English summary paragraph."""
    n_crit = sum(1 for n in nodes if n["overall_flag"] == "CRITICAL")
    n_warn = sum(1 for n in nodes if n["overall_flag"] == "WARNING")
    total_crit_ev = sum(n["log_critical"] for n in nodes)

    corr_summary = json.dumps({
        k: v for k, v in correlations.items()
        if k in ("uptime_split", "log_critical_outliers", "kernel_call_traces",
                 "lnet_multi_node_clients", "ib_port_errors", "sysctl_drift",
                 "load_outliers")
    }, indent=2)

    prompt = (
        f"System: You are a DDN Lustre storage expert. Write a single concise paragraph "
        f"(4-6 sentences) summarizing the key findings from this cluster diagnostic data. "
        f"Be specific — cite hostnames, counts, and what the patterns likely mean. "
        f"No bullet points. No markdown. Plain text only.\n\n"
        f"User: Cluster: {len(nodes)} nodes, {n_crit} critical, {n_warn} warning. "
        f"Total log events: {total_crit_ev} critical.\n\n"
        f"Key correlations:\n{corr_summary}"
    )

    payload = json.dumps({
        "model":  MODEL,
        "prompt": prompt,
        "stream": False,
        "think":  False,
        "options": {"num_ctx": 8192, "num_predict": 400, "temperature": 0.2},
    }).encode()

    try:
        req = urllib.request.Request(
            OLLAMA_URL, data=payload,
            headers={"Content-Type": "application/json"}, method="POST"
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            result = json.loads(resp.read())
        text = result.get("response", "").strip()
        text = re.sub(r'^```.*?```', '', text, flags=re.DOTALL).strip()
        return text
    except Exception as e:
        print(f"[WARN] LLM narrative failed: {e}", file=sys.stderr)
        return ""


# ─── Main HTML assembly ───────────────────────────────────────────────────────

def build_html(report_dir: Path, nodes: list[dict], correlations: dict,
               generated: str, narrative: str) -> str:

    appliances    = group_by_appliance(nodes)
    appliance_html = "\n".join(render_appliance(a, ns) for a, ns in appliances.items())

    narrative_html = ""
    if narrative:
        narrative_html = f"""<div style="margin:0 20px 16px;padding:14px 16px;background:{C['card']};border-left:4px solid {C['info']};border-radius:6px;font-size:13px;line-height:1.7;color:{C['text']}">
  <div style="font-size:10px;text-transform:uppercase;letter-spacing:0.08em;color:{C['muted']};margin-bottom:6px">AI Analysis Summary</div>
  {escape(narrative)}
</div>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Cluster SOS Report — {escape(generated)}</title>
<style>{CSS}</style>
</head>
<body>

<!-- Header -->
<div style="padding:16px 20px;border-bottom:1px solid {C['border']};display:flex;justify-content:space-between;align-items:center">
  <h1 style="font-size:20px">📊 Cluster SOS Report</h1>
  <span style="font-size:12px;color:{C['muted']}">{escape(generated)} &nbsp;·&nbsp; {len(nodes)} nodes</span>
</div>

<!-- Visual summary -->
{render_visual_header(nodes, report_dir)}

{narrative_html}

<!-- Anomaly panel -->
{render_anomaly_panel(correlations)}

<!-- Overview table -->
{render_overview_table(nodes)}

<!-- Node cards by appliance — collapsible -->
<div style="padding:0 20px 20px 20px">
  <div onclick="toggleNodeDetail()" style="display:flex;justify-content:space-between;align-items:center;cursor:pointer;padding:10px 14px;background:{C['border']};border-radius:6px;margin-bottom:12px;user-select:none">
    <h2 style="margin:0;font-size:15px;color:{C['muted']};text-transform:uppercase;letter-spacing:0.06em">Node Detail</h2>
    <span id="node-detail-chevron" style="color:{C['muted']};font-size:12px;transition:transform 0.2s ease">▶</span>
  </div>
  <div id="node-detail-body" style="display:none">
    {appliance_html}
  </div>
</div>

<!-- Footer -->
<div style="padding:12px 20px;border-top:1px solid {C['border']};font-size:11px;color:{C['muted']}">
  Generated by generate_report.py &nbsp;·&nbsp; {escape(generated)}
</div>

<script>{JS}</script>
</body>
</html>"""


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <report_output_dir> [--no-llm] [--debug]", file=sys.stderr)
        sys.exit(1)

    report_dir = Path(sys.argv[1]).expanduser().resolve()
    if not report_dir.is_dir():
        print(f"[ERROR] Not a directory: {report_dir}", file=sys.stderr)
        sys.exit(1)

    no_llm    = "--no-llm" in sys.argv
    debug     = "--debug" in sys.argv
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    print(f"[*] Loading report data from: {report_dir}")
    cluster_diff, nodes = load_cluster(report_dir)
    if not nodes:
        print("[ERROR] No node directories found", file=sys.stderr)
        sys.exit(1)
    print(f"[*] Loaded {len(nodes)} nodes")

    print(f"[*] Computing correlations...")
    correlations = compute_correlations(nodes)
    print(f"    {len(correlations)} groups: {', '.join(correlations.keys())}")

    narrative = ""
    if not no_llm:
        print(f"[*] Generating LLM narrative summary...")
        narrative = generate_narrative(correlations, nodes)
        if narrative:
            print(f"    done ({len(narrative.split())} words)")
        else:
            print(f"    skipped (Ollama unavailable or failed)")

    print(f"[*] Building HTML dashboard...")
    html = build_html(report_dir, nodes, correlations, generated, narrative)

    output_path = report_dir / "cluster" / OUTPUT_FILE
    output_path.write_text(html)
    size_kb = len(html) // 1024
    print(f"[*] Dashboard written → {output_path} ({size_kb} KB)")
    print(f"[*] Open: file://{output_path}")

    if debug:
        corr_path = report_dir / "cluster" / "correlations_debug.json"
        corr_path.write_text(json.dumps(correlations, indent=2))
        print(f"[DEBUG] Correlations → {corr_path}")


if __name__ == "__main__":
    main()