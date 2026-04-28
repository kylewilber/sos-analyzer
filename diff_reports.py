#!/usr/bin/env python3
"""
diff_reports.py — Compare two sos-analyzer report runs (before vs after)

Usage:
    python3 diff_reports.py --before <report_dir> --after <report_dir> [--output <html_file>]

Example:
    python3 diff_reports.py \\
        --before reports/20260101-pre-upgrade/ \\
        --after  reports/20260415-post-upgrade/ \\
        --output reports/diff-upgrade.html

Generates a single HTML diff report showing improvements, regressions,
and unchanged items across all cluster nodes.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from html import escape
from pathlib import Path

# ─── Colors ───────────────────────────────────────────────────────────────────

C = {
    "bg":          "#0f1117",
    "card":        "#1a2236",
    "border":      "#253048",
    "text":        "#e2e8f0",
    "muted":       "#64748b",
    "critical":    "#f87171",
    "warning":     "#fbbf24",
    "ok":          "#34d399",
    "info":        "#60a5fa",
    "improved":    "#34d399",   # green
    "regressed":   "#f87171",   # red
    "unchanged":   "#64748b",   # muted
    "added":       "#a78bfa",   # purple
    "removed":     "#fb923c",   # orange
}


def flag_color(flag: str) -> str:
    return {
        "CRITICAL": C["critical"], "WARNING": C["warning"],
        "OK": C["ok"], "INFO": C["info"],
    }.get(str(flag).upper(), C["muted"])


def badge(text: str, color: str) -> str:
    return (f'<span style="background:{color};color:#0f1117;padding:2px 8px;'
            f'border-radius:4px;font-size:11px;font-weight:700">{escape(str(text))}</span>')


def flag_badge(flag: str) -> str:
    return badge(flag, flag_color(flag))


# ─── Data loading ─────────────────────────────────────────────────────────────

def load_report(report_dir: Path) -> dict:
    """Load cluster_diff.json and per-node detail JSONs."""
    diff_path = report_dir / "cluster" / "cluster_diff.json"
    if not diff_path.exists():
        print(f"[ERROR] Not found: {diff_path}", file=sys.stderr)
        sys.exit(1)

    diff = json.loads(diff_path.read_text())
    nodes_dir = report_dir / "nodes"

    node_detail = {}
    if nodes_dir.exists():
        for node_dir in sorted(nodes_dir.iterdir()):
            if not node_dir.is_dir():
                continue
            hn = node_dir.name
            node_detail[hn] = {}
            for fname in ["sysctl", "network", "lustre", "sfa", "services"]:
                fpath = node_dir / f"{fname}.json"
                if fpath.exists():
                    try:
                        node_detail[hn][fname] = json.loads(fpath.read_text())
                    except Exception:
                        pass

    return {
        "summary":     diff.get("cluster_summary", {}),
        "nodes":       {n["hostname"]: n for n in diff.get("nodes", [])},
        "node_detail": node_detail,
        "path":        report_dir,
    }


# ─── Diff logic ───────────────────────────────────────────────────────────────

# Fields where a decrease is an improvement
LOWER_IS_BETTER = {
    "mem_used_pct", "log_critical", "log_warnings",
    "ost_critical", "devices_down", "failed_services",
    "sfa_pool_not_optimal", "sfa_tz_inconsistent", "sfa_ib_fw_flag",
    "sysctl_drift_count",
}

# Flag fields — flag order: CRITICAL > WARNING > OK/INFO/N/A
FLAG_FIELDS = {
    "overall_flag", "mem_flag", "services_flag", "log_flag",
    "lustre_flag", "sfa_flag", "sysctl_flag",
}

FLAG_SEVERITY = {"CRITICAL": 3, "WARNING": 2, "OK": 1, "INFO": 1, "N/A": 0}


def flag_change(before: str, after: str) -> str:
    """improved / regressed / unchanged"""
    bs = FLAG_SEVERITY.get(str(before).upper(), 0)
    as_ = FLAG_SEVERITY.get(str(after).upper(), 0)
    if as_ < bs: return "improved"
    if as_ > bs: return "regressed"
    return "unchanged"


def numeric_change(field: str, before, after) -> str:
    try:
        b, a = float(str(before)), float(str(after))
    except (ValueError, TypeError):
        return "unchanged"
    if b == a: return "unchanged"
    diff = a - b
    if field in LOWER_IS_BETTER:
        return "improved" if diff < 0 else "regressed"
    else:
        return "improved" if diff > 0 else "regressed"


def diff_node(hostname: str, before: dict, after: dict,
              before_detail: dict, after_detail: dict) -> dict:
    """Produce a structured diff for one node."""
    changes = []

    # ── Numeric / flag fields ──
    display_fields = [
        ("kernel",           "Kernel",         "text"),
        ("uptime_days",      "Uptime (days)",   "numeric"),
        ("overall_flag",     "Overall Flag",    "flag"),
        ("mem_used_pct",     "Memory Used %",   "numeric"),
        ("mem_flag",         "Memory Flag",     "flag"),
        ("log_critical",     "Log Critical",    "numeric"),
        ("log_warnings",     "Log Warnings",    "numeric"),
        ("log_flag",         "Log Flag",        "flag"),
        ("lustre_flag",      "Lustre Flag",     "flag"),
        ("ost_critical",     "OST Critical",    "numeric"),
        ("devices_down",     "Devices Down",    "numeric"),
        ("failed_services",  "Failed Services", "numeric"),
        ("services_flag",    "Services Flag",   "flag"),
        ("sfa_flag",         "SFA Flag",        "flag"),
        ("sfa_tz_inconsistent", "SFA TZ Inconsistent", "numeric"),
        ("sysctl_flag",      "Sysctl Flag",     "flag"),
        ("sysctl_drift_count", "Sysctl Drift",  "numeric"),
    ]

    for field, label, ftype in display_fields:
        bval = before.get(field, "N/A")
        aval = after.get(field,  "N/A")
        if bval == aval:
            status = "unchanged"
        elif ftype == "flag":
            status = flag_change(str(bval), str(aval))
        elif ftype == "numeric":
            status = numeric_change(field, bval, aval)
        else:
            status = "changed"
        changes.append({
            "field":  label,
            "before": bval,
            "after":  aval,
            "status": status,
        })

    # ── Sysctl drift detail ──
    sysctl_diff = []
    b_sysctl = before_detail.get("sysctl", {})
    a_sysctl = after_detail.get("sysctl",  {})
    b_drift  = {d["param"]: d for d in b_sysctl.get("drift_flags", [])}
    a_drift  = {d["param"]: d for d in a_sysctl.get("drift_flags", [])}
    all_params = set(b_drift) | set(a_drift)
    for param in sorted(all_params):
        if param in b_drift and param not in a_drift:
            sysctl_diff.append({"param": param, "status": "improved",
                                 "before": b_drift[param]["actual"],
                                 "after":  b_drift[param]["recommended"],
                                 "note": "Fixed"})
        elif param not in b_drift and param in a_drift:
            sysctl_diff.append({"param": param, "status": "regressed",
                                 "before": a_drift[param]["recommended"],
                                 "after":  a_drift[param]["actual"],
                                 "note": "New drift"})
        elif param in b_drift and param in a_drift:
            if b_drift[param]["actual"] != a_drift[param]["actual"]:
                sysctl_diff.append({"param": param, "status": "changed",
                                     "before": b_drift[param]["actual"],
                                     "after":  a_drift[param]["actual"],
                                     "note": "Changed"})

    # ── IB port errors ──
    ib_diff = []
    b_ib = {p["ca"]: p for p in before_detail.get("network", {}).get("infiniband", [])}
    a_ib = {p["ca"]: p for p in after_detail.get("network",  {}).get("infiniband", [])}
    for ca in sorted(set(b_ib) | set(a_ib)):
        be = b_ib.get(ca, {}).get("error_count", 0)
        ae = a_ib.get(ca, {}).get("error_count", 0)
        if be != ae:
            status = "improved" if ae < be else "regressed"
            ib_diff.append({"port": ca, "before": be, "after": ae, "status": status})

    # Overall node status
    n_improved  = sum(1 for c in changes if c["status"] == "improved")
    n_regressed = sum(1 for c in changes if c["status"] == "regressed")
    if n_regressed > 0:
        node_status = "regressed"
    elif n_improved > 0:
        node_status = "improved"
    else:
        node_status = "unchanged"

    return {
        "hostname":    hostname,
        "status":      node_status,
        "changes":     changes,
        "sysctl_diff": sysctl_diff,
        "ib_diff":     ib_diff,
        "n_improved":  n_improved,
        "n_regressed": n_regressed,
    }


# ─── HTML rendering ───────────────────────────────────────────────────────────

def render_change_row(c: dict) -> str:
    status = c["status"]
    color  = {"improved": C["improved"], "regressed": C["regressed"],
               "changed": C["warning"]}.get(status, C["muted"])
    icon   = {"improved": "▲", "regressed": "▼", "changed": "~"}.get(status, "·")

    bval = c["before"]
    aval = c["after"]

    # Render flag values with color
    if str(bval).upper() in FLAG_SEVERITY:
        bval_html = flag_badge(str(bval))
    else:
        bval_html = f'<span style="color:{C["text"]}">{escape(str(bval))}</span>'

    if str(aval).upper() in FLAG_SEVERITY:
        aval_html = flag_badge(str(aval))
    else:
        aval_html = f'<span style="color:{C["text"]}">{escape(str(aval))}</span>'

    if status == "unchanged":
        return (f'<tr style="opacity:0.4">'
                f'<td style="color:{C["muted"]}">{escape(c["field"])}</td>'
                f'<td>{bval_html}</td><td>·</td><td>{aval_html}</td></tr>')

    return (f'<tr>'
            f'<td style="font-weight:600">{escape(c["field"])}</td>'
            f'<td>{bval_html}</td>'
            f'<td style="color:{color};font-weight:700;font-size:14px">{icon}</td>'
            f'<td>{aval_html}</td></tr>')


def render_node_diff(nd: dict, before_node: dict, after_node: dict) -> str:
    hostname = nd["hostname"]
    status   = nd["status"]
    color    = {"improved": C["improved"], "regressed": C["regressed"],
                "unchanged": C["muted"]}.get(status, C["muted"])

    before_flag = before_node.get("overall_flag", "N/A") if before_node else "MISSING"
    after_flag  = after_node.get("overall_flag",  "N/A") if after_node  else "MISSING"

    summary = ""
    if nd["n_improved"] or nd["n_regressed"]:
        parts = []
        if nd["n_improved"]:
            parts.append(f'<span style="color:{C["improved"]}">{nd["n_improved"]} improved</span>')
        if nd["n_regressed"]:
            parts.append(f'<span style="color:{C["regressed"]}">{nd["n_regressed"]} regressed</span>')
        summary = " &nbsp;·&nbsp; ".join(parts)

    # Change rows — show changed first, then unchanged collapsed
    changed_rows   = [c for c in nd["changes"] if c["status"] != "unchanged"]
    unchanged_rows = [c for c in nd["changes"] if c["status"] == "unchanged"]

    changed_html = "\n".join(render_change_row(c) for c in changed_rows)
    unchanged_html = "\n".join(render_change_row(c) for c in unchanged_rows)

    # Sysctl diff table
    sysctl_html = ""
    if nd["sysctl_diff"]:
        rows = ""
        for d in nd["sysctl_diff"]:
            sc = {"improved": C["improved"], "regressed": C["regressed"],
                  "changed": C["warning"]}.get(d["status"], C["muted"])
            rows += (f'<tr><td style="font-family:monospace;font-size:11px">{escape(d["param"])}</td>'
                     f'<td>{escape(str(d["before"]))}</td>'
                     f'<td style="color:{sc};font-weight:600">{escape(str(d["after"]))}</td>'
                     f'<td style="color:{sc}">{escape(d["note"])}</td></tr>')
        sysctl_html = f"""
<div style="margin-top:10px">
  <div style="font-size:11px;text-transform:uppercase;color:{C['muted']};margin-bottom:4px">Sysctl Changes</div>
  <table style="width:100%;border-collapse:collapse;font-size:12px">
    <thead><tr style="color:{C['muted']}"><th style="text-align:left;padding:3px">Parameter</th>
    <th>Before</th><th>After</th><th>Status</th></tr></thead>
    <tbody>{rows}</tbody>
  </table>
</div>"""

    # IB diff
    ib_html = ""
    if nd["ib_diff"]:
        rows = ""
        for d in nd["ib_diff"]:
            ic = C["improved"] if d["status"] == "improved" else C["regressed"]
            rows += (f'<tr><td style="font-family:monospace">{escape(d["port"])}</td>'
                     f'<td>{d["before"]}</td>'
                     f'<td style="color:{ic};font-weight:600">{d["after"]}</td></tr>')
        ib_html = f"""
<div style="margin-top:10px">
  <div style="font-size:11px;text-transform:uppercase;color:{C['muted']};margin-bottom:4px">IB Port Error Changes</div>
  <table style="width:100%;border-collapse:collapse;font-size:12px">
    <thead><tr style="color:{C['muted']}"><th style="text-align:left;padding:3px">Port</th>
    <th>Before</th><th>After</th></tr></thead>
    <tbody>{rows}</tbody>
  </table>
</div>"""

    unchanged_section = ""
    if unchanged_rows:
        unchanged_section = f"""
<div onclick="this.nextElementSibling.style.display=this.nextElementSibling.style.display==='none'?'block':'none'"
     style="cursor:pointer;color:{C['muted']};font-size:11px;margin-top:8px;user-select:none">
  ▶ {len(unchanged_rows)} unchanged fields
</div>
<div style="display:none">
  <table style="width:100%;border-collapse:collapse;font-size:12px;margin-top:4px">
    <tbody>{unchanged_html}</tbody>
  </table>
</div>"""

    return f"""
<div style="background:{C['card']};border:1px solid {color};border-radius:8px;
     margin-bottom:10px;overflow:hidden">
  <div style="display:flex;justify-content:space-between;align-items:center;
       padding:10px 14px;background:{C['border']}">
    <div style="display:flex;align-items:center;gap:10px">
      <h3 style="margin:0;font-size:13px;font-family:monospace">{escape(hostname)}</h3>
      {flag_badge(before_flag)} → {flag_badge(after_flag)}
    </div>
    <div style="font-size:12px">{summary}</div>
  </div>
  <div style="padding:10px 14px">
    {"<p style='color:" + C['muted'] + ";font-size:12px'>No changes detected</p>" if not changed_rows and not nd['sysctl_diff'] and not nd['ib_diff'] else ""}
    {"<table style='width:100%;border-collapse:collapse;font-size:12px'><thead><tr style='color:" + C['muted'] + "'><th style='text-align:left;padding:3px'>Field</th><th>Before</th><th></th><th>After</th></tr></thead><tbody>" + changed_html + "</tbody></table>" if changed_rows else ""}
    {sysctl_html}
    {ib_html}
    {unchanged_section}
  </div>
</div>"""


def render_summary_bar(before: dict, after: dict,
                       node_diffs: list[dict]) -> str:
    """Top-level summary cards."""
    n_improved  = sum(1 for n in node_diffs if n["status"] == "improved")
    n_regressed = sum(1 for n in node_diffs if n["status"] == "regressed")
    n_unchanged = sum(1 for n in node_diffs if n["status"] == "unchanged")
    n_added     = sum(1 for n in node_diffs if n.get("added"))
    n_removed   = sum(1 for n in node_diffs if n.get("removed"))

    b_crit = sum(1 for n in before["nodes"].values() if n.get("overall_flag") == "CRITICAL")
    a_crit = sum(1 for n in after["nodes"].values()  if n.get("overall_flag") == "CRITICAL")
    b_warn = sum(1 for n in before["nodes"].values() if n.get("overall_flag") == "WARNING")
    a_warn = sum(1 for n in after["nodes"].values()  if n.get("overall_flag") == "WARNING")

    crit_color = C["improved"] if a_crit < b_crit else C["regressed"] if a_crit > b_crit else C["muted"]
    warn_color = C["improved"] if a_warn < b_warn else C["regressed"] if a_warn > b_warn else C["muted"]

    def stat_card(label, before_val, after_val, color):
        arrow = "▲" if after_val > before_val else "▼" if after_val < before_val else "·"
        return f"""<div style="background:{C['card']};border:1px solid {C['border']};
             border-radius:8px;padding:12px 16px;text-align:center;flex:1 1 120px">
          <div style="font-size:10px;text-transform:uppercase;color:{C['muted']};margin-bottom:4px">{label}</div>
          <div style="font-size:22px;font-weight:700;color:{color}">{before_val} → {after_val}</div>
          <div style="font-size:18px;color:{color}">{arrow}</div>
        </div>"""

    def count_card(label, val, color):
        return f"""<div style="background:{C['card']};border:1px solid {C['border']};
             border-radius:8px;padding:12px 16px;text-align:center;flex:1 1 100px">
          <div style="font-size:10px;text-transform:uppercase;color:{C['muted']};margin-bottom:4px">{label}</div>
          <div style="font-size:28px;font-weight:700;color:{color}">{val}</div>
        </div>"""

    return f"""<div style="display:flex;flex-wrap:wrap;gap:10px;padding:16px 20px;
         background:#111827;border-bottom:1px solid {C['border']}">
      {stat_card("Critical Nodes", b_crit, a_crit, crit_color)}
      {stat_card("Warning Nodes",  b_warn, a_warn, warn_color)}
      {count_card("Improved",  n_improved,  C['improved'])}
      {count_card("Regressed", n_regressed, C['regressed'])}
      {count_card("Unchanged", n_unchanged, C['muted'])}
      {"" if not n_added   else count_card("Added",   n_added,   C['added'])}
      {"" if not n_removed else count_card("Removed", n_removed, C['removed'])}
    </div>"""


# ─── Main report assembly ─────────────────────────────────────────────────────

def build_diff_report(before: dict, after: dict, generated: str) -> str:
    b_gen = before["summary"].get("generated", "unknown")
    a_gen = after["summary"].get("generated", "unknown")
    b_nodes = len(before["nodes"])
    a_nodes = len(after["nodes"])

    # Match nodes
    all_hostnames = sorted(set(before["nodes"]) | set(after["nodes"]))
    node_diffs = []

    for hostname in all_hostnames:
        bn = before["nodes"].get(hostname)
        an = after["nodes"].get(hostname)
        bd = before["node_detail"].get(hostname, {})
        ad = after["node_detail"].get(hostname, {})

        if bn is None:
            node_diffs.append({
                "hostname": hostname, "status": "added",
                "added": True, "changes": [], "sysctl_diff": [], "ib_diff": [],
                "n_improved": 0, "n_regressed": 0,
            })
        elif an is None:
            node_diffs.append({
                "hostname": hostname, "status": "removed",
                "removed": True, "changes": [], "sysctl_diff": [], "ib_diff": [],
                "n_improved": 0, "n_regressed": 0,
            })
        else:
            nd = diff_node(hostname, bn, an, bd, ad)
            node_diffs.append(nd)

    summary_bar = render_summary_bar(before, after, node_diffs)

    # Sort: regressed first, then improved, then unchanged
    order = {"regressed": 0, "improved": 1, "added": 2, "removed": 3, "unchanged": 4}
    node_diffs.sort(key=lambda n: order.get(n["status"], 5))

    # Node diff cards
    node_cards_html = ""
    for nd in node_diffs:
        hostname = nd["hostname"]
        if nd.get("added"):
            node_cards_html += f"""<div style="background:{C['card']};border:1px solid {C['added']};
                border-radius:8px;padding:12px 16px;margin-bottom:10px">
                <span style="font-family:monospace;font-weight:600">{escape(hostname)}</span>
                &nbsp; {badge("NEW NODE", C['added'])}
                </div>"""
        elif nd.get("removed"):
            node_cards_html += f"""<div style="background:{C['card']};border:1px solid {C['removed']};
                border-radius:8px;padding:12px 16px;margin-bottom:10px">
                <span style="font-family:monospace;font-weight:600">{escape(hostname)}</span>
                &nbsp; {badge("REMOVED", C['removed'])}
                </div>"""
        else:
            bn = before["nodes"].get(hostname, {})
            an = after["nodes"].get(hostname,  {})
            node_cards_html += render_node_diff(nd, bn, an)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Cluster Diff Report — {escape(generated)}</title>
<style>
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{ background: {C['bg']}; color: {C['text']};
       font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
       line-height: 1.5; font-size: 13px; }}
table {{ width: 100%; border-collapse: collapse; }}
th, td {{ padding: 5px 8px; text-align: left;
          border-bottom: 1px solid {C['border']}; }}
th {{ color: {C['muted']}; font-weight: 600; font-size: 11px;
     text-transform: uppercase; letter-spacing: 0.04em; }}
::-webkit-scrollbar {{ width: 6px; height: 6px; }}
::-webkit-scrollbar-track {{ background: {C['bg']}; }}
::-webkit-scrollbar-thumb {{ background: {C['border']}; border-radius: 3px; }}
</style>
</head>
<body>

<div style="padding:14px 20px;border-bottom:1px solid {C['border']};
     display:flex;justify-content:space-between;align-items:center">
  <h1 style="font-size:18px">📊 Cluster Diff Report</h1>
  <span style="font-size:11px;color:{C['muted']}">Generated {escape(generated)}</span>
</div>

<div style="padding:12px 20px;background:#111827;border-bottom:1px solid {C['border']};
     display:flex;gap:32px;font-size:12px">
  <div><span style="color:{C['muted']}">BEFORE &nbsp;</span>
       <span style="font-family:monospace">{escape(str(before['path'].name))}</span>
       <span style="color:{C['muted']}"> &nbsp;·&nbsp; {escape(b_gen)} &nbsp;·&nbsp; {b_nodes} nodes</span>
  </div>
  <div style="color:{C['muted']}">→</div>
  <div><span style="color:{C['muted']}">AFTER &nbsp;</span>
       <span style="font-family:monospace">{escape(str(after['path'].name))}</span>
       <span style="color:{C['muted']}"> &nbsp;·&nbsp; {escape(a_gen)} &nbsp;·&nbsp; {a_nodes} nodes</span>
  </div>
</div>

{summary_bar}

<div style="padding:16px 20px">
  <h2 style="font-size:13px;text-transform:uppercase;letter-spacing:0.06em;
       color:{C['muted']};margin-bottom:12px">Per-Node Changes
    <span style="font-size:10px;font-weight:normal;margin-left:8px">
      (regressed first · click unchanged fields to expand)
    </span>
  </h2>
  {node_cards_html}
</div>

<div style="padding:10px 20px;border-top:1px solid {C['border']};
     font-size:11px;color:{C['muted']}">
  Generated by diff_reports.py &nbsp;·&nbsp; {escape(generated)}
</div>

</body>
</html>"""


# ─── CLI ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        prog="sos-analyzer-diff",
        description="Compare two sos-analyzer report runs (before vs after).",
    )
    parser.add_argument("--before", "-b", required=True,
                        help="Before report directory")
    parser.add_argument("--after",  "-a", required=True,
                        help="After report directory")
    parser.add_argument("--output", "-o", default=None,
                        help="Output HTML file (default: <after_dir>/cluster/diff_report.html)")
    args = parser.parse_args()

    before_dir = Path(args.before).expanduser().resolve()
    after_dir  = Path(args.after).expanduser().resolve()

    print(f"[*] Loading BEFORE: {before_dir}")
    before = load_report(before_dir)
    print(f"    {len(before['nodes'])} nodes, generated: {before['summary'].get('generated','?')}")

    print(f"[*] Loading AFTER:  {after_dir}")
    after = load_report(after_dir)
    print(f"    {len(after['nodes'])} nodes, generated: {after['summary'].get('generated','?')}")

    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    print(f"[*] Computing diff...")
    html = build_diff_report(before, after, generated)

    if args.output:
        out_path = Path(args.output).expanduser().resolve()
    else:
        out_path = after_dir / "cluster" / "diff_report.html"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html)
    size_kb = len(html) // 1024
    print(f"[*] Diff report written → {out_path} ({size_kb} KB)")
    print(f"[*] Open: file://{out_path}")


if __name__ == "__main__":
    main()
