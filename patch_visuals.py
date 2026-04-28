#!/usr/bin/env python3
"""
patch_visuals.py — Inject a visual summary header (gauges + node heatmap)
into an existing cluster_report_ai.html.

Usage:
    python3 patch_visuals.py <report_dir>

Example:
    python3 patch_visuals.py ~/work/sos-analyzer/reports/test99

Reads:  <report_dir>/cluster/cluster_diff.json
        <report_dir>/nodes/*/resources.json  (for OST fill data)
Writes: <report_dir>/cluster/cluster_report_ai.html  (patched in-place)
"""

import json
import re
import sys
from pathlib import Path
from statistics import median

if len(sys.argv) < 2:
    print(f"Usage: {sys.argv[0]} <report_dir>")
    sys.exit(1)

report_dir   = Path(sys.argv[1]).expanduser().resolve()
diff_path    = report_dir / "cluster" / "cluster_diff.json"
html_path    = report_dir / "cluster" / "cluster_report_ai.html"

if not diff_path.exists():
    print(f"[ERROR] Not found: {diff_path}")
    sys.exit(1)
if not html_path.exists():
    print(f"[ERROR] Not found: {html_path}")
    sys.exit(1)

# ── Load cluster_diff ─────────────────────────────────────────────────────────
diff  = json.loads(diff_path.read_text())
nodes = diff["nodes"]
n     = len(nodes)

# ── Compute metrics ───────────────────────────────────────────────────────────

# Status counts
n_crit = sum(1 for x in nodes if x["overall_flag"] == "CRITICAL")
n_warn = sum(1 for x in nodes if x["overall_flag"] == "WARNING")
n_ok   = sum(1 for x in nodes if x["overall_flag"] == "OK")

# Memory — average used %
mem_pcts  = [x["mem_used_pct"] for x in nodes]
avg_mem   = round(sum(mem_pcts) / len(mem_pcts), 1)
max_mem   = max(mem_pcts)
max_mem_host = next(x["hostname"] for x in nodes if x["mem_used_pct"] == max_mem)

# CPU idle — all 0.00 on this cluster, so show load instead
# Load averages are in identity.json — use log_critical as a proxy for pressure
# We'll show a per-node load heatmap using data from resources.json
load_data = {}
nodes_dir = report_dir / "nodes"
for node_dir in sorted(nodes_dir.iterdir()):
    if node_dir.is_dir():
        ident_path = node_dir / "identity.json"
        if ident_path.exists():
            ident = json.loads(ident_path.read_text())
            hostname = ident.get("hostname", node_dir.name)
            la = ident.get("load_average", "0")
            try:
                load_1m = float(str(la).split(",")[0].strip())
            except Exception:
                load_1m = 0.0
            load_data[hostname] = load_1m

# OST fill data from resources.json
ost_fills = []
for node_dir in sorted(nodes_dir.iterdir()):
    if node_dir.is_dir():
        res_path = node_dir / "resources.json"
        if res_path.exists():
            res = json.loads(res_path.read_text())
            for d in res.get("disk", []):
                mount = d.get("mount", "")
                if "/lustre/" in mount and "ost" in mount:
                    ost_fills.append({
                        "mount":    mount,
                        "used_pct": d.get("used_pct", 0),
                        "used_tb":  round(d.get("used_gb", 0) / 1024, 1),
                        "total_tb": round(d.get("total_gb", 0) / 1024, 1),
                    })

avg_ost_fill  = round(median([x["used_pct"] for x in ost_fills]), 1) if ost_fills else 0
max_ost_fill  = max((x["used_pct"] for x in ost_fills), default=0)
total_used_pb = round(sum(x["used_tb"] for x in ost_fills) / 1024, 2)
total_cap_pb  = round(sum(x["total_tb"] for x in ost_fills) / 1024, 2)

# Log event totals
total_crit_events = sum(x["log_critical"] for x in nodes)
total_warn_events = sum(x["log_warnings"] for x in nodes)

# Max load
max_load      = max(load_data.values(), default=0)
max_load_host = max(load_data, key=load_data.get) if load_data else "unknown"
avg_load      = round(sum(load_data.values()) / len(load_data), 1) if load_data else 0

# ── SVG helpers ───────────────────────────────────────────────────────────────

def gauge_color(pct, warn=60, crit=80):
    if pct >= crit: return "#f87171"
    if pct >= warn: return "#fbbf24"
    return "#34d399"

def arc_path(cx, cy, r, start_deg, end_deg):
    """SVG arc path from start_deg to end_deg (0=top, clockwise)."""
    import math
    def pt(deg):
        rad = math.radians(deg - 90)
        return cx + r * math.cos(rad), cy + r * math.sin(rad)
    x1, y1 = pt(start_deg)
    x2, y2 = pt(end_deg)
    large = 1 if (end_deg - start_deg) > 180 else 0
    return f"M {x1:.1f} {y1:.1f} A {r} {r} 0 {large} 1 {x2:.1f} {y2:.1f}"

def semicircle_gauge(label, value, unit, pct, warn=60, crit=80,
                     sublabel="", width=180, height=120):
    """
    Semicircular gauge (180° sweep, flat bottom).
    pct: 0-100 fill amount.
    """
    import math
    cx, cy, r = width/2, height-20, (width/2) - 15
    color     = gauge_color(pct, warn, crit)
    bg_color  = "#1e2d45"

    # Track arc: 180° to 360° (left to right, flat bottom)
    def arc(deg_start, deg_end, col, stroke_w=14):
        import math
        def pt(d):
            rad = math.radians(d)
            return cx + r * math.cos(rad), cy + r * math.sin(rad)
        x1, y1 = pt(deg_start)
        x2, y2 = pt(deg_end)
        large  = 1 if abs(deg_end - deg_start) > 180 else 0
        sweep  = 1
        return (f'<path d="M {x1:.1f},{y1:.1f} A {r},{r} 0 {large},{sweep} {x2:.1f},{y2:.1f}" '
                f'fill="none" stroke="{col}" stroke-width="{stroke_w}" '
                f'stroke-linecap="round"/>')

    fill_deg = 180 + (pct / 100) * 180
    track    = arc(180, 360, bg_color, 14)
    fill     = arc(180, min(fill_deg, 359.9), color, 14)

    # Needle
    needle_deg  = 180 + (pct / 100) * 180
    needle_rad  = math.radians(needle_deg)
    nx          = cx + (r - 8) * math.cos(needle_rad)
    ny          = cy + (r - 8) * math.sin(needle_rad)
    needle_svg  = f'<line x1="{cx}" y1="{cy}" x2="{nx:.1f}" y2="{ny:.1f}" stroke="{color}" stroke-width="2" stroke-linecap="round"/>'

    return f"""<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}">
  {track}
  {fill}
  {needle_svg}
  <circle cx="{cx}" cy="{cy}" r="4" fill="{color}"/>
  <text x="{cx}" y="{cy-8}" text-anchor="middle" font-size="18" font-weight="bold" fill="{color}">{value}{unit}</text>
  <text x="{cx}" y="{cy+14}" text-anchor="middle" font-size="11" fill="#94a3b8">{label}</text>
  {f'<text x="{cx}" y="{cy+26}" text-anchor="middle" font-size="10" fill="#64748b">{sublabel}</text>' if sublabel else ''}
</svg>"""


def donut_chart(crit, warn, ok, total, width=160, height=160):
    """Status donut chart."""
    import math
    cx, cy, r_outer, r_inner = width/2, height/2, 60, 38
    segments = [
        (crit, "#f87171", "Critical"),
        (warn, "#fbbf24", "Warning"),
        (ok,   "#34d399", "OK"),
    ]
    svgs  = []
    angle = -90  # start top

    for count, color, label in segments:
        if count == 0:
            continue
        sweep = (count / total) * 360
        end   = angle + sweep

        def pt(deg):
            rad = math.radians(deg)
            return cx + r_outer * math.cos(rad), cy + r_outer * math.sin(rad)
        def pt_i(deg):
            rad = math.radians(deg)
            return cx + r_inner * math.cos(rad), cy + r_inner * math.sin(rad)

        x1o, y1o = pt(angle)
        x2o, y2o = pt(end)
        x1i, y1i = pt_i(angle)
        x2i, y2i = pt_i(end)
        large = 1 if sweep > 180 else 0

        path = (f'M {x1o:.1f},{y1o:.1f} '
                f'A {r_outer},{r_outer} 0 {large},1 {x2o:.1f},{y2o:.1f} '
                f'L {x2i:.1f},{y2i:.1f} '
                f'A {r_inner},{r_inner} 0 {large},0 {x1i:.1f},{y1i:.1f} Z')
        svgs.append(f'<path d="{path}" fill="{color}" opacity="0.9"/>')
        angle = end

    # Center text
    svgs.append(f'<text x="{cx}" y="{cy-6}" text-anchor="middle" font-size="22" font-weight="bold" fill="#e2e8f0">{total}</text>')
    svgs.append(f'<text x="{cx}" y="{cy+12}" text-anchor="middle" font-size="11" fill="#94a3b8">nodes</text>')

    # Legend below
    legend_y = height - 18
    lx = 8
    for count, color, label in segments:
        svgs.append(f'<rect x="{lx}" y="{legend_y}" width="10" height="10" fill="{color}" rx="2"/>')
        svgs.append(f'<text x="{lx+13}" y="{legend_y+9}" font-size="10" fill="#94a3b8">{count} {label}</text>')
        lx += 70

    return f'<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}">{"".join(svgs)}</svg>'


def load_heatmap(load_data, nodes):
    """
    5×4 grid heatmap of per-node load. One cell per node.
    Grouped by appliance (ddn1..ddn5), sorted by vm number.
    """
    # Group hostnames by appliance
    from collections import defaultdict
    groups = defaultdict(list)
    for node in nodes:
        h = node["hostname"]
        m = re.match(r'^(.*-ddn\d+)', h)
        key = m.group(1) if m else "unknown"
        groups[key].append(h)

    appliances = sorted(groups.keys())
    cell_w, cell_h = 52, 36
    pad_x, pad_y   = 8, 28
    cols           = max(len(v) for v in groups.values())
    rows           = len(appliances)
    width          = pad_x * 2 + cols * cell_w + (cols - 1) * 4 + 60
    height         = pad_y + rows * cell_h + (rows - 1) * 4 + 10

    # Color scale for load: 0=dark, 5=green, 20=amber, 40+=red
    def load_color(load):
        if load >= 40: return "#f87171"
        if load >= 20: return "#fbbf24"
        if load >= 5:  return "#34d399"
        return "#1e3a5f"

    svgs = []
    # Column headers (vm0, vm1, vm2, vm3)
    for ci in range(cols):
        x = pad_x + 56 + ci * (cell_w + 4) + cell_w // 2
        svgs.append(f'<text x="{x}" y="16" text-anchor="middle" font-size="9" fill="#64748b">vm{ci}</text>')

    for ri, appl in enumerate(appliances):
        y = pad_y + ri * (cell_h + 4)
        # Row label
        short = appl.split("-")[-1]  # ddn1, ddn2, etc.
        svgs.append(f'<text x="{pad_x+50}" y="{y + cell_h//2 + 4}" text-anchor="end" font-size="10" fill="#94a3b8">{short}</text>')

        for ci, hostname in enumerate(sorted(groups[appl])):
            x     = pad_x + 56 + ci * (cell_w + 4)
            load  = load_data.get(hostname, 0)
            color = load_color(load)
            flag  = next((nd["overall_flag"] for nd in nodes if nd["hostname"] == hostname), "OK")

            svgs.append(f'<rect x="{x}" y="{y}" width="{cell_w}" height="{cell_h}" '
                        f'rx="4" fill="{color}" opacity="0.85" '
                        f'style="cursor:pointer" '
                        f'onclick="document.getElementById(\'node-{hostname}\').scrollIntoView({{behavior:\'smooth\'}})" />')

            # Load value
            load_str = f"{load:.0f}" if load >= 10 else f"{load:.1f}"
            svgs.append(f'<text x="{x + cell_w//2}" y="{y + cell_h//2 + 3}" '
                        f'text-anchor="middle" font-size="12" font-weight="bold" fill="#0f1117">{load_str}</text>')

            # Tiny status dot
            dot_color = "#f87171" if flag == "CRITICAL" else "#fbbf24" if flag == "WARNING" else "#34d399"
            svgs.append(f'<circle cx="{x + cell_w - 6}" cy="{y + 6}" r="3" fill="{dot_color}"/>')

    # Legend
    ly = height - 6
    for load_val, label, color in [(0, "idle", "#1e3a5f"), (5, "light", "#34d399"),
                                    (20, "heavy", "#fbbf24"), (40, "critical", "#f87171")]:
        svgs.append(f'<rect x="{pad_x+56 + (load_val // 5) * 52}" y="{ly-8}" width="10" height="8" fill="{color}" rx="1"/>')

    return (f'<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}" '
            f'style="overflow:visible">{"".join(svgs)}</svg>')


# ── Build the summary header HTML ─────────────────────────────────────────────

mem_pct_fill   = avg_mem
mem_color      = gauge_color(avg_mem, 70, 85)
ost_color      = gauge_color(avg_ost_fill, 70, 85)

# Load gauge: normalize against cpu_count (24 cores) — 100% = load of 24
load_pct       = min(round((avg_load / 24) * 100, 1), 100)
load_color     = gauge_color(load_pct, 40, 70)

mem_gauge_svg  = semicircle_gauge("Avg Memory", f"{avg_mem}%", "", avg_mem,
                                   warn=70, crit=85,
                                   sublabel=f"max {max_mem}% ({max_mem_host})")
ost_gauge_svg  = semicircle_gauge("OST Fill", f"{avg_ost_fill}%", "", avg_ost_fill,
                                   warn=70, crit=85,
                                   sublabel=f"{total_used_pb} / {total_cap_pb} PB")
load_gauge_svg = semicircle_gauge("Avg Load", f"{avg_load}", "", load_pct,
                                   warn=40, crit=70,
                                   sublabel=f"max {max_load:.0f} ({max_load_host})")
donut_svg      = donut_chart(n_crit, n_warn, n_ok, n)
heatmap_svg    = load_heatmap(load_data, nodes)

summary_html = f"""
<!-- VISUAL SUMMARY HEADER — injected by patch_visuals.py -->
<style>
.vsummary {{
  display: flex;
  flex-wrap: wrap;
  gap: 16px;
  padding: 20px 24px;
  background: #111827;
  border-bottom: 1px solid #253048;
  align-items: flex-start;
}}
.vsummary-panel {{
  background: #1a2236;
  border: 1px solid #253048;
  border-radius: 8px;
  padding: 14px 18px;
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 4px;
}}
.vsummary-panel h4 {{
  margin: 0 0 8px 0;
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  color: #64748b;
  font-weight: 600;
}}
.vsummary-logcounts {{
  display: flex;
  gap: 20px;
  align-items: center;
}}
.logcount-block {{
  text-align: center;
}}
.logcount-val {{
  font-size: 32px;
  font-weight: 700;
  line-height: 1;
}}
.logcount-val.critical {{ color: #f87171; }}
.logcount-val.warning  {{ color: #fbbf24; }}
.logcount-label {{
  font-size: 11px;
  color: #64748b;
  margin-top: 2px;
}}
.heatmap-tip {{
  font-size: 10px;
  color: #475569;
  margin-top: 4px;
}}
</style>

<div class="vsummary">

  <div class="vsummary-panel">
    <h4>Node Status</h4>
    {donut_svg}
  </div>

  <div class="vsummary-panel">
    <h4>Memory Pressure</h4>
    {mem_gauge_svg}
  </div>

  <div class="vsummary-panel">
    <h4>CPU Load</h4>
    {load_gauge_svg}
  </div>

  <div class="vsummary-panel">
    <h4>OST Capacity</h4>
    {ost_gauge_svg}
  </div>

  <div class="vsummary-panel">
    <h4>Log Events (cluster total)</h4>
    <div class="vsummary-logcounts">
      <div class="logcount-block">
        <div class="logcount-val critical">{total_crit_events}</div>
        <div class="logcount-label">Critical Events</div>
      </div>
      <div class="logcount-block">
        <div class="logcount-val warning">{total_warn_events}</div>
        <div class="logcount-label">Warnings</div>
      </div>
    </div>
  </div>

  <div class="vsummary-panel">
    <h4>Load Heatmap (click cell → node card)</h4>
    {heatmap_svg}
    <div class="heatmap-tip">Cell color = 1-min load avg &nbsp;|&nbsp; Dot = node status</div>
  </div>

</div>
<!-- END VISUAL SUMMARY HEADER -->
"""

# ── Patch the HTML ────────────────────────────────────────────────────────────
html = html_path.read_text()

if "VISUAL SUMMARY HEADER" in html:
    print("[*] Visual summary already present — replacing...")
    html = re.sub(
        r'<!-- VISUAL SUMMARY HEADER.*?END VISUAL SUMMARY HEADER -->',
        summary_html.strip(),
        html,
        flags=re.DOTALL,
    )
else:
    # Insert after <body> tag
    html = html.replace("<body>", "<body>\n" + summary_html, 1)
    if "<body>" not in html and "<body " not in html:
        # Fallback: insert at very start of content
        html = summary_html + html

html_path.write_text(html)
print(f"[*] Patched: {html_path}")
print(f"[*] Stats:")
print(f"    Nodes: {n_crit} critical / {n_warn} warning / {n_ok} OK")
print(f"    Avg memory: {avg_mem}%  |  Max: {max_mem}% ({max_mem_host})")
print(f"    Avg load: {avg_load}  |  Max: {max_load:.1f} ({max_load_host})")
print(f"    OST fill: {avg_ost_fill}% median  |  {total_used_pb}/{total_cap_pb} PB")
print(f"    Log events: {total_crit_events} critical / {total_warn_events} warnings")
print(f"[*] Reload browser to see changes")
