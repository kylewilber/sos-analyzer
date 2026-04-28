#!/usr/bin/env python3
"""
analyze_cluster.py — LLM-powered interactive HTML dashboard for SOS cluster reports.

Architecture:
  - One LLM call per SFA appliance (4 nodes each) → generates node cards + appliance summary
  - One final LLM call → generates HTML skeleton (CSS, JS, header, anomaly panel, overview table)
  - Python stitches all parts into a single self-contained HTML file

Usage:
    python3 analyze_cluster.py <report_output_dir> [--debug]

Example:
    python3 analyze_cluster.py ~/work/sos-analyzer/reports/test99

Writes:
    <dir>/cluster/cluster_report_ai.html

Requires: Ollama at http://172.16.0.252:11434 with qwen3-coder:30b
          OLLAMA_CONTEXT_LENGTH=32768 in ollama systemd service
"""

import json
import re
import sys
import urllib.request
import urllib.error
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import median, stdev

# ─── Config ───────────────────────────────────────────────────────────────────

OLLAMA_URL        = "http://172.16.0.252:11434/api/generate"
MODEL             = "qwen3-coder:30b"
OUTPUT_FILE       = "cluster_report_ai.html"
NODE_FILES        = ["identity", "resources", "network", "logs", "sfa"]
MAX_CRIT_EVENTS   = 3
MAX_CLIENT_EVENTS = 3
MAX_EVENT_LEN     = 120

# ─── Data loading ─────────────────────────────────────────────────────────────

def load_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def load_node(node_dir: Path) -> dict:
    data  = {name: load_json(node_dir / f"{name}.json") for name in NODE_FILES}
    ident = data["identity"]
    res   = data["resources"]
    mem   = res.get("memory", {})
    cpu   = res.get("cpu", {})
    net   = data["network"]
    logs  = data["logs"]
    sfa   = data["sfa"]

    # Sysctl — optional, may not exist on older runs
    sysctl = load_json(node_dir / "sysctl.json")

    ib_ports      = net.get("infiniband", [])
    crit_events   = logs.get("critical_events", logs.get("critical", []))
    client_events = logs.get("client_events", [])

    disk_warnings = []
    for d in res.get("disk", []):
        fs = d.get("filesystem", "")
        if any(x in fs for x in ("tmpfs", "devtmpfs")):
            continue
        if d.get("flag") not in ("OK", None):
            disk_warnings.append({
                "mount":    d.get("mount"),
                "used_pct": d.get("used_pct", 0),
                "used_gb":  round(d.get("used_gb", 0), 1),
                "total_gb": round(d.get("total_gb", 0), 1),
                "flag":     d.get("flag", "OK"),
            })

    ib_errors = [
        {"port": p.get("ca"), "errors": p.get("error_count"),
         "detail": p.get("error_detail"), "fw": p.get("firmware")}
        for p in ib_ports if p.get("error_count", 0) > 0
    ]

    # Sysctl drift findings — compact list for the card prompt
    sysctl_drift = sysctl.get("drift_flags", [])

    return {
        "hostname":          ident.get("hostname", node_dir.name),
        "os":                f"{ident.get('os_name','')} {ident.get('os_version','')}".strip(),
        "kernel":            ident.get("kernel", ""),
        "cpu_count":         ident.get("cpu_count"),
        "uptime_days":       ident.get("uptime_days"),
        "load_average":      ident.get("load_average"),
        "collection_date":   ident.get("collection_date"),

        "mem_used_pct":      mem.get("used_pct", 0),
        "mem_used_gb":       mem.get("used_gb"),
        "mem_total_gb":      mem.get("total_gb"),
        "swap_used_pct":     mem.get("swap_used_pct", 0),

        "cpu_usr_pct":       cpu.get("usr_pct", "0.00"),
        "cpu_iowait_pct":    cpu.get("iowait_pct", "0.00"),
        "cpu_idle_pct":      cpu.get("idle_pct", "0.00"),

        "disk_warnings":     disk_warnings,
        "ib_flag":           net.get("ib_flag", "OK"),
        "ib_errors":         ib_errors,

        "log_flag":          logs.get("flag", "OK"),
        "log_critical":      logs.get("critical_count", logs.get("log_critical", 0)),
        "log_warnings":      logs.get("warning_count",  logs.get("log_warnings", 0)),
        "log_client_events": logs.get("client_event_count", 0),
        "critical_events":   [str(e)[:MAX_EVENT_LEN] for e in
                              (crit_events[:MAX_CRIT_EVENTS] if isinstance(crit_events, list) else [])],
        "client_events":     [str(e)[:MAX_EVENT_LEN] for e in
                              (client_events[:MAX_CLIENT_EVENTS] if isinstance(client_events, list) else [])],

        "sfa_flag":             sfa.get("flag", "OK"),
        "sfa_tz_inconsistent":  sfa.get("tz_inconsistent", 0),
        "sfa_pool_not_optimal": sfa.get("pool_not_optimal", 0),
        "sfa_ib_fw_summary":    sfa.get("ib_fw_summary", ""),

        "sysctl_flag":        sysctl.get("flag", "N/A") if sysctl.get("available") else "N/A",
        "sysctl_drift_count": sysctl.get("drift_count", 0),
        "sysctl_drift":       sysctl_drift,

        "overall_flag":      "OK",  # filled in after cluster_diff load
    }


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


# ─── Appliance grouping ───────────────────────────────────────────────────────

def group_by_appliance(nodes: list[dict]) -> dict[str, list[dict]]:
    groups = defaultdict(list)
    for node in nodes:
        m = re.match(r'^(.*-ddn\d+)', node["hostname"])
        key = m.group(1) if m else "unknown"
        groups[key].append(node)
    return dict(sorted(groups.items()))


# ─── Pre-computed correlations ────────────────────────────────────────────────

def compute_correlations(nodes: list[dict]) -> dict:
    findings = {}

    # Uptime split
    uptime_groups = defaultdict(list)
    for n in nodes:
        uptime_groups[n["uptime_days"]].append(n["hostname"])
    if len(uptime_groups) > 1:
        findings["uptime_split"] = {str(k): v for k, v in sorted(uptime_groups.items())}

    # Log outliers (> 2σ)
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

    # LNet client IPs appearing on 3+ nodes
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
        for e in n.get("ib_errors", []):
            ib_errors.setdefault(n["hostname"], []).append(e)
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
        d["used_pct"] for n in nodes for d in n.get("disk_warnings", [])
        if "/lustre/" in str(d.get("mount", "")) and "ost" in str(d.get("mount", ""))
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

    # Sysctl drift — collect per-param findings across all nodes
    sysctl_warning_nodes = [n["hostname"] for n in nodes if n.get("sysctl_flag") == "WARNING"]
    sysctl_info_nodes    = [n["hostname"] for n in nodes if n.get("sysctl_flag") == "INFO"]
    if sysctl_warning_nodes or sysctl_info_nodes:
        param_to_nodes = defaultdict(list)
        for n in nodes:
            for drift in n.get("sysctl_drift", []):
                param = drift.get("param", "")
                if param:
                    param_to_nodes[param].append({
                        "hostname":    n["hostname"],
                        "actual":      drift.get("actual", ""),
                        "recommended": drift.get("recommended", ""),
                    })
        findings["sysctl_drift"] = {
            "warning_nodes": sysctl_warning_nodes,
            "info_nodes":    sysctl_info_nodes,
            "params":        dict(param_to_nodes),
        }

    return findings


# ─── Appliance summary ────────────────────────────────────────────────────────

def appliance_summary(appliance: str, nodes: list[dict]) -> dict:
    flags         = [n["overall_flag"] for n in nodes]
    total_crit    = sum(n["log_critical"] for n in nodes)
    total_warn    = sum(n["log_warnings"] for n in nodes)
    ib_issues     = [n["hostname"] for n in nodes if n["ib_flag"] != "OK"]
    sfa_issues    = [n["hostname"] for n in nodes if n["sfa_flag"] != "OK"]
    sysctl_issues = [n["hostname"] for n in nodes if n.get("sysctl_flag") in ("WARNING", "INFO")]
    uptimes       = list(set(n["uptime_days"] for n in nodes))
    return {
        "appliance":          appliance,
        "node_count":         len(nodes),
        "hostnames":          [n["hostname"] for n in nodes],
        "overall_flags":      dict(zip([n["hostname"] for n in nodes], flags)),
        "critical_count":     flags.count("CRITICAL"),
        "warning_count":      flags.count("WARNING"),
        "ok_count":           flags.count("OK"),
        "total_log_critical": total_crit,
        "total_log_warnings": total_warn,
        "ib_issues":          ib_issues,
        "sfa_issues":         sfa_issues,
        "sysctl_issues":      sysctl_issues,
        "uptimes_days":       uptimes,
    }


# ─── LLM call ─────────────────────────────────────────────────────────────────

SYSTEM_PREAMBLE = (
    "You are an expert web developer. "
    "Output ONLY raw HTML fragments — no markdown fences, no explanation, no preamble. "
    "Do not include <!DOCTYPE>, <html>, <head>, <body> tags unless explicitly asked. "
    "Output must be valid HTML that can be embedded directly into a page."
)

SYSTEM_PREAMBLE_SKELETON = SYSTEM_PREAMBLE


def ollama_generate(prompt: str, system: str, label: str,
                    num_predict: int = 12000) -> str:
    full_prompt = f"System: {system}\n\nUser: {prompt}"

    payload = json.dumps({
        "model":  MODEL,
        "prompt": full_prompt,
        "stream": True,
        "think":  False,
        "options": {
            "num_ctx":     32768,
            "num_predict": num_predict,
            "temperature": 0.15,
        },
    }).encode()

    req = urllib.request.Request(
        OLLAMA_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    print(f"[*] Generating {label}...")
    tokens     = []
    gen_tokens = 0

    try:
        with urllib.request.urlopen(req, timeout=1800) as resp:
            for line in resp:
                line = line.strip()
                if not line:
                    continue
                try:
                    chunk = json.loads(line)
                except json.JSONDecodeError:
                    continue
                token = chunk.get("response", "")
                if token:
                    tokens.append(token)
                    gen_tokens += 1
                    if gen_tokens % 1000 == 0:
                        print(f"    ... {gen_tokens} tokens", flush=True)
                if chunk.get("done"):
                    gen_tokens = chunk.get("eval_count", gen_tokens)
                    break
    except urllib.error.URLError as e:
        print(f"[ERROR] Ollama unreachable: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"    done — {gen_tokens} tokens generated")
    content = "".join(tokens)
    content = re.sub(r'^```html?\s*\n?', '', content.strip(), flags=re.IGNORECASE)
    content = re.sub(r'\n?```\s*$',      '', content.strip())
    return content.strip()


# ─── Prompt builders ──────────────────────────────────────────────────────────

def prompt_appliance_cards(appliance: str, nodes: list[dict]) -> str:
    node_json = json.dumps(nodes, separators=(",", ":"))
    return f"""Generate HTML node cards for SFA appliance "{appliance}" ({len(nodes)} nodes).

OUTPUT: HTML fragment only. No <!DOCTYPE>, no <html>, no <head>, no <body>.
        Just the card divs, ready to embed in a dashboard page.

THEME: dark — background #0f1117, cards #1a2236, borders #253048, text #e2e8f0
STATUS COLORS: CRITICAL=#f87171, WARNING=#fbbf24, OK=#34d399, INFO=#60a5fa, muted=#64748b

APPLIANCE SECTION STRUCTURE:
<div class="appliance-group" id="appliance-{appliance}">
  <div class="appliance-header">
    <h2>{appliance}</h2>
    <span>N nodes | X critical | Y warning</span>
  </div>
  <!-- one node-card per node below -->
</div>

NODE CARD STRUCTURE (repeat for each node):
<div class="node-card" id="node-HOSTNAME" data-status="OVERALL_FLAG" data-hostname="HOSTNAME">
  <div class="node-header">
    <h3>HOSTNAME</h3>
    <span class="badge badge-LOWERCASEFLAG">OVERALL_FLAG</span>
  </div>
  <div class="node-summary">
    <div class="summary-grid">
      <div>Uptime: Xd | Load: X.XX</div>
      <div>Mem: X% (X GB / X GB) | Swap: X%</div>
      <div>CPU: usr=X% iowait=X% idle=X%</div>
      <div>IB: FLAG | Logs: X crit / X warn / X client</div>
      <div>Sysctl: FLAG (X drift findings)</div>
    </div>
    <div class="mem-bar"><div class="mem-fill" style="width:X%"></div></div>
  </div>

  <!-- Collapsible sections -->
  <div class="collapsible" onclick="toggleSection(this)">
    <span>Disk Warnings</span><span class="chevron">▶</span>
  </div>
  <div class="collapsible-content">
    <!-- disk_warnings table or "All disks OK" -->
  </div>

  <div class="collapsible" onclick="toggleSection(this)">
    <span>InfiniBand</span><span class="chevron">▶</span>
  </div>
  <div class="collapsible-content">
    <!-- ib_errors table or "All IB ports clean" -->
  </div>

  <div class="collapsible" onclick="toggleSection(this)">
    <span>Log Events</span><span class="chevron">▶</span>
  </div>
  <div class="collapsible-content">
    <!-- critical_events in <pre> or "No critical events" -->
  </div>

  <div class="collapsible" onclick="toggleSection(this)">
    <span>Client NI Events</span><span class="chevron">▶</span>
  </div>
  <div class="collapsible-content">
    <!-- client_events in <pre> or "No client events" -->
  </div>

  <div class="collapsible" onclick="toggleSection(this)">
    <span>SFA</span><span class="chevron">▶</span>
  </div>
  <div class="collapsible-content">
    <!-- sfa_flag, sfa_tz_inconsistent, sfa_pool_not_optimal, sfa_ib_fw_summary -->
  </div>

  <div class="collapsible" onclick="toggleSection(this)">
    <span>Sysctl Tuning</span><span class="chevron">▶</span>
  </div>
  <div class="collapsible-content">
    <!-- If sysctl_drift_count == 0 or sysctl_flag == "N/A": show "No drift findings"
         Otherwise: table with columns Param | Actual | Recommended
         for each entry in sysctl_drift. Color the Actual cell amber if it differs. -->
  </div>
</div>

Fill in all values from NODE DATA. Color badges and stats by severity.
For Sysctl summary line: color flag text by severity (WARNING=amber, INFO=blue, OK=green, N/A=muted).

NODE DATA:
{node_json}
"""


def prompt_skeleton(cluster_diff: dict, nodes: list[dict],
                    correlations: dict,
                    appliance_summaries: list[dict],
                    generated: str) -> str:
    n_nodes   = len(nodes)
    corr_json = json.dumps(correlations, indent=2)
    summ_json = json.dumps(appliance_summaries, indent=2)

    table_rows = []
    for n in nodes:
        la = str(n.get("load_average", "")).split(",")[0].strip()
        table_rows.append({
            "hostname":     n["hostname"],
            "status":       n["overall_flag"],
            "uptime":       n["uptime_days"],
            "load":         la,
            "mem_pct":      n["mem_used_pct"],
            "cpu_idle":     n["cpu_idle_pct"],
            "log_crit":     n["log_critical"],
            "log_warn":     n["log_warnings"],
            "ib":           n["ib_flag"],
            "sfa":          n["sfa_flag"],
            "sysctl":       n.get("sysctl_flag", "N/A"),
            "sysctl_drift": n.get("sysctl_drift_count", 0),
        })
    table_json = json.dumps(table_rows, separators=(",", ":"))

    return f"""Generate an HTML fragment for a {n_nodes}-node DDN Lustre cluster SOS dashboard.
Generated: {generated}

OUTPUT: HTML fragment only. No <!DOCTYPE>, no <html>, no <head>, no <body> tags.
        Output three sections in order: <style>, then the dashboard body content, then <script>.

THEME: dark background #0f1117, cards #1a2236, borders #253048, text #e2e8f0
STATUS COLORS: CRITICAL=#f87171 WARNING=#fbbf24 OK=#34d399 INFO=#60a5fa muted=#64748b

SECTION 1 — <style> tag with ALL CSS:
   body, h1, h2, h3, p, table, th, td, pre
   .badge .badge-critical .badge-warning .badge-ok .badge-info
   .node-card .node-header .node-summary .summary-grid
   .appliance-group .appliance-header
   .collapsible .collapsible-content .chevron (rotate 90deg when open)
   .mem-bar .mem-fill
   .anomaly-panel .anomaly-card .anomaly-card-critical .anomaly-card-warning .anomaly-card-info
   .overview-table th[sortable] cursor:pointer, sort arrows
   .filter-bar input, .filter-buttons button
   .flash animation (brief border highlight)

SECTION 2 — dashboard body content in this order:
   a) Header div: "📊 Cluster SOS Report" h1, subtitle with generated timestamp and node count
   b) Anomaly panel: one .anomaly-card per key in CORRELATIONS DATA.
      Severity map:
        log_critical_outliers → critical
        kernel_call_traces → critical
        ib_port_errors → critical
        log_warning_outliers → warning
        uptime_split → warning
        lnet_multi_node_clients → warning
        load_outliers → warning
        sysctl_drift → warning (list warning_nodes, then for each param in params: show param name, recommended value, and which nodes have the wrong value)
        ib_fw_variants → info
        ost_fill_stats → info
      Each card: human-readable title, severity badge, specific data from the JSON.
   c) Overview table section:
      - Search input (filters rows by hostname)
      - Status filter buttons: All | Critical | Warning | OK
      - Table columns: Hostname | Status | Uptime(d) | Load | Mem% | CPU Idle% | Log Crit | Log Warn | IB | SFA | Sysctl
      - Sysctl column: show sysctl flag colored by severity; if sysctl_drift > 0 append "(Xd)" in muted text
      - Each th has onclick sort. Each td colored by value severity.
      - Each tr has data-status and data-hostname. Click scrolls to node card.
      - Populate from TABLE DATA below.
   d) <div id="node-cards-container"><!-- NODE_CARDS_PLACEHOLDER --></div>
   e) Footer: "Generated by {MODEL} via analyze_cluster.py | {generated}"

SECTION 3 — <script> tag with ALL JavaScript:
   toggleSection(el): toggle .open on collapsible-content sibling, rotate chevron
   Table sort: th onclick sorts tbody rows asc/desc, updates arrow indicator
   Search: input oninput filters tr rows by data-hostname contains value
   Filter buttons: onclick shows only tr rows matching data-status (or all)
   Row click: document.getElementById('node-'+hostname).scrollIntoView(), add .flash class

CORRELATIONS DATA:
{corr_json}

APPLIANCE SUMMARIES:
{summ_json}

TABLE DATA:
{table_json}
"""


# ─── Validation ───────────────────────────────────────────────────────────────

def validate_html(html: str) -> tuple[bool, str]:
    if not html.lower().startswith("<!doctype"):
        return False, "Does not start with <!DOCTYPE html>"
    if not html.lower().rstrip().endswith("</html>"):
        return False, "Does not end with </html> — likely truncated"
    if "<body" not in html.lower():
        return False, "Missing <body> tag"
    return True, "OK"


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <report_output_dir> [--debug]", file=sys.stderr)
        sys.exit(1)

    report_dir = Path(sys.argv[1]).expanduser().resolve()
    if not report_dir.is_dir():
        print(f"[ERROR] Not a directory: {report_dir}", file=sys.stderr)
        sys.exit(1)

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

    appliances = group_by_appliance(nodes)
    print(f"[*] Appliances: {', '.join(appliances.keys())}")

    all_cards_html = []
    summaries      = []

    for appliance, appl_nodes in appliances.items():
        prompt = prompt_appliance_cards(appliance, appl_nodes)

        if debug:
            dbg = report_dir / "cluster" / f"debug_prompt_{appliance}.txt"
            dbg.write_text(prompt)
            print(f"  [DEBUG] {appliance} prompt → {dbg} (~{len(prompt)//4} tokens)")

        cards_html = ollama_generate(
            prompt, SYSTEM_PREAMBLE,
            label=f"cards for {appliance} ({len(appl_nodes)} nodes)",
            num_predict=12000,
        )
        all_cards_html.append(cards_html)
        summaries.append(appliance_summary(appliance, appl_nodes))

    combined_cards = "\n\n".join(all_cards_html)

    skel_prompt = prompt_skeleton(
        cluster_diff, nodes, correlations, summaries, generated
    )

    if debug:
        dbg = report_dir / "cluster" / "debug_prompt_skeleton.txt"
        dbg.write_text(skel_prompt)
        print(f"  [DEBUG] skeleton prompt → {dbg} (~{len(skel_prompt)//4} tokens)")

    skeleton_html = ollama_generate(
        skel_prompt, SYSTEM_PREAMBLE_SKELETON,
        label="HTML skeleton (header + anomaly panel + overview table + JS/CSS)",
        num_predict=14000,
    )

    # Inject cards
    if "<!-- NODE_CARDS_PLACEHOLDER -->" in skeleton_html:
        skeleton_html = skeleton_html.replace(
            "<!-- NODE_CARDS_PLACEHOLDER -->", combined_cards, 1)
    else:
        print("[WARN] Placeholder missing — injecting at end of content")
        skeleton_html += "\n" + combined_cards

    # Ensure toggleSection is always present — coordination gap between card/skeleton calls
    toggle_js = """
<script>
function toggleSection(el) {
  const content = el.nextElementSibling;
  const chevron = el.querySelector('.chevron');
  if (!content) return;
  const isOpen = content.classList.contains('open');
  content.classList.toggle('open', !isOpen);
  content.style.display = isOpen ? 'none' : 'block';
  if (chevron) {
    chevron.style.display = 'inline-block';
    chevron.style.transition = 'transform 0.2s ease';
    chevron.style.transform = isOpen ? 'rotate(0deg)' : 'rotate(90deg)';
  }
}
</script>"""
    if "function toggleSection" not in skeleton_html:
        if "</body>" in skeleton_html:
            skeleton_html = skeleton_html.replace("</body>", toggle_js + "\n</body>", 1)
        else:
            skeleton_html += toggle_js

    final_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Cluster SOS Report — {generated}</title>
</head>
<body>
{skeleton_html}
</body>
</html>"""

    valid, reason = validate_html(final_html)
    if not valid:
        raw_path = report_dir / "cluster" / "cluster_report_ai_raw.html"
        raw_path.write_text(final_html)
        print(f"[WARN] Validation failed: {reason}", file=sys.stderr)
        print(f"[WARN] Raw output → {raw_path}", file=sys.stderr)
        sys.exit(1)

    output_path = report_dir / "cluster" / OUTPUT_FILE
    output_path.write_text(final_html)
    size_kb = len(final_html) // 1024
    print(f"\n[*] Dashboard written → {output_path} ({size_kb} KB)")
    print(f"[*] Open: file://{output_path}")


if __name__ == "__main__":
    main()
