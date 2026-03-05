#!/usr/bin/env bash
# export.sh — generate CSV, HTML, and PDF outputs from cluster_diff.json + per-node data
# Usage: export.sh <results_dir> <cluster_out_dir> [csv|html|pdf|all]

source "$(dirname "$0")/lib/common.sh"

RESULTS="$1"
CLUSTER="$2"
FORMAT="${3:-all}"

[[ -z "$RESULTS" || -z "$CLUSTER" ]] && { log_error "Usage: $0 <results_dir> <cluster_dir> [csv|html|pdf|all]"; exit 1; }

EXPORT_DIR="$CLUSTER/exports"
mkdir -p "$EXPORT_DIR"

# ─── Helper ───────────────────────────────────────────────────────────────────
jget() {
    local file="$1" key="$2"
    grep -oP "\"${key}\":\s*\K(\"[^\"]*\"|-?[0-9.]+)" "$file" 2>/dev/null | head -1 | tr -d '"'
}

node_dirs=()
for d in "$RESULTS"/*/; do
    [[ -f "$d/identity.json" ]] && node_dirs+=("$d")
done

# ════════════════════════════════════════════════════════════════════════════
# CSV EXPORT
# ════════════════════════════════════════════════════════════════════════════
export_csv() {
    log_info "Generating CSV exports..."

    # ── cluster_overview.csv ──
    {
        echo "hostname,os,kernel,cpu_count,uptime_days,mem_used_pct,mem_flag,failed_services,log_flag,lustre_flag,overall_flag,collection_date"
        for d in "${node_dirs[@]}"; do
            id="$d/identity.json"; res="$d/resources.json"
            svc="$d/services.json"; log="$d/logs.json"; lus="$d/lustre.json"
            hostname=$(jget "$id" hostname)
            os="$(jget "$id" os_name) $(jget "$id" os_version)"
            kernel=$(jget "$id" kernel)
            cpu=$(jget "$id" cpu_count)
            uptime=$(jget "$id" uptime_days)
            mem_pct=$(jget "$res" used_pct)
            mem_flag=$(jget "$res" flag)
            failed_svc=$(jget "$svc" failed_count)
            log_flag=$(jget "$log" flag)
            lus_flag=$(jget "$lus" flag)
            col_date=$(jget "$id" collection_date)
            overall="OK"
            for f in "$mem_flag" "$log_flag" "$lus_flag"; do
                [[ "$f" == "CRITICAL" ]] && overall="CRITICAL" && break
                [[ "$f" == "WARNING"  ]] && overall="WARNING"
            done
            echo "\"$hostname\",\"$os\",\"$kernel\",$cpu,$uptime,$mem_pct,$mem_flag,$failed_svc,$log_flag,$lus_flag,$overall,\"$col_date\""
        done
    } > "$EXPORT_DIR/cluster_overview.csv"

    # ── disk_usage.csv ──
    {
        echo "hostname,mount,filesystem,total_gb,used_gb,avail_gb,used_pct,flag"
        for d in "${node_dirs[@]}"; do
            hostname=$(jget "$d/identity.json" hostname)
            res="$d/resources.json"
            [[ ! -f "$res" ]] && continue
            # Extract disk array entries
            python3 -c "
import json, sys
data = json.load(open('$res'))
for disk in data.get('disk', []):
    print(','.join([
        '\"$hostname\"',
        '\"' + disk.get('mount','') + '\"',
        '\"' + disk.get('filesystem','') + '\"',
        str(disk.get('total_gb',0)),
        str(disk.get('used_gb',0)),
        str(disk.get('avail_gb',0)),
        str(disk.get('used_pct',0)),
        disk.get('flag','')
    ]))
" 2>/dev/null
        done
    } > "$EXPORT_DIR/disk_usage.csv"

    # ── failed_services.csv ──
    {
        echo "hostname,unit,load,active,sub,description"
        for d in "${node_dirs[@]}"; do
            hostname=$(jget "$d/identity.json" hostname)
            svc="$d/services.json"
            [[ ! -f "$svc" ]] && continue
            python3 -c "
import json
data = json.load(open('$svc'))
for u in data.get('failed_units', []):
    print(','.join([
        '\"$hostname\"',
        '\"' + u.get('unit','') + '\"',
        u.get('load',''),
        u.get('active',''),
        u.get('sub',''),
        '\"' + u.get('description','') + '\"'
    ]))
" 2>/dev/null
        done
    } > "$EXPORT_DIR/failed_services.csv"

    # ── network_interfaces.csv ──
    {
        echo "hostname,interface,ip_cidr,scope"
        for d in "${node_dirs[@]}"; do
            hostname=$(jget "$d/identity.json" hostname)
            net="$d/network.json"
            [[ ! -f "$net" ]] && continue
            python3 -c "
import json
data = json.load(open('$net'))
for iface in data.get('interfaces', []):
    print(','.join([
        '\"$hostname\"',
        '\"' + iface.get('interface','') + '\"',
        '\"' + iface.get('ip_cidr','') + '\"',
        iface.get('scope','')
    ]))
" 2>/dev/null
        done
    } > "$EXPORT_DIR/network_interfaces.csv"

    # ── installed_rpms.csv ──
    {
        echo "hostname,name,version,release,arch,install_date"
        for d in "${node_dirs[@]}"; do
            hostname=$(jget "$d/identity.json" hostname)
            rpms="$d/rpms.json"
            [[ ! -f "$rpms" ]] && continue
            python3 -c "
import json
data = json.load(open('$rpms'))
for p in data.get('packages', []):
    print(','.join([
        '\"$hostname\"',
        '\"' + p.get('name','') + '\"',
        '\"' + p.get('version','') + '\"',
        '\"' + p.get('release','') + '\"',
        p.get('arch',''),
        '\"' + p.get('install_date','') + '\"'
    ]))
" 2>/dev/null
        done
    } > "$EXPORT_DIR/installed_rpms.csv"

    # ── lustre_osts.csv ──
    {
        echo "hostname,uuid,mount,total_tb,used_tb,avail_tb,used_pct,flag"
        for d in "${node_dirs[@]}"; do
            hostname=$(jget "$d/identity.json" hostname)
            lus="$d/lustre.json"
            [[ ! -f "$lus" ]] && continue
            python3 -c "
import json
data = json.load(open('$lus'))
for o in data.get('osts', []):
    print(','.join([
        '\"$hostname\"',
        '\"' + o.get('uuid','') + '\"',
        '\"' + o.get('mount','') + '\"',
        str(o.get('total_tb',0)),
        str(o.get('used_tb',0)),
        str(o.get('avail_tb',0)),
        str(o.get('used_pct',0)),
        o.get('flag','')
    ]))
" 2>/dev/null
        done
    } > "$EXPORT_DIR/lustre_osts.csv"

    log_ok "CSVs written to $EXPORT_DIR/"
    ls -lh "$EXPORT_DIR/"*.csv 2>/dev/null | awk '{print "  " $5, $9}'
}

# ════════════════════════════════════════════════════════════════════════════
# HTML EXPORT
# ════════════════════════════════════════════════════════════════════════════
export_html() {
    log_info "Generating HTML report..."
    local html_file="$CLUSTER/reports/cluster_report.html"
    mkdir -p "$CLUSTER/reports"

    # Collect node data via python for HTML generation
    python3 - "$RESULTS" "$html_file" <<'PYEOF'
import json, os, sys, glob
from datetime import datetime

results_dir = sys.argv[1]
output_file = sys.argv[2]

def jload(path):
    try:
        with open(path) as f:
            return json.load(f)
    except:
        return {}

nodes = []
for d in sorted(glob.glob(os.path.join(results_dir, '*/'))):
    if not os.path.exists(os.path.join(d, 'identity.json')):
        continue
    node = {
        'identity': jload(os.path.join(d, 'identity.json')),
        'resources': jload(os.path.join(d, 'resources.json')),
        'services': jload(os.path.join(d, 'services.json')),
        'network': jload(os.path.join(d, 'network.json')),
        'logs': jload(os.path.join(d, 'logs.json')),
        'lustre': jload(os.path.join(d, 'lustre.json')),
        'rpms': jload(os.path.join(d, 'rpms.json')),
    }
    # Compute overall flag
    flags = [node['resources'].get('memory',{}).get('flag','OK'),
             node['services'].get('flag','OK'),
             node['logs'].get('flag','OK'),
             node['lustre'].get('flag','OK')]
    if 'CRITICAL' in flags:    node['overall_flag'] = 'CRITICAL'
    elif 'WARNING' in flags:   node['overall_flag'] = 'WARNING'
    else:                       node['overall_flag'] = 'OK'
    nodes.append(node)

def flag_class(f):
    return {'CRITICAL':'crit','WARNING':'warn','OK':'ok'}.get(f,'ok')

def flag_icon(f):
    return {'CRITICAL':'&#10060;','WARNING':'&#9888;','OK':'&#9989;'}.get(f,'')

html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Cluster SOS Report — {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: 'Segoe UI', sans-serif; background: #0f1117; color: #e2e8f0; font-size: 14px; }}
  h1 {{ padding: 24px 32px 8px; font-size: 22px; color: #f8fafc; }}
  .subtitle {{ padding: 0 32px 20px; color: #94a3b8; font-size: 13px; }}
  .section {{ padding: 0 32px 32px; }}
  h2 {{ font-size: 15px; color: #94a3b8; text-transform: uppercase; letter-spacing: .08em; margin-bottom: 12px; padding-top: 24px; border-top: 1px solid #1e293b; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
  th {{ background: #1e293b; color: #94a3b8; text-align: left; padding: 8px 12px; font-weight: 600; letter-spacing: .04em; }}
  td {{ padding: 7px 12px; border-bottom: 1px solid #1e293b; vertical-align: top; }}
  tr:hover td {{ background: #151c2c; }}
  .crit {{ color: #f87171; font-weight: 700; }}
  .warn {{ color: #fbbf24; font-weight: 600; }}
  .ok   {{ color: #34d399; }}
  .badge {{ display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 700; }}
  .badge-crit {{ background: #7f1d1d; color: #fca5a5; }}
  .badge-warn {{ background: #78350f; color: #fcd34d; }}
  .badge-ok   {{ background: #064e3b; color: #6ee7b7; }}
  .mono {{ font-family: 'Consolas','Courier New',monospace; font-size: 12px; }}
  details summary {{ cursor: pointer; color: #60a5fa; margin: 4px 0; }}
  details summary:hover {{ color: #93c5fd; }}
  .node-card {{ background: #1a2236; border: 1px solid #253048; border-radius: 8px; margin-bottom: 16px; overflow: hidden; }}
  .node-header {{ padding: 12px 16px; background: #1e293b; display: flex; align-items: center; gap: 12px; }}
  .node-header h3 {{ font-size: 15px; color: #e2e8f0; flex: 1; }}
  .node-body {{ padding: 16px; display: grid; grid-template-columns: repeat(auto-fit, minmax(280px,1fr)); gap: 16px; }}
  .card {{ background: #0f1117; border: 1px solid #253048; border-radius: 6px; padding: 12px; }}
  .card-title {{ font-size: 11px; text-transform: uppercase; color: #64748b; letter-spacing: .06em; margin-bottom: 8px; }}
  .stat {{ font-size: 24px; font-weight: 700; color: #e2e8f0; }}
  .stat-label {{ font-size: 11px; color: #64748b; }}
  pre {{ background: #0f1117; padding: 10px; border-radius: 4px; overflow-x: auto; font-size: 11px; color: #94a3b8; max-height: 200px; }}
</style>
</head>
<body>
<h1>&#128202; Cluster SOS Report</h1>
<div class="subtitle">Generated {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')} &nbsp;|&nbsp; {len(nodes)} nodes</div>
<div class="section">
"""

# ── Cluster overview table ──
html += '<h2>Cluster Overview</h2>\n'
html += '<table><tr><th>Hostname</th><th>OS</th><th>Kernel</th><th>CPUs</th><th>Uptime</th><th>Memory</th><th>Failed Svcs</th><th>Log Events</th><th>IB</th><th>Lustre</th><th>Status</th></tr>\n'
import re as _re
for n in nodes:
    ident = n['identity']
    res   = n['resources']
    svc   = n['services']
    logs  = n['logs']
    lus   = n['lustre']
    net   = n['network']
    mem   = res.get('memory', {})
    fc    = flag_class(n['overall_flag'])
    badge = f'<span class="badge badge-{flag_class(n["overall_flag"])}">{flag_icon(n["overall_flag"])} {n["overall_flag"]}</span>'
    # IB summary for overview
    ib_ports = net.get('infiniband', [])
    ib_flag  = net.get('ib_flag', 'OK')
    ib_fc    = flag_class(ib_flag)
    total_ib_errors = sum(p.get('error_count', 0) for p in ib_ports)
    rate_str = ib_ports[0].get('rate', '—') if ib_ports else '—'
    m = _re.match(r'(\d+)Gb/sec\((\d+X\w+)\)', rate_str.replace(' ',''))
    if m:
        rate_str = f'{m.group(1)}G {m.group(2)}'
    ib_cell = f'{rate_str} / {len(ib_ports)}p / err:{total_ib_errors}'
    # Log events summary
    log_parts = []
    if logs.get('critical_count', 0) > 0:
        log_parts.append(f'{logs.get("critical_count",0)} crit')
    if logs.get('warning_count', 0) > 0:
        log_parts.append(f'{logs.get("warning_count",0)} warn')
    if logs.get('client_event_count', 0) > 0:
        log_parts.append(f'{logs.get("client_event_count",0)} client')
    log_cell = ' / '.join(log_parts) if log_parts else 'OK'
    html += f'''<tr>
      <td class="mono">{ident.get("hostname","?")}</td>
      <td>{ident.get("os_name","?")} {ident.get("os_version","")}</td>
      <td class="mono" style="font-size:11px">{ident.get("kernel","?")}</td>
      <td>{ident.get("cpu_count","?")}</td>
      <td>{ident.get("uptime_days","?")}d</td>
      <td class="{flag_class(mem.get("flag","OK"))}">{mem.get("used_pct","?")}%</td>
      <td class="{flag_class(svc.get("flag","OK"))}">{svc.get("failed_count",0)} ({svc.get("ignored_count",0)} ignored)</td>
      <td class="{flag_class(logs.get("flag","OK"))}">{log_cell}</td>
      <td class="{ib_fc}">{ib_cell}</td>
      <td class="{flag_class(lus.get("flag","OK"))}">{lus.get("flag","N/A")}</td>
      <td>{badge}</td>
    </tr>\n'''
html += '</table>\n'

# ── Disk usage diff table ──
html += '<h2>Disk Usage — All Nodes</h2>\n'
# Collect all mountpoints
mounts = {}
for n in nodes:
    for d in n['resources'].get('disk', []):
        mounts[d['mount']] = mounts.get(d['mount'], [])
if mounts:
    html += '<table><tr><th>Mount</th>'
    for n in nodes:
        html += f'<th>{n["identity"].get("hostname","?")}</th>'
    html += '</tr>\n'
    # Build mount → node → (pct, flag) map
    mount_data = {}
    for n in nodes:
        hn = n['identity'].get('hostname','?')
        for d in n['resources'].get('disk', []):
            m = d['mount']
            if m not in mount_data:
                mount_data[m] = {}
            mount_data[m][hn] = d
    for m in sorted(mount_data.keys()):
        html += f'<tr><td class="mono">{m}</td>'
        for n in nodes:
            hn = n['identity'].get('hostname','?')
            d = mount_data.get(m, {}).get(hn)
            if d:
                fc = flag_class(d.get('flag','OK'))
                html += f'<td class="{fc}">{d.get("used_pct","?")}% ({d.get("used_gb","?")} GB)</td>'
            else:
                html += '<td style="color:#475569">—</td>'
        html += '</tr>\n'
    html += '</table>\n'

# ── Failed services diff ──
html += '<h2>Failed Services — All Nodes</h2>\n'
any_failed = any(n['services'].get('failed_count', 0) > 0 for n in nodes)
any_ignored = any(n['services'].get('ignored_count', 0) > 0 for n in nodes)
if any_failed:
    html += '<table><tr><th>Hostname</th><th>Unit</th><th>State</th><th>Description</th></tr>\n'
    for n in nodes:
        hn = n['identity'].get('hostname','?')
        for u in n['services'].get('failed_units', []):
            html += f'<tr><td class="mono">{hn}</td><td class="mono crit">{u.get("unit","")}</td>'
            html += f'<td>{u.get("active","")}/{u.get("sub","")}</td><td>{u.get("description","")}</td></tr>\n'
    html += '</table>\n'
else:
    html += '<p class="ok">&#9989; No failed services across any node.</p>\n'
if any_ignored:
    html += '<details><summary style="cursor:pointer;color:#888">&#9432; Ignored failed services (see conf/ignore_services.txt)</summary>\n'
    html += '<table><tr><th>Hostname</th><th>Unit</th><th>State</th><th>Description</th></tr>\n'
    for n in nodes:
        hn = n['identity'].get('hostname','?')
        for u in n['services'].get('ignored_units', []):
            html += f'<tr><td class="mono">{hn}</td><td class="mono" style="color:#888">{u.get("unit","")}</td>'
            html += f'<td style="color:#888">{u.get("active","")}/{u.get("sub","")}</td><td style="color:#888">{u.get("description","")}</td></tr>\n'
    html += '</table></details>\n'

# ── Network & InfiniBand summary ──
html += '<h2>Network &amp; InfiniBand — All Nodes</h2>\n'
html += '<table><tr><th>Hostname</th><th>Mgmt IP</th><th>mlxib0</th><th>mlxib1</th><th>IB Rate / Width</th><th>FW Version</th><th>Port Errors</th><th>Error Detail</th><th>IB Flag</th></tr>\n'
for n in nodes:
    hn  = n['identity'].get('hostname','?')
    net = n['network']
    ifaces = net.get('interfaces', [])
    iface_map = {}
    for iface in ifaces:
        name = iface.get('interface','')
        ip   = iface.get('ip_cidr','').split('/')[0]
        iface_map[name] = ip
    mgmt_ip = iface_map.get('mgmt0', iface_map.get('eth0', iface_map.get('eno1', '—')))
    mlxib0  = iface_map.get('mlxib0', '—')
    mlxib1  = iface_map.get('mlxib1', '—')
    ib_ports = net.get('infiniband', [])
    ib_flag  = net.get('ib_flag', 'OK')
    fc       = flag_class(ib_flag)
    # Rate/width from first port
    rate_str = ib_ports[0].get('rate', '—') if ib_ports else '—'
    m = _re.match(r'(\d+)Gb/sec\((\d+X\w+)\)', rate_str.replace(' ',''))
    if m:
        rate_str = f'{m.group(1)} Gb/sec &nbsp;&#183;&nbsp; {m.group(2)}'
    # FW version
    fw = ib_ports[0].get('firmware', '—') if ib_ports else '—'
    # Per-port error summary
    port_err_cells = []
    err_detail_cells = []
    for p in ib_ports:
        errs = p.get('error_count', 0)
        detail = p.get('error_detail', '')
        ec = 'crit' if errs > 0 else 'ok'
        port_err_cells.append(f'<span class="{ec}">{p.get("ca","?")}:{errs}</span>')
        if detail:
            # Format each counter on its own line
            detail_fmt = '<br>'.join(f'{p.get("ca","?")}/{c}' for c in detail.split(','))
            err_detail_cells.append(detail_fmt)
    total_errors = sum(p.get('error_count', 0) for p in ib_ports)
    err_class = 'crit' if total_errors > 0 else 'ok'
    port_errs_html = ' &nbsp; '.join(port_err_cells) if port_err_cells else '—'
    err_detail_html = '<br>'.join(err_detail_cells) if err_detail_cells else '—'
    html += f'<tr><td class="mono">{hn}</td><td class="mono">{mgmt_ip}</td>'
    html += f'<td class="mono">{mlxib0}</td><td class="mono">{mlxib1}</td>'
    html += f'<td>{rate_str}</td>'
    html += f'<td style="font-size:11px">{fw}</td>'
    html += f'<td>{port_errs_html}</td>'
    html += f'<td class="{err_class}" style="font-size:11px">{err_detail_html}</td>'
    html += f'<td class="{fc}">{ib_flag}</td></tr>\n'
html += '</table>\n'

# ── Lustre summary ──
html += '<h2>Lustre — OST/MDT Summary</h2>\n'
html += '<table><tr><th>Hostname</th><th>OSTs</th><th>MDTs</th><th>Devices Down</th><th>Critical OSTs</th><th>Warning OSTs</th><th>Flag</th></tr>\n'
for n in nodes:
    hn = n['identity'].get('hostname','?')
    lus = n['lustre']
    if not lus:
        continue
    fc = flag_class(lus.get('flag','OK'))
    html += f'<tr><td class="mono">{hn}</td><td>{lus.get("ost_count","?")}</td>'
    html += f'<td>{lus.get("mdt_count","?")}</td>'
    dd = lus.get("devices_down", 0)
    html += f'<td class="{"crit" if dd>0 else "ok"}">{dd}</td>'
    oc = lus.get("ost_critical", 0)
    ow = lus.get("ost_warning", 0)
    html += f'<td class="{"crit" if oc>0 else "ok"}">{oc}</td>'
    html += f'<td class="{"warn" if ow>0 else "ok"}">{ow}</td>'
    html += f'<td class="{fc}"><b>{lus.get("flag","?")}</b></td></tr>\n'
html += '</table>\n'

# ── Per-node detail cards ──
html += '<h2>Per-Node Details</h2>\n'
for n in nodes:
    hn = n['identity'].get('hostname','?')
    overall_fc = flag_class(n['overall_flag'])
    badge = f'<span class="badge badge-{overall_fc}">{flag_icon(n["overall_flag"])} {n["overall_flag"]}</span>'
    html += f'<div class="node-card"><div class="node-header"><h3>&#128421; {hn}</h3>{badge}</div><div class="node-body">'

    # Identity card
    ident = n['identity']
    html += f'''<div class="card"><div class="card-title">Identity</div>
      <div class="mono" style="font-size:12px;line-height:1.7">
        OS: {ident.get("os_name","")} {ident.get("os_version","")}<br>
        Kernel: {ident.get("kernel","")}<br>
        CPUs: {ident.get("cpu_count","?")}  |  Up: {ident.get("uptime_days","?")} days<br>
        Load: {ident.get("load_average","?")}<br>
        Collected: {ident.get("collection_date","?")}
      </div></div>'''

    # Memory card
    mem = n['resources'].get('memory', {})
    mem_fc = flag_class(mem.get('flag','OK'))
    html += f'''<div class="card"><div class="card-title">Memory</div>
      <div class="stat {mem_fc}">{mem.get("used_pct","?")}%</div>
      <div class="stat-label">{mem.get("used_gb","?")} / {mem.get("total_gb","?")} GB used</div>
      <div style="margin-top:6px;font-size:12px;color:#64748b">Swap: {mem.get("swap_used_pct","?")}% used</div>
      </div>'''

    # CPU card
    cpu = n['resources'].get('cpu', {})
    html += f'''<div class="card"><div class="card-title">CPU ({cpu.get("count","?")} cores)</div>
      <div class="mono" style="font-size:12px;line-height:1.7">
        User: {cpu.get("usr_pct","?")}% &nbsp; Sys: {cpu.get("sys_pct","?")}%<br>
        I/O Wait: {cpu.get("iowait_pct","?")}%<br>
        Idle: {cpu.get("idle_pct","?")}%
      </div></div>'''

    # Log events card
    logs = n['logs']
    log_fc = flag_class(logs.get('flag','OK'))
    crit_evs = logs.get('critical_events', [])
    warn_evs = logs.get('warning_events', [])
    client_evs = logs.get('client_events', [])
    client_ips = logs.get('client_ips', [])
    html += f'<div class="card"><div class="card-title">Log Events</div>'
    html += f'<div class="{log_fc}">{logs.get("critical_count",0)} critical &nbsp; {logs.get("warning_count",0)} warnings &nbsp; {logs.get("client_event_count",0)} client events</div>'
    if crit_evs:
        html += f'<details><summary>Show critical events ({len(crit_evs)})</summary><pre>'
        html += '\n'.join(str(e)[:200] for e in crit_evs[-10:])
        html += '</pre></details>'
    if client_ips:
        ip_summary = ', '.join(f'{x["ip"]} ({x["count"]})' for x in client_ips)
        html += f'<details><summary>Client events by IP: {ip_summary}</summary><pre>'
        html += '\n'.join(str(e)[:200] for e in client_evs[-10:])
        html += '</pre></details>'
    html += '</div>'

    # InfiniBand card
    net = n['network']
    ib_ports = net.get('infiniband', [])
    ib_flag  = net.get('ib_flag', 'OK')
    ib_fc    = flag_class(ib_flag)
    if ib_ports:
        html += f'<div class="card"><div class="card-title">InfiniBand</div>'
        html += f'<div class="{ib_fc}" style="margin-bottom:8px">Overall: {ib_flag}</div>'
        html += '<table style="font-size:11px;width:100%"><tr><th>Port</th><th>State</th><th>Rate / Width</th><th>FW</th><th>Total Errors</th><th>Error Counters</th></tr>'
        for p in ib_ports:
            pfc = flag_class(p.get('flag','OK'))
            errs = p.get('error_count', 0)
            err_detail = p.get('error_detail','')
            rate = p.get('rate','—')
            m = _re.match(r'(\d+)Gb/sec\((\d+X\w+)\)', rate.replace(' ',''))
            if m:
                rate = f'{m.group(1)} Gb/sec {m.group(2)}'
            fw = p.get('firmware','—')
            err_class = 'crit' if errs > 0 else 'ok'
            # Format error counters as individual lines
            if err_detail:
                counter_lines = '<br>'.join(
                    f'<span class="crit">{c.split(":")[0]}: {c.split(":")[1]}</span>'
                    for c in err_detail.split(',') if ':' in c
                )
            else:
                counter_lines = '<span class="ok">clean</span>'
            html += f'<tr class="{pfc}"><td class="mono">{p.get("ca","?")}</td>'
            html += f'<td>{p.get("state","?")}</td><td>{rate}</td>'
            html += f'<td style="font-size:10px">{fw}</td>'
            html += f'<td class="{err_class}">{errs}</td>'
            html += f'<td style="font-size:10px">{counter_lines}</td></tr>'
        html += '</table></div>'

    html += '</div></div>\n'  # end node-body, node-card

html += '</div></body></html>'

with open(output_file, 'w') as f:
    f.write(html)
print(f"HTML written: {output_file}")
PYEOF

    log_ok "HTML report: $html_file"
}

# ════════════════════════════════════════════════════════════════════════════
# PDF EXPORT
# ════════════════════════════════════════════════════════════════════════════
export_pdf() {
    log_info "Generating PDF report..."
    local html_file="$CLUSTER/reports/cluster_report.html"
    local pdf_file="$CLUSTER/reports/cluster_report.pdf"

    # Ensure HTML exists first
    [[ ! -f "$html_file" ]] && export_html

    if command -v wkhtmltopdf &>/dev/null; then
        wkhtmltopdf --quiet --page-size A4 --orientation Landscape \
            --margin-top 10mm --margin-bottom 10mm \
            --margin-left 10mm --margin-right 10mm \
            "$html_file" "$pdf_file" 2>/dev/null
        log_ok "PDF report (wkhtmltopdf): $pdf_file"
    elif command -v pandoc &>/dev/null; then
        # Try weasyprint first (best HTML-to-PDF), fall back to pdflatex
        if command -v weasyprint &>/dev/null; then
            pandoc "$html_file" -o "$pdf_file" --pdf-engine=weasyprint 2>/dev/null
        else
            pandoc "$html_file" -o "$pdf_file" 2>/dev/null
        fi
        if [[ -f "$pdf_file" ]]; then
            log_ok "PDF report (pandoc): $pdf_file"
        else
            log_warn "PDF generation failed. HTML report available at $html_file"
        fi
    else
        log_warn "No PDF engine found (install wkhtmltopdf or pandoc). HTML report available at $html_file"
    fi
}

# ─── Run requested formats ────────────────────────────────────────────────────
case "$FORMAT" in
    csv)  export_csv ;;
    html) export_html ;;
    pdf)  export_pdf ;;
    all)  export_csv; export_html; export_pdf ;;
    *)    log_error "Unknown format: $FORMAT. Use csv|html|pdf|all" ;;
esac
