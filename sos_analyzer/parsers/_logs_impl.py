#!/usr/bin/env python3
# parsers/parse_logs.py — kernel and system log error extraction
# Usage: parse_logs.py <sos_root> <output_dir>
#
# Three event categories:
#   STORAGE INTERNAL — errors on the node itself, drives CRITICAL/WARNING flag
#   CLIENT EVENTS    — bulk IO errors/reconnects from remote clients, informational only
#   NOISE            — known benign patterns, counted but not flagged

# ─── Imports ──────────────────────────────────────────────────────────────────
# All standard library — no pip install needed
import re       # regular expressions (replaces grep)
import os       # file path operations
import sys      # sys.argv for command-line arguments, sys.exit()
import json     # JSON serialization — replaces our hand-rolled build_json_arr
from collections import defaultdict  # dict that auto-initializes missing keys

# ─── Arguments ────────────────────────────────────────────────────────────────
# In bash we used $1 and $2. In Python, sys.argv is a list:
#   sys.argv[0] = script name
#   sys.argv[1] = sos_root
#   sys.argv[2] = output_dir
if len(sys.argv) != 3:
    print(f"Usage: {sys.argv[0]} <sos_root> <output_dir>", file=sys.stderr)
    sys.exit(1)

sos_root   = sys.argv[1]
output_dir = sys.argv[2]

# Create output directory if it doesn't exist — same as mkdir -p
os.makedirs(output_dir, exist_ok=True)

# ─── Pattern sets ─────────────────────────────────────────────────────────────
# In bash these were strings passed to grep -iE.
# In Python, re.compile() pre-compiles the pattern once — much faster than
# spawning a new grep process for every line.
# re.IGNORECASE = the -i flag. re.search() = grep behavior (match anywhere in line).

NOISE_PAT = re.compile(
    r'lctl set_param.*failed'
    r'|systemd-udevd.*lctl'
    r'|dnf\['
    r'|systemd-logind'
    r'|session.*logged'
    r'|session.*Succeeded'
    r'|New session'
    r'|Started Session'
    r'|Removed session'
    r'|LAPIC_NMI'
    r'|mounted filesystem with'
    r'|mounting filesystem'
    r'|ADDRCONF.NETDEV_UP.'
    r'|link is not ready'
    r'|locking_type.*deprecated'
    r'|dracut-initqueue.*WARNING'
    r'|SATA link down'
    r'|ata[0-9].*: failed to resume link'
    r'|systemd-journal-gatewayd'
    r'|microhttpd'
    r'|emf.*WARNING.*sensitive data'
    r'|This command may collect sensitive'
    r'|Quorum acquired'
    r'|notice: Quorum',
    re.IGNORECASE
)

# Client-originated: line must contain an LNet address (@o2ib or @tcp)
CLIENT_ADDR_PAT = re.compile(r'@o2ib|@tcp[0-9]')

# AND one of these client event keywords
CLIENT_EVENT_PAT = re.compile(
    r'Bulk IO'
    r'|client will retry'
    r'|reconnecting'
    r'|peer NIs in recovery'
    r'|kiblnd_rejected'
    r'|lnet_create_reply_msg'
    r'|lnet_resend'
    r'|peer.*recovery'
    r'|recovery.*peer',
    re.IGNORECASE
)

# Extract client IP from a line like "172.1.1.215@o2ib"
CLIENT_IP_PAT = re.compile(r'(\d+\.\d+\.\d+\.\d+)@(?:o2ib|tcp)')

CRIT_PAT = re.compile(
    r'Out of memory'
    r'|oom_kill'
    r'|Kernel panic'
    r'|kernel BUG'
    r'|BUG: unable'
    r'|Call Trace'
    r'|segfault'
    r'|SCSI error'
    r'|I/O error'
    r'|EXT4-fs error'
    r'|XFS.*error'
    r'|hardware error'
    r'|MCE'
    r'|Machine Check'
    r'|NMI received'
    r'|ldiskfs.*error[^s=]'
    r'|osd.*error'
    r'|journal.*error'
    r'|disk.*error'
    r'|block.*error'
    r'|LustreError.*osd'
    r'|LustreError.*ldiskfs'
    r'|LustreError.*timeout.*local'
    r'|LustreError.*lost.*conn.*local'
    r'|LustreError.*not available for connect'
    r'|LustreError.*operation.*failed.*rc'
    r'|LustreError.*mount.*failed'
    r'|LustreError.*evicting client'
    r'|pacemaker.*error'
    r'|Quorum lost',
    re.IGNORECASE
)

WARN_PAT = re.compile(
    r'WARNING:'
    r'|warn_slowpath'
    r'|Quorum lost'
    r'|Quorum acquired'
    r'|Blind faith.*fencing'
    r'|Primary configuration corrupt'
    r'|pacemaker.*warning'
    r'|soft lockup'
    r'|hung_task'
    r'|link is not ready'
    r'|link down'
    r'|carrier lost'
    r'|throttling'
    r'|degraded'
    r'|authentication failure',
    re.IGNORECASE
)

# Combined pattern — used as a first-pass filter so we only inspect
# lines that match at least one category. Same trick as the bash version's
# outer grep before the inner per-line classification loop.
ANY_PAT = re.compile(
    NOISE_PAT.pattern + '|' +
    CLIENT_ADDR_PAT.pattern + '|' +
    CRIT_PAT.pattern + '|' +
    WARN_PAT.pattern,
    re.IGNORECASE
)

# ─── State — replaces bash arrays and associative arrays ──────────────────────
# Python lists are like bash arrays. Sets handle deduplication natively.
# defaultdict(int) is like bash's ${client_ips[$ip]:-0} — auto-initializes to 0.
crit_events   = []
warn_events   = []
client_events = []
client_ips    = defaultdict(int)   # ip -> count
noise_count   = 0

# ─── process_log() ────────────────────────────────────────────────────────────
# In Python, functions are defined with 'def'. Parameters work like bash
# local variables. The 'prefix' parameter has a default value of "" —
# same as ${2:-} in bash.
def process_log(filepath, prefix=""):
    """Read a log file and classify each matching line into the appropriate bucket."""

    # These are the module-level lists/counters — 'global' tells Python we want
    # to modify them, not create new local variables with the same name.
    global noise_count

    # 'with open(...)' is Python's safe file handling — automatically closes
    # the file when done, even if an exception occurs. Like bash's while read loop
    # but the file handle is managed for you.
    # errors='replace' handles non-UTF8 bytes gracefully instead of crashing.
    try:
        import re as _re_ts, datetime as _dt
        ISO_TS = _re_ts.compile(r'^(\d{4}-\d{2}-\d{2})T')
        with open(filepath, errors='replace') as fh:
            for line in fh:
                line = line.rstrip('\n')  # strip trailing newline, like chomp
                # Skip lines older than cutoff date
                if cutoff_dt is not None:
                    m = ISO_TS.match(line)
                    if m:
                        try:
                            if _dt.date.fromisoformat(m.group(1)) < cutoff_dt:
                                continue
                        except Exception:
                            pass

                # First-pass filter — skip lines that can't match anything
                # This is the equivalent of the outer grep in the bash version
                if not ANY_PAT.search(line):
                    continue

                # Noise — skip and count
                if NOISE_PAT.search(line):
                    noise_count += 1
                    continue

                # Client-originated: must have LNet addr AND client event keyword
                if CLIENT_ADDR_PAT.search(line) and CLIENT_EVENT_PAT.search(line):
                    client_events.append(prefix + line)
                    # Extract IP — re.search() returns a Match object or None
                    m = CLIENT_IP_PAT.search(line)
                    if m:
                        client_ips[m.group(1)] += 1  # m.group(1) = first capture group
                    continue

                # Storage internal critical
                if CRIT_PAT.search(line):
                    crit_events.append(prefix + line)
                    continue

                # Storage internal warning
                if WARN_PAT.search(line):
                    warn_events.append(prefix + line)
                    continue

    except OSError as e:
        print(f"Warning: could not read {filepath}: {e}", file=sys.stderr)

# ─── Parse log files ──────────────────────────────────────────────────────────
# Determine collection date for log age filtering
import datetime
cutoff_dt = None
date_file = os.path.join(sos_root, 'date')
if os.path.isfile(date_file):
    try:
        date_txt = open(date_file).read()
        import re as _re_date
        m = _re_date.search(r'Universal time:\s+\w+\s+(\d{4}-\d{2}-\d{2})', date_txt)
        if m:
            coll_date = datetime.date.fromisoformat(m.group(1))
            cutoff_dt = coll_date - datetime.timedelta(days=30)
    except Exception:
        pass

msg_file = os.path.join(sos_root, 'var', 'log', 'messages')
if os.path.isfile(msg_file):
    process_log(msg_file)

# Find kern log — equivalent of: ls $SOS/var/log/kern/* | head -1
kern_dir = os.path.join(sos_root, 'var', 'log', 'kern')
if os.path.isdir(kern_dir):
    kern_files = sorted(os.listdir(kern_dir))
    if kern_files:
        process_log(os.path.join(kern_dir, kern_files[0]), prefix='[kern] ')

# ─── Raw counts before dedup ──────────────────────────────────────────────────
crit_count   = len(crit_events)
warn_count   = len(warn_events)
client_count = len(client_events)

# ─── Deduplicate and cap ──────────────────────────────────────────────────────
# dict.fromkeys() preserves order while removing duplicates (Python 3.7+)
# [-50:] is a slice — last 50 elements, same as tail -50
crit_events   = list(dict.fromkeys(crit_events))[-50:]
warn_events   = list(dict.fromkeys(warn_events))[-50:]
client_events = list(dict.fromkeys(client_events))[-50:]

# ─── Flag ─────────────────────────────────────────────────────────────────────
if crit_count > 0:
    log_flag = 'CRITICAL'
elif warn_count > 0:
    log_flag = 'WARNING'
elif client_count > 0:
    log_flag = 'CLIENT_ISSUES'
else:
    log_flag = 'OK'

# ─── Client IP summary ────────────────────────────────────────────────────────
# Sort by count descending so busiest clients appear first
client_ip_list = sorted(client_ips.items(), key=lambda x: x[1], reverse=True)

# ─── Write JSON ───────────────────────────────────────────────────────────────
# json.dumps() handles all escaping automatically — no more json_escape()
# indent=2 makes it human-readable, same as our hand-formatted heredoc
output = {
    'flag':               log_flag,
    'critical_count':     crit_count,
    'warning_count':      warn_count,
    'client_event_count': client_count,
    'noise_count':        noise_count,
    'client_ips':         [{'ip': ip, 'count': count} for ip, count in client_ip_list],
    'critical_events':    crit_events,
    'warning_events':     warn_events,
    'client_events':      client_events,
}

json_path = os.path.join(output_dir, 'logs.json')
with open(json_path, 'w') as fh:
    json.dump(output, fh, indent=2)

# ─── Write text summary ───────────────────────────────────────────────────────
txt_path = os.path.join(output_dir, 'logs.txt')
with open(txt_path, 'w') as fh:
    fh.write('=== LOG ANALYSIS ===\n')
    fh.write(f'  Storage Critical:  {crit_count}  [{log_flag}]\n')
    fh.write(f'  Storage Warning:   {warn_count}\n')
    fh.write(f'  Client Events:     {client_count} ({len(client_ips)} unique client IPs)\n')
    fh.write(f'  Noise (filtered):  {noise_count}\n\n')

    if crit_events:
        fh.write('-- STORAGE CRITICAL Events (last 20) --\n')
        fh.write('\n'.join(crit_events[-20:]) + '\n\n')

    if warn_events:
        fh.write('-- STORAGE WARNING Events (last 20) --\n')
        fh.write('\n'.join(warn_events[-20:]) + '\n\n')

    if client_events:
        fh.write('-- CLIENT Events by IP --\n')
        for ip, count in client_ip_list:
            fh.write(f'    {ip}: {count} events\n')
        fh.write('\n-- CLIENT Events (last 20) --\n')
        fh.write('\n'.join(client_events[-20:]) + '\n\n')

    if not crit_events and not warn_events and not client_events:
        fh.write('  No significant events found.\n')

print(f"[OK] Logs parsed: {crit_count} critical, {warn_count} warning, {client_count} client events")
