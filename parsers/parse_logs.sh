#!/usr/bin/env bash
# parsers/parse_logs.sh — kernel and system log error extraction
# Usage: parse_logs.sh <sos_root> <output_dir>
#
# Three event categories:
#   STORAGE INTERNAL — errors on the node itself, drives CRITICAL/WARNING flag
#   CLIENT EVENTS    — bulk IO errors/reconnects from remote clients, informational only
#   NOISE            — known benign patterns, counted but not flagged

source "$(dirname "$0")/../lib/common.sh"

SOS="$1"
OUT="$2"

[[ -z "$SOS" || -z "$OUT" ]] && { log_error "Usage: $0 <sos_root> <output_dir>"; exit 1; }
mkdir -p "$OUT"

# ─── Pattern sets ─────────────────────────────────────────────────────────────

# Noise — skip these entirely (boot races, session mgmt, known benign)
NOISE_PAT='lctl set_param.*failed|systemd-udevd.*lctl|dnf\[|systemd-logind|session.*logged|session.*Succeeded|New session|Started Session|Removed session'

# Client-originated patterns — involves a remote LNet address doing IO
# These indicate a client issue, not a storage node issue
CLIENT_PAT='@o2ib|@tcp[0-9]'
CLIENT_EVENT_PAT='Bulk IO|client will retry|reconnecting|peer NIs in recovery|kiblnd_rejected|lnet_create_reply_msg|lnet_resend|peer.*recovery|recovery.*peer'

# Storage-internal critical patterns — problems on this node itself
CRIT_PAT='Out of memory|oom_kill|Kernel panic|kernel BUG|BUG: unable|Call Trace|segfault|SCSI error|I/O error|EXT4-fs error|XFS.*error|hardware error|MCE|Machine Check|NMI|ldiskfs.*error|osd.*error|journal.*error|disk.*error|block.*error|LustreError.*osd|LustreError.*ldiskfs|LustreError.*timeout.*local|LustreError.*lost.*conn.*local'

# Storage-internal warning patterns
WARN_PAT='WARNING:|warn_slowpath|soft lockup|hung_task|link is not ready|link down|carrier lost|throttling|degraded|authentication failure'

# ─── Process a log file ───────────────────────────────────────────────────────
process_log() {
    local file="$1"
    local prefix="${2:-}"

    while IFS= read -r line; do
        # Skip noise
        echo "$line" | grep -qiP "$NOISE_PAT" && (( noise_count++ )) || true && continue

        # Client-originated events — has a remote LNet addr AND a client event pattern
        if echo "$line" | grep -qP "$CLIENT_PAT" && \
           echo "$line" | grep -qiP "$CLIENT_EVENT_PAT"; then
            client_events+=("${prefix}${line}")
            # Extract client IP for summarization
            local ip
            ip=$(echo "$line" | grep -oP '\d+\.\d+\.\d+\.\d+(?=@o2ib|@tcp)' | head -1)
            [[ -n "$ip" ]] && client_ips["$ip"]=$(( ${client_ips["$ip"]:-0} + 1 ))
            continue
        fi

        # Storage internal critical
        if echo "$line" | grep -qiP "$CRIT_PAT"; then
            crit_events+=("${prefix}${line}")
            continue
        fi

        # Storage internal warning
        if echo "$line" | grep -qiP "$WARN_PAT"; then
            warn_events+=("${prefix}${line}")
            continue
        fi

    done < <(grep -iP "$NOISE_PAT|$CLIENT_PAT|$CRIT_PAT|$WARN_PAT" "$file" 2>/dev/null)
}

# ─── Parse logs ───────────────────────────────────────────────────────────────
crit_events=(); warn_events=(); client_events=()
declare -A client_ips
noise_count=0

msg_file="$SOS/var/log/messages"
[[ -f "$msg_file" ]] && process_log "$msg_file"

kern_log=$(ls "$SOS/var/log/kern/"* 2>/dev/null | head -1)
[[ -f "$kern_log" ]] && process_log "$kern_log" "[kern] "

# Raw counts before dedup
crit_count=${#crit_events[@]}
warn_count=${#warn_events[@]}
client_count=${#client_events[@]}

# ─── Deduplicate and cap ──────────────────────────────────────────────────────
mapfile -t crit_events   < <(printf '%s\n' "${crit_events[@]}"   | sort -u | tail -50)
mapfile -t warn_events   < <(printf '%s\n' "${warn_events[@]}"   | sort -u | tail -50)
mapfile -t client_events < <(printf '%s\n' "${client_events[@]}" | sort -u | tail -50)

# ─── Build client IP summary ──────────────────────────────────────────────────
client_ip_summary=""
client_ip_json="["
ip_first=1
for ip in "${!client_ips[@]}"; do
    count=${client_ips[$ip]}
    client_ip_summary+="    $ip: $count events\n"
    [[ $ip_first -eq 0 ]] && client_ip_json+=","
    client_ip_json+=$(printf '{"ip":"%s","count":%d}' "$ip" "$count")
    ip_first=0
done
client_ip_json+="]"

# ─── Flag — only storage-internal events drive the flag ──────────────────────
if (( crit_count > 0 )); then
    log_flag="CRITICAL"
elif (( warn_count > 0 )); then
    log_flag="WARNING"
elif (( client_count > 0 )); then
    log_flag="CLIENT_ISSUES"
else
    log_flag="OK"
fi

# ─── Build JSON arrays ────────────────────────────────────────────────────────
build_json_arr() {
    local out="["
    local first=1
    for item in "$@"; do
        [[ $first -eq 0 ]] && out+=","
        out+="\"$(json_escape "$item")\""
        first=0
    done
    out+="]"
    echo "$out"
}

crit_json=$(build_json_arr "${crit_events[@]}")
warn_json=$(build_json_arr "${warn_events[@]}")
client_json=$(build_json_arr "${client_events[@]}")

# ─── Write JSON ───────────────────────────────────────────────────────────────
cat > "$OUT/logs.json" <<EOF
{
  "flag": "$log_flag",
  "critical_count": $crit_count,
  "warning_count": $warn_count,
  "client_event_count": $client_count,
  "noise_count": $noise_count,
  "client_ips": $client_ip_json,
  "critical_events": $crit_json,
  "warning_events": $warn_json,
  "client_events": $client_json
}
EOF

# ─── Write text summary ───────────────────────────────────────────────────────
{
    echo "=== LOG ANALYSIS ==="
    echo "  Storage Critical:  $crit_count  [$log_flag]"
    echo "  Storage Warning:   $warn_count"
    echo "  Client Events:     $client_count ($(echo "${!client_ips[@]}" | wc -w) unique client IPs)"
    echo "  Noise (filtered):  $noise_count"
    echo ""

    if (( crit_count > 0 )); then
        echo "-- STORAGE CRITICAL Events (last 20) --"
        printf '%s\n' "${crit_events[@]}" | tail -20
        echo ""
    fi

    if (( warn_count > 0 )); then
        echo "-- STORAGE WARNING Events (last 20) --"
        printf '%s\n' "${warn_events[@]}" | tail -20
        echo ""
    fi

    if (( client_count > 0 )); then
        echo "-- CLIENT Events by IP --"
        printf '%b' "$client_ip_summary"
        echo ""
        echo "-- CLIENT Events (last 20) --"
        printf '%s\n' "${client_events[@]}" | tail -20
        echo ""
    fi

    if (( crit_count == 0 && warn_count == 0 && client_count == 0 )); then
        echo "  No significant events found."
    fi
} > "$OUT/logs.txt"

log_ok "Logs parsed: $crit_count critical, $warn_count warning, $client_count client events"
