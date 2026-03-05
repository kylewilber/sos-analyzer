#!/usr/bin/env bash
# parsers/parse_identity.sh — extract node identity fields
# Usage: parse_identity.sh <sos_root> <output_dir>

source "$(dirname "$0")/../lib/common.sh"

SOS="$1"
OUT="$2"

[[ -z "$SOS" || -z "$OUT" ]] && { log_error "Usage: $0 <sos_root> <output_dir>"; exit 1; }
mkdir -p "$OUT"

# ─── Hostname ────────────────────────────────────────────────────────────────
hostname=$(cat "$SOS/hostname" 2>/dev/null | tr -d '[:space:]')
[[ -z "$hostname" ]] && hostname="unknown"

# ─── OS / Release ────────────────────────────────────────────────────────────
os_name=$(grep '^NAME=' "$SOS/etc/os-release" 2>/dev/null | cut -d= -f2 | tr -d '"')
os_version=$(grep '^VERSION=' "$SOS/etc/os-release" 2>/dev/null | cut -d= -f2 | tr -d '"')
os_id=$(grep '^ID=' "$SOS/etc/os-release" 2>/dev/null | cut -d= -f2 | tr -d '"')
[[ -z "$os_name" ]] && os_name=$(cat "$SOS/etc/redhat-release" 2>/dev/null || echo "unknown")

# ─── Kernel ──────────────────────────────────────────────────────────────────
uname_str=$(cat "$SOS/uname" 2>/dev/null)
kernel=$(echo "$uname_str" | awk '{print $3}')
arch=$(echo "$uname_str"   | awk '{print $NF}')

# ─── CPU count from SAR header ───────────────────────────────────────────────
sar_file=$(ls "$SOS/sos_commands/sar/sar"[0-9]* 2>/dev/null | head -1)
cpu_count=""
if [[ -n "$sar_file" ]]; then
    cpu_count=$(grep -m1 '([0-9]* CPU)' "$sar_file" | grep -oP '\d+(?= CPU)')
fi
[[ -z "$cpu_count" ]] && cpu_count=$(grep -c "^processor" "$SOS/proc/cpuinfo" 2>/dev/null || echo "unknown")

# ─── Uptime ──────────────────────────────────────────────────────────────────
uptime_raw=$(cat "$SOS/uptime" 2>/dev/null)
uptime_days=$(echo "$uptime_raw" | grep -oP '\d+(?= day)')
uptime_load=$(echo "$uptime_raw" | grep -oP 'load average: [\d., ]+' | sed 's/load average: //')
[[ -z "$uptime_days" ]] && uptime_days=0

# ─── Collection date ─────────────────────────────────────────────────────────
collect_date=$(grep 'Universal time:' "$SOS/date" 2>/dev/null | awk '{print $3, $4, $5}')
[[ -z "$collect_date" ]] && collect_date=$(cat "$SOS/date" 2>/dev/null | head -1)

# ─── SOS version ─────────────────────────────────────────────────────────────
sos_version=$(cat "$SOS/version.txt" 2>/dev/null | grep -oP '[\d.]+' | head -1)

# ─── Write JSON ──────────────────────────────────────────────────────────────
cat > "$OUT/identity.json" <<EOF
{
  "hostname": "$(json_escape "$hostname")",
  "os_name": "$(json_escape "$os_name")",
  "os_version": "$(json_escape "$os_version")",
  "os_id": "$(json_escape "$os_id")",
  "kernel": "$(json_escape "$kernel")",
  "arch": "$(json_escape "$arch")",
  "cpu_count": "$cpu_count",
  "uptime_days": $uptime_days,
  "load_average": "$(json_escape "$uptime_load")",
  "collection_date": "$(json_escape "$collect_date")",
  "sos_version": "$(json_escape "$sos_version")"
}
EOF

# ─── Write text summary ──────────────────────────────────────────────────────
cat > "$OUT/identity.txt" <<EOF
=== NODE IDENTITY: $hostname ===
OS:              $os_name $os_version
Kernel:          $kernel ($arch)
CPUs:            $cpu_count
Uptime:          ${uptime_days} days
Load Average:    $uptime_load
Collected:       $collect_date
SOS Version:     $sos_version
EOF

log_ok "Identity parsed: $hostname ($os_name $os_version)"
