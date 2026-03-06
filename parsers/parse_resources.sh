#!/usr/bin/env bash
# parsers/parse_resources.sh — CPU, memory, disk usage
# Usage: parse_resources.sh <sos_root> <output_dir>

source "$(dirname "$0")/../lib/common.sh"

SOS="$1"
OUT="$2"

[[ -z "$SOS" || -z "$OUT" ]] && { log_error "Usage: $0 <sos_root> <output_dir>"; exit 1; }
mkdir -p "$OUT"

# ─── Memory ──────────────────────────────────────────────────────────────────
mem_total_kb=$(grep '^MemTotal:'     "$SOS/proc/meminfo" 2>/dev/null | awk '{print $2}')
mem_free_kb=$(grep  '^MemFree:'      "$SOS/proc/meminfo" 2>/dev/null | awk '{print $2}')
mem_avail_kb=$(grep '^MemAvailable:' "$SOS/proc/meminfo" 2>/dev/null | awk '{print $2}')
swap_total_kb=$(grep '^SwapTotal:'   "$SOS/proc/meminfo" 2>/dev/null | awk '{print $2}')
swap_free_kb=$(grep  '^SwapFree:'    "$SOS/proc/meminfo" 2>/dev/null | awk '{print $2}')

mem_total_gb=0; mem_used_gb=0; mem_used_pct=0
if [[ -n "$mem_total_kb" && "$mem_total_kb" -gt 0 ]]; then
    mem_used_kb=$(( mem_total_kb - mem_avail_kb ))
    mem_used_pct=$(( mem_used_kb * 100 / mem_total_kb ))
    mem_total_gb=$(awk "BEGIN {printf \"%.1f\", $mem_total_kb/1048576}")
    mem_used_gb=$(awk  "BEGIN {printf \"%.1f\", $mem_used_kb/1048576}")
fi

swap_used_kb=0; swap_used_pct=0
if [[ -n "$swap_total_kb" && "$swap_total_kb" -gt 0 ]]; then
    swap_used_kb=$(( swap_total_kb - swap_free_kb ))
    swap_used_pct=$(( swap_used_kb * 100 / swap_total_kb ))
fi

mem_flag=$(flag_mem "$mem_used_pct")

# ─── CPU — from SAR (today's file, 'all' average) ────────────────────────────
cpu_usr="N/A"; cpu_sys="N/A"; cpu_idle="N/A"; cpu_iowait="N/A"
cpu_count=$(grep -c "^processor" "$SOS/proc/cpuinfo" 2>/dev/null)
if [[ -z "$cpu_count" || "$cpu_count" == "0" ]]; then
    dmi="$SOS/sos_commands/hardware/dmidecode"
    if [[ -f "$dmi" ]]; then
        threads=$(grep 'Thread Count:' "$dmi" | awk '{print $NF}' | head -1)
        sockets=$(grep -c 'Socket Designation:.*CPU' "$dmi" 2>/dev/null || echo 1)
        if [[ -n "$threads" && "$threads" =~ ^[0-9]+$ ]]; then
            cpu_count=$(( threads * sockets ))
        fi
    fi
fi
[[ -z "$cpu_count" || "$cpu_count" == "0" ]] && cpu_count="unknown"

# Try plain-text SAR files first, then skip XML (requires sadf which may not be present)
sar_file=$(find "$SOS/sos_commands/sar/" -maxdepth 1 -name 'sar[0-9]*' ! -name '*.xml' 2>/dev/null | sort | tail -1)
if [[ -n "$sar_file" ]]; then
    avg_line=$(grep '^Average:' "$sar_file" | grep -E '\ball\b' | tail -1)
    if [[ -n "$avg_line" ]]; then
        cpu_usr=$(echo "$avg_line"    | awk '{print $3}')
        cpu_sys=$(echo "$avg_line"    | awk '{print $5}')
        cpu_iowait=$(echo "$avg_line" | awk '{print $6}')
        cpu_idle=$(echo "$avg_line"   | awk '{print $NF}')
        # Normalize if summed across cores
        if [[ "$cpu_count" =~ ^[0-9]+$ && "$cpu_usr" =~ ^[0-9]+\. ]]; then
            if awk "BEGIN {exit !($cpu_usr > 100)}"; then
                cpu_usr=$(awk    "BEGIN {printf \"%.2f\", $cpu_usr/$cpu_count}")
                cpu_sys=$(awk    "BEGIN {printf \"%.2f\", $cpu_sys/$cpu_count}")
                cpu_iowait=$(awk "BEGIN {printf \"%.2f\", $cpu_iowait/$cpu_count}")
                cpu_idle=$(awk   "BEGIN {printf \"%.2f\", $cpu_idle/$cpu_count}")
            fi
        fi
    fi
fi

# ─── Disk — real filesystems only (skip pseudo fs with 0 blocks) ─────────────
disk_json_arr="["
disk_txt_lines=""
first=1
while IFS= read -r line; do
    # Skip header, pseudo filesystems (size=0 or '-'), and tmpfs under 100MB
    [[ "$line" =~ ^Filesystem ]] && continue
    blocks=$(echo "$line" | awk '{print $2}')
    [[ "$blocks" == "0" || "$blocks" == "-" ]] && continue
    # Only include if size > 100000 blocks (~100MB)
    [[ "$blocks" =~ ^[0-9]+$ ]] || continue
    (( blocks < 100000 )) && continue

    fs=$(echo "$line"      | awk '{print $1}')
    used=$(echo "$line"    | awk '{print $3}')
    avail=$(echo "$line"   | awk '{print $4}')
    pct_str=$(echo "$line" | awk '{print $5}')
    mount=$(echo "$line"   | awk '{print $6}')
    pct="${pct_str//%/}"
    [[ "$pct" =~ ^[0-9]+$ ]] || pct=0

    flag=$(flag_disk "$pct")
    used_gb=$(awk  "BEGIN {printf \"%.1f\", $used/1048576}")
    avail_gb=$(awk "BEGIN {printf \"%.1f\", $avail/1048576}")
    total_gb=$(awk "BEGIN {printf \"%.1f\", ($used+$avail)/1048576}")

    [[ $first -eq 0 ]] && disk_json_arr+=","
    disk_json_arr+=$(cat <<EJSON
{
      "filesystem": "$(json_escape "$fs")",
      "mount": "$(json_escape "$mount")",
      "total_gb": $total_gb,
      "used_gb": $used_gb,
      "avail_gb": $avail_gb,
      "used_pct": $pct,
      "flag": "$flag"
    }
EJSON
)
    disk_txt_lines+=$(printf "  %-45s %6s%% (%s/%s GB) [%s]\n" "$mount" "$pct" "$used_gb" "$total_gb" "$flag")
    disk_txt_lines+=$'\n'
    first=0
done < <(cat "$SOS/df" 2>/dev/null)
disk_json_arr+="]"

# ─── Write JSON ──────────────────────────────────────────────────────────────
cat > "$OUT/resources.json" <<EOF
{
  "memory": {
    "total_gb": $mem_total_gb,
    "used_gb": $mem_used_gb,
    "used_pct": $mem_used_pct,
    "swap_total_kb": ${swap_total_kb:-0},
    "swap_used_kb": $swap_used_kb,
    "swap_used_pct": $swap_used_pct,
    "flag": "$mem_flag"
  },
  "cpu": {
    "count": "$cpu_count",
    "usr_pct": "$cpu_usr",
    "sys_pct": "$cpu_sys",
    "iowait_pct": "$cpu_iowait",
    "idle_pct": "$cpu_idle"
  },
  "disk": $disk_json_arr
}
EOF

# ─── Write text summary ──────────────────────────────────────────────────────
cat > "$OUT/resources.txt" <<EOF
=== RESOURCES ===

-- Memory --
  Total:    ${mem_total_gb} GB
  Used:     ${mem_used_gb} GB (${mem_used_pct}%) [$mem_flag]
  Swap Used: ${swap_used_pct}%

-- CPU ($cpu_count cores, SAR daily avg) --
  User:     ${cpu_usr}%
  System:   ${cpu_sys}%
  I/O Wait: ${cpu_iowait}%
  Idle:     ${cpu_idle}%

-- Disk (real filesystems only) --
${disk_txt_lines}
EOF

log_ok "Resources parsed (mem: ${mem_used_pct}% used, disk entries: $(echo "$disk_txt_lines" | grep -c '\['))"
