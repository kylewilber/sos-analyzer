#!/usr/bin/env bash
# parsers/parse_lustre.sh — Lustre OST/MDT status and devices
# Usage: parse_lustre.sh <sos_root> <output_dir>

source "$(dirname "$0")/../lib/common.sh"

SOS="$1"
OUT="$2"

[[ -z "$SOS" || -z "$OUT" ]] && { log_error "Usage: $0 <sos_root> <output_dir>"; exit 1; }
mkdir -p "$OUT"

LUSTRE_DIR="$SOS/sos_commands/lustre"

# ─── lctl device list — authoritative OST/MDT counts and device state ─────────
# This is the source of truth for what a node actually owns/serves.
# obdfilter = OST server, mdt = MDT server
device_file="$LUSTRE_DIR/lctl_device_list"
dev_up=0; dev_down=0; dev_json_arr="["; dev_first=1
ost_count=0; mdt_count=0
ost_names=(); mdt_names=()

if [[ -f "$device_file" ]] && [[ -s "$device_file" ]]; then
    while IFS= read -r line; do
        [[ -z "$line" ]] && continue
        state=$(echo "$line" | awk '{print $2}')
        type=$(echo "$line"  | awk '{print $3}')
        name=$(echo "$line"  | awk '{print $4}')

        [[ $dev_first -eq 0 ]] && dev_json_arr+=","
        dev_json_arr+=$(printf '{"name":"%s","type":"%s","state":"%s"}' \
            "$(json_escape "$name")" "$(json_escape "$type")" "$(json_escape "$state")")

        if [[ "$state" == "UP" ]]; then
            dev_up=$(( dev_up + 1 ))
        else
            dev_down=$(( dev_down + 1 ))
        fi

        [[ "$type" == "obdfilter" ]] && { ost_names+=("$name"); (( ost_count++ )) || true; }
        [[ "$type" == "mdt" ]]       && { mdt_names+=("$name"); (( mdt_count++ )) || true; }

        dev_first=0
    done < "$device_file"
fi
dev_json_arr+="]"

# ─── lfs df — usage percentages only, matched to locally-owned OSTs/MDTs ──────
# Only used for disk usage data, never for counts.
lfs_df_file="$LUSTRE_DIR/lfs_df"
declare -A ost_usage mdt_usage   # name -> "used_pct:used_tb:total_tb:avail_tb:flag"

if [[ -f "$lfs_df_file" ]] && [[ -s "$lfs_df_file" ]]; then
    while IFS= read -r line; do
        [[ "$line" =~ ^UUID ]] && continue
        [[ -z "$line" ]] && continue

        uuid=$(echo "$line"    | awk '{print $1}')
        blocks=$(echo "$line"  | awk '{print $2}')
        used=$(echo "$line"    | awk '{print $3}')
        avail=$(echo "$line"   | awk '{print $4}')
        pct_str=$(echo "$line" | awk '{print $5}')
        pct="${pct_str//%/}"
        [[ "$pct" =~ ^[0-9]+$ ]] || pct=0

        used_tb=$(awk  "BEGIN {printf \"%.2f\", $used/1073741824}")
        avail_tb=$(awk "BEGIN {printf \"%.2f\", $avail/1073741824}")
        total_tb=$(awk "BEGIN {printf \"%.2f\", $blocks/1073741824}")
        flag=$(flag_disk "$pct")

        # Strip _UUID suffix to get base name for matching
        base="${uuid/_UUID/}"
        ost_usage["$base"]="$pct:$used_tb:$total_tb:$avail_tb:$flag"
        mdt_usage["$base"]="$pct:$used_tb:$total_tb:$avail_tb:$flag"
    done < "$lfs_df_file"
fi

# ─── Build OST JSON array using lctl counts + lfs_df usage where available ────
ost_json_arr="["; ost_first=1
ost_crit=0; ost_warn=0; ost_txt=""

for name in "${ost_names[@]}"; do
    # Try to match lfs_df data for this OST
    if [[ -n "${ost_usage[$name]+_}" ]]; then
        IFS=: read -r pct used_tb total_tb avail_tb flag <<< "${ost_usage[$name]}"
    else
        pct=0; used_tb="0.00"; total_tb="0.00"; avail_tb="0.00"; flag="OK"
    fi

    [[ $ost_first -eq 0 ]] && ost_json_arr+=","
    ost_json_arr+=$(printf '{"uuid":"%s_UUID","total_tb":%s,"used_tb":%s,"avail_tb":%s,"used_pct":%s,"flag":"%s"}' \
        "$(json_escape "$name")" "$total_tb" "$used_tb" "$avail_tb" "$pct" "$flag")
    ost_txt+=$(printf "  %-40s %4s%% (%s/%s TB) [%s]\n" "$name" "$pct" "$used_tb" "$total_tb" "$flag")
    ost_txt+=$'\n'
    ost_first=0
    [[ "$flag" == "CRITICAL" ]] && (( ost_crit++ )) || true
    [[ "$flag" == "WARNING" ]]  && (( ost_warn++ ))  || true
done
ost_json_arr+="]"

# ─── Build MDT JSON array ──────────────────────────────────────────────────────
mdt_json_arr="["; mdt_first=1; mdt_txt=""

for name in "${mdt_names[@]}"; do
    if [[ -n "${mdt_usage[$name]+_}" ]]; then
        IFS=: read -r pct used_tb total_tb avail_tb flag <<< "${mdt_usage[$name]}"
    else
        pct=0; used_tb="0.00"; total_tb="0.00"; avail_tb="0.00"; flag="OK"
    fi

    [[ $mdt_first -eq 0 ]] && mdt_json_arr+=","
    mdt_json_arr+=$(printf '{"uuid":"%s_UUID","total_tb":%s,"used_tb":%s,"avail_tb":%s,"used_pct":%s,"flag":"%s"}' \
        "$(json_escape "$name")" "$total_tb" "$used_tb" "$avail_tb" "$pct" "$flag")
    mdt_txt+=$(printf "  %-40s %4s%% (%s/%s TB) [%s]\n" "$name" "$pct" "$used_tb" "$total_tb" "$flag")
    mdt_txt+=$'\n'
    mdt_first=0
done
mdt_json_arr+="]"

# ─── Overall Lustre flag ──────────────────────────────────────────────────────
if (( dev_down > 0 || ost_crit > 0 )); then
    lustre_flag="CRITICAL"
elif (( ost_warn > 0 )); then
    lustre_flag="WARNING"
else
    lustre_flag="OK"
fi

# ─── Write JSON ───────────────────────────────────────────────────────────────
cat > "$OUT/lustre.json" <<EOF
{
  "flag": "$lustre_flag",
  "ost_count": $ost_count,
  "mdt_count": $mdt_count,
  "ost_critical": $ost_crit,
  "ost_warning": $ost_warn,
  "devices_up": $dev_up,
  "devices_down": $dev_down,
  "osts": $ost_json_arr,
  "mdts": $mdt_json_arr,
  "devices": $dev_json_arr
}
EOF

# ─── Write text summary ───────────────────────────────────────────────────────
cat > "$OUT/lustre.txt" <<EOF
=== LUSTRE ===
  OSTs:      $ost_count  (Critical: $ost_crit  Warning: $ost_warn)
  MDTs:      $mdt_count
  Devices:   UP:$dev_up  DOWN:$dev_down
  Flag:      $lustre_flag

-- OST Usage --
${ost_txt:-  (no OST usage data)}
-- MDT Usage --
${mdt_txt:-  (no MDT usage data)}
EOF

log_ok "Lustre parsed: $ost_count OSTs, $mdt_count MDTs, $dev_up/$((dev_up+dev_down)) devices UP"
