#!/usr/bin/env bash
# parsers/parse_services.sh — systemd service status
# Usage: parse_services.sh <sos_root> <output_dir>

source "$(dirname "$0")/../lib/common.sh"

SOS="$1"
OUT="$2"

[[ -z "$SOS" || -z "$OUT" ]] && { log_error "Usage: $0 <sos_root> <output_dir>"; exit 1; }
mkdir -p "$OUT"

SYSTEMD="$SOS/sos_commands/systemd"

# ─── Failed units ─────────────────────────────────────────────────────────────
failed_json_arr="["
failed_txt=""
failed_count=0
first=1
while IFS= read -r line; do
    # Lines starting with ● are failed unit entries
    [[ "$line" =~ ^●[[:space:]] ]] || continue
    unit=$(echo "$line"   | awk '{print $2}')
    load=$(echo "$line"   | awk '{print $3}')
    active=$(echo "$line" | awk '{print $4}')
    sub=$(echo "$line"    | awk '{print $5}')
    desc=$(echo "$line"   | awk '{$1=$2=$3=$4=$5=""; print}' | sed 's/^ *//')

    [[ $first -eq 0 ]] && failed_json_arr+=","
    failed_json_arr+=$(cat <<EJSON
{"unit": "$(json_escape "$unit")", "load": "$load", "active": "$active", "sub": "$sub", "description": "$(json_escape "$desc")"}
EJSON
)
    failed_txt+=$(printf "  %-55s  LOAD:%-8s ACTIVE:%-8s SUB:%s\n" "$unit" "$load" "$active" "$sub")
    failed_txt+=$'\n'
    (( failed_count++ ))
    first=0
done < <(cat "$SYSTEMD/systemctl_list-units_--failed" 2>/dev/null)
failed_json_arr+="]"

# ─── Enabled services count ───────────────────────────────────────────────────
enabled_count=$(grep -c ' enabled' "$SYSTEMD/systemctl_list-unit-files" 2>/dev/null || echo 0)
disabled_count=$(grep -c ' disabled' "$SYSTEMD/systemctl_list-unit-files" 2>/dev/null || echo 0)
total_unit_files=$(grep -c '\.service' "$SYSTEMD/systemctl_list-unit-files" 2>/dev/null || echo 0)

# ─── Overall flag ─────────────────────────────────────────────────────────────
if (( failed_count > 0 )); then
    svc_flag="WARNING"
else
    svc_flag="OK"
fi

# ─── Write JSON ───────────────────────────────────────────────────────────────
cat > "$OUT/services.json" <<EOF
{
  "failed_count": $failed_count,
  "enabled_count": $enabled_count,
  "disabled_count": $disabled_count,
  "total_unit_files": $total_unit_files,
  "flag": "$svc_flag",
  "failed_units": $failed_json_arr
}
EOF

# ─── Write text summary ───────────────────────────────────────────────────────
cat > "$OUT/services.txt" <<EOF
=== SERVICES ===
  Failed Units:    $failed_count  [$svc_flag]
  Enabled:         $enabled_count
  Disabled:        $disabled_count
  Total .service unit files: $total_unit_files

-- Failed Units --
${failed_txt:-  (none)}
EOF

log_ok "Services parsed: $failed_count failed, $enabled_count enabled"
