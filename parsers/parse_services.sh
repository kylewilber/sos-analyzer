#!/usr/bin/env bash
# parsers/parse_services.sh — systemd service status
# Usage: parse_services.sh <sos_root> <output_dir>

source "$(dirname "$0")/../lib/common.sh"

SOS="$1"
OUT="$2"

[[ -z "$SOS" || -z "$OUT" ]] && { log_error "Usage: $0 <sos_root> <output_dir>"; exit 1; }
mkdir -p "$OUT"

SYSTEMD="$SOS/sos_commands/systemd"
IGNORE_FILE="$(dirname "$0")/../conf/ignore_services.txt"

# ─── Load ignore list ─────────────────────────────────────────────────────────
ignore_patterns=()
if [[ -f "$IGNORE_FILE" ]]; then
    while IFS= read -r line; do
        [[ "$line" =~ ^#  ]] && continue
        [[ -z "$line" ]]     && continue
        ignore_patterns+=("$line")
    done < "$IGNORE_FILE"
fi

is_ignored() {
    local unit="$1"
    for pat in "${ignore_patterns[@]}"; do
        [[ "$unit" == *"$pat"* ]] && return 0
    done
    return 1
}

# ─── Failed units ─────────────────────────────────────────────────────────────
failed_json_arr="["
ignored_json_arr="["
failed_txt=""
ignored_txt=""
failed_count=0
ignored_count=0
first=1
first_ignored=1

while IFS= read -r line; do
    [[ "$line" =~ ^●[[:space:]] ]] || continue
    unit=$(echo "$line"   | awk '{print $2}')
    load=$(echo "$line"   | awk '{print $3}')
    active=$(echo "$line" | awk '{print $4}')
    sub=$(echo "$line"    | awk '{print $5}')
    desc=$(echo "$line"   | awk '{$1=$2=$3=$4=$5=""; print}' | sed 's/^ *//')

    entry=$(printf '{"unit":"%s","load":"%s","active":"%s","sub":"%s","description":"%s"}' \
        "$(json_escape "$unit")" "$load" "$active" "$sub" "$(json_escape "$desc")")
    line_txt=$(printf "  %-55s  LOAD:%-8s ACTIVE:%-8s SUB:%s\n" "$unit" "$load" "$active" "$sub")

    if is_ignored "$unit"; then
        [[ $first_ignored -eq 0 ]] && ignored_json_arr+=","
        ignored_json_arr+="$entry"
        ignored_txt+="$line_txt"$'\n'
        (( ignored_count++ )) || true
        first_ignored=0
    else
        [[ $first -eq 0 ]] && failed_json_arr+=","
        failed_json_arr+="$entry"
        failed_txt+="$line_txt"$'\n'
        (( failed_count++ )) || true
        first=0
    fi
done < <(cat "$SYSTEMD/systemctl_list-units_--failed" 2>/dev/null)

failed_json_arr+="]"
ignored_json_arr+="]"

# ─── Enabled services count ───────────────────────────────────────────────────
enabled_count=$(grep -c ' enabled' "$SYSTEMD/systemctl_list-unit-files" 2>/dev/null || echo 0)
disabled_count=$(grep -c ' disabled' "$SYSTEMD/systemctl_list-unit-files" 2>/dev/null || echo 0)
total_unit_files=$(grep -c '\.service' "$SYSTEMD/systemctl_list-unit-files" 2>/dev/null || echo 0)

# ─── Overall flag — only non-ignored failures drive status ───────────────────
if (( failed_count > 0 )); then
    svc_flag="WARNING"
else
    svc_flag="OK"
fi

# ─── Write JSON ───────────────────────────────────────────────────────────────
cat > "$OUT/services.json" <<EOF
{
  "failed_count": $failed_count,
  "ignored_count": $ignored_count,
  "enabled_count": $enabled_count,
  "disabled_count": $disabled_count,
  "total_unit_files": $total_unit_files,
  "flag": "$svc_flag",
  "failed_units": $failed_json_arr,
  "ignored_units": $ignored_json_arr
}
EOF

# ─── Write text summary ───────────────────────────────────────────────────────
cat > "$OUT/services.txt" <<EOF
=== SERVICES ===
  Failed Units:    $failed_count  [$svc_flag]
  Ignored Units:   $ignored_count  (see conf/ignore_services.txt)
  Enabled:         $enabled_count
  Disabled:        $disabled_count
  Total .service unit files: $total_unit_files

-- Failed Units --
${failed_txt:-  (none)}
-- Ignored Failed Units --
${ignored_txt:-  (none)}
EOF

log_ok "Services parsed: $failed_count failed, $ignored_count ignored, $enabled_count enabled"
