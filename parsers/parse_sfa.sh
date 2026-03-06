#!/usr/bin/env bash
# parsers/parse_sfa.sh — DDN SFA enclosure health parser
# Usage: parse_sfa.sh <sos_root> <output_dir>

source "$(dirname "$0")/../lib/common.sh"

SOS="$1"
OUT="$2"

[[ -z "$SOS" || -z "$OUT" ]] && { log_error "Usage: $0 <sos_root> <output_dir>"; exit 1; }
mkdir -p "$OUT"

SFA_DIR="$SOS/sos_commands/sfa"

if [[ ! -d "$SFA_DIR" ]]; then
    echo '{"available": false}' > "$OUT/sfa.json"
    exit 0
fi

sfa_flag="OK"

# ─── Helper: data rows only (skip || header rows and error lines) ─────────────
sfa_rows() { grep '^|[^|]' "$1" 2>/dev/null | grep -v '^x '; }

# ─── Subsystem list ───────────────────────────────────────────────────────────
# Columns: | Source IPs | UUID | Name | Platform | Health | NTP Enabled | ... | Timezone | ...
#            $2           $3     $4     $5         $6       $7             ...   $9
sub_file="$SFA_DIR/emf_sfa_subsystem_list_--table-style_jira"
sub_json="[]"
tz_flag=0
tz_list=""
if [[ -f "$sub_file" ]]; then
    sub_json="["
    first=1
    while IFS= read -r line; do
        name=$(echo     "$line" | awk -F'|' '{gsub(/^ +| +$/,"",$4); print $4}')
        platform=$(echo "$line" | awk -F'|' '{gsub(/^ +| +$/,"",$5); print $5}')
        health=$(echo   "$line" | awk -F'|' '{gsub(/^ +| +$/,"",$6); print $6}')
        tz=$(echo       "$line" | awk -F'|' '{gsub(/^ +| +$/,"",$9); print $9}')
        [[ -z "$name" ]] && continue
        [[ $first -eq 0 ]] && sub_json+=","
        sub_json+="{\"name\":\"$(json_escape "$name")\",\"platform\":\"$(json_escape "$platform")\",\"health\":\"$(json_escape "$health")\",\"timezone\":\"$(json_escape "$tz")\"}"
        tz_list="$tz_list $tz"
        first=0
    done < <(sfa_rows "$sub_file")
    sub_json+="]"
    # Check timezone consistency
    unique_tz=$(echo "$tz_list" | tr ' ' '\n' | grep -v '^$' | sort -u | wc -l | tr -d ' ')
    [[ "$unique_tz" -gt 1 ]] && tz_flag=1 && sfa_flag="WARNING"
fi

# ─── Pool list ────────────────────────────────────────────────────────────────
# Columns: | Source IPs | Index | Name | State | Health | ...
#            $2           $3      $4     $5      $6
pool_file="$SFA_DIR/emf_sfa_pool_list_--table-style_jira"
pool_not_optimal=0
pool_issues=""
if [[ -f "$pool_file" ]]; then
    while IFS= read -r line; do
        name=$(echo  "$line" | awk -F'|' '{gsub(/^ +| +$/,"",$4); print $4}')
        state=$(echo "$line" | awk -F'|' '{gsub(/^ +| +$/,"",$5); print $5}')
        [[ -z "$name" || "$name" == *"|"* ]] && continue
        if [[ "$state" != "Optimal" ]]; then
            (( pool_not_optimal++ ))
            pool_issues="$pool_issues $name:$state"
            sfa_flag="CRITICAL"
        fi
    done < <(sfa_rows "$pool_file")
fi

# ─── IOC list — IB HCA firmware consistency per part number ──────────────────
# Columns: | Source IPs | Index | VM Index | Pending VM Index | Controller | AP | Part num | FW version | Port type |
#            $2           $3      $4         $5                 $6           $7   $8         $9           $10
ioc_file="$SFA_DIR/emf_sfa_ioc_list_--table-style_jira"
declare -A part_fw   # part_num -> first fw seen
declare -A part_mismatch
ib_fw_flag=0
ib_summary=""
if [[ -f "$ioc_file" ]]; then
    while IFS= read -r line; do
        port_type=$(echo "$line" | awk -F'|' '{gsub(/^ +| +$/,"",$10); print $10}')
        [[ "$port_type" != "Infiniband" ]] && continue
        part=$(echo "$line" | awk -F'|' '{gsub(/^ +| +$/,"",$8); print $8}')
        fw=$(echo   "$line" | awk -F'|' '{gsub(/^ +| +$/,"",$9); print $9}')
        [[ -z "$part" || -z "$fw" ]] && continue
        if [[ -z "${part_fw[$part]+x}" ]]; then
            part_fw["$part"]="$fw"
        elif [[ "${part_fw[$part]}" != "$fw" ]]; then
            part_mismatch["$part"]="expected:${part_fw[$part]} got:$fw"
            ib_fw_flag=1
            [[ "$sfa_flag" == "OK" ]] && sfa_flag="WARNING"
        fi
    done < <(sfa_rows "$ioc_file")
    # Build summary string: "MCX653106A-HDA_Ax=20.43.2566 MCX755106AS-HEA_Ax=28.43.2566"
    for part in "${!part_fw[@]}"; do
        ib_summary="$ib_summary ${part}=${part_fw[$part]}"
        [[ -n "${part_mismatch[$part]+x}" ]] && ib_summary="$ib_summary[MISMATCH:${part_mismatch[$part]}]"
    done
fi

# ─── Write JSON ───────────────────────────────────────────────────────────────
cat > "$OUT/sfa.json" <<EOF
{
  "available": true,
  "flag": "$sfa_flag",
  "tz_inconsistent": $tz_flag,
  "pool_not_optimal": $pool_not_optimal,
  "pool_issues": "$(json_escape "$pool_issues")",
  "ib_fw_flag": $ib_fw_flag,
  "ib_fw_summary": "$(json_escape "${ib_summary# }")",
  "subsystems": $sub_json
}
EOF

# ─── Write text summary ───────────────────────────────────────────────────────
cat > "$OUT/sfa.txt" <<EOF
=== SFA HARDWARE ===
Flag:             $sfa_flag
Timezone:         $([ $tz_flag -eq 0 ] && echo "consistent" || echo "INCONSISTENT")
Pools not optimal: $pool_not_optimal$([ -n "$pool_issues" ] && echo " ($pool_issues)")
IB FW consistent: $([ $ib_fw_flag -eq 0 ] && echo "yes" || echo "NO - mismatch detected")
IB FW:            $ib_summary
EOF

log_ok "SFA parsed: flag=$sfa_flag, pools_not_optimal=$pool_not_optimal, tz_inconsistent=$tz_flag, ib_fw_flag=$ib_fw_flag"
