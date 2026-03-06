#!/usr/bin/env bash
# parsers/parse_network.sh — IP interfaces, routes, InfiniBand
# Usage: parse_network.sh <sos_root> <output_dir>

source "$(dirname "$0")/../lib/common.sh"

SOS="$1"
OUT="$2"

[[ -z "$SOS" || -z "$OUT" ]] && { log_error "Usage: $0 <sos_root> <output_dir>"; exit 1; }
mkdir -p "$OUT"

# ─── IP Interfaces (from ip_addr) ────────────────────────────────────────────
iface_json_arr="["
iface_txt=""
first=1

while IFS= read -r line; do
    if [[ "$line" =~ inet[[:space:]]([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+/[0-9]+) ]]; then
        ip_cidr="${BASH_REMATCH[1]}"
        iface=$(echo "$line" | awk -F'\\\\' '{print $1}' | awk '{print $NF}' | tr -d ' ')
        [[ -z "$iface" ]] && continue
        [[ "$iface" == "lo" ]] && continue
        scope=$(echo "$line" | perl -ne 'print $1 if /scope (\S+)/')
        [[ $first -eq 0 ]] && iface_json_arr+=","
        iface_json_arr+=$(printf '{"interface": "%s", "ip_cidr": "%s", "scope": "%s"}' \
            "$(json_escape "$iface")" "$(json_escape "$ip_cidr")" "$(json_escape "$scope")")
        iface_txt+=$(printf "  %-12s  %s\n" "$iface" "$ip_cidr")
        iface_txt+=$'\n'
        first=0
    fi
done < <(cat "$SOS/ip_addr" 2>/dev/null)
iface_json_arr+="]"

# ─── Default gateway ─────────────────────────────────────────────────────────
default_gw=$(grep '^default' "$SOS/ip_route" 2>/dev/null | grep -v 'table' | head -1 | awk '{print $3}')
[[ -z "$default_gw" ]] && default_gw=$(grep '^default' "$SOS/ip_route" 2>/dev/null | head -1 | awk '{print $3}')

# ─── DNS servers ─────────────────────────────────────────────────────────────
dns_servers=$(grep '^nameserver' "$SOS/etc/resolv.conf" 2>/dev/null | awk '{print $2}' | tr '\n' ',' | sed 's/,$//')

# ─── Hostname / FQDN ─────────────────────────────────────────────────────────
hostname=$(cat "$SOS/hostname" 2>/dev/null | tr -d '[:space:]')

# ─── InfiniBand — ibstatus (state, rate, lane width) ─────────────────────────
# Parse ibstatus for rate string like "200 Gb/sec (4X HDR)"
declare -A ib_rate_str ib_state_str
ibstatus_file="$SOS/sos_commands/infiniband/ibstatus"
if [[ -f "$ibstatus_file" ]]; then
    current_dev=""
    while IFS= read -r line; do
        if [[ "$line" =~ Infiniband\ device\ \'([^\']+)\' ]]; then
            current_dev="${BASH_REMATCH[1]}"
        fi
        if [[ -n "$current_dev" ]]; then
            [[ "$line" =~ rate:[[:space:]]+(.+) ]] && ib_rate_str["$current_dev"]="${BASH_REMATCH[1]// /}"
            [[ "$line" =~ state:[[:space:]]+[0-9]+:\ (.+) ]] && ib_state_str["$current_dev"]="${BASH_REMATCH[1]// /}"
        fi
    done < "$ibstatus_file"
fi

# ─── InfiniBand — perfquery error counters ────────────────────────────────────
# Files: perfquery_-C_mlx5_0_-P_1, perfquery_-C_mlx5_1_-P_1, etc.
declare -A ib_errors   # ca -> total error count
declare -A ib_error_detail  # ca -> "counter:val,counter:val"

for pq_file in "$SOS/sos_commands/infiniband/perfquery_-C_"*"_-P_"*; do
    [[ -f "$pq_file" ]] || continue
    # Extract CA name from filename: perfquery_-C_mlx5_0_-P_1 -> mlx5_0
    ca=$(basename "$pq_file" | sed 's/perfquery_-C_//;s/_-P_.*//')
    total_errors=0
    error_detail=""

    while IFS= read -r line; do
        # Error counters we care about (exclude data/packet counters which wrap)
        if [[ "$line" =~ ^(SymbolErrorCounter|LinkErrorRecoveryCounter|LinkDownedCounter|PortRcvErrors|PortRcvRemotePhysicalErrors|PortXmitDiscards|LocalLinkIntegrityErrors|ExcessiveBufferOverrunErrors|PortRcvConstraintErrors|PortXmitConstraintErrors|VL15Dropped|QP1Dropped):\.+([0-9]+)$ ]]; then
            counter="${BASH_REMATCH[1]}"
            val="${BASH_REMATCH[2]}"
            if (( val > 0 )); then
                total_errors=$(( total_errors + val ))
                [[ -n "$error_detail" ]] && error_detail+=","
                error_detail+="$counter:$val"
            fi
        fi
    done < "$pq_file"

    ib_errors["$ca"]=$total_errors
    ib_error_detail["$ca"]="$error_detail"
done

# ─── InfiniBand — ibstat (CA type, firmware, port details) ───────────────────
ib_json_arr="["
ib_txt=""
ib_first=1
ib_flag="OK"
ibstat_file="$SOS/sos_commands/infiniband/ibstat"

if [[ -f "$ibstat_file" ]]; then
    current_ca=""
    ib_state=""; ib_rate=""; ib_fw=""
    while IFS= read -r line; do
        if [[ "$line" =~ ^CA\ \'(.+)\' ]]; then
            if [[ -n "$current_ca" ]]; then
                # Enrich with ibstatus rate string and error counters
                rate_full="${ib_rate_str[$current_ca]:-${ib_rate}Gb/sec}"
                err_count="${ib_errors[$current_ca]:-0}"
                err_detail="${ib_error_detail[$current_ca]:-}"
                port_flag="OK"
                (( err_count > 0 )) && port_flag="WARNING" && ib_flag="WARNING"
                [[ "$ib_state" != "Active" ]] && port_flag="CRITICAL" && ib_flag="CRITICAL"

                [[ $ib_first -eq 0 ]] && ib_json_arr+=","
                ib_json_arr+=$(printf '{"ca":"%s","state":"%s","rate":"%s","firmware":"%s","error_count":%d,"error_detail":"%s","flag":"%s"}' \
                    "$(json_escape "$current_ca")" "$(json_escape "$ib_state")" \
                    "$(json_escape "$rate_full")"  "$(json_escape "$ib_fw")" \
                    "$err_count" "$(json_escape "$err_detail")" "$port_flag")
                ib_txt+=$(printf "  %-10s  State:%-8s Rate:%-22s FW:%-14s Errors:%d [%s]\n" \
                    "$current_ca" "$ib_state" "$rate_full" "$ib_fw" "$err_count" "$port_flag")
                [[ -n "$err_detail" ]] && ib_txt+=$(printf "             %s\n" "$err_detail")
                ib_txt+=$'\n'
                ib_first=0
            fi
            current_ca="${BASH_REMATCH[1]}"
            ib_state=""; ib_rate=""; ib_fw=""
        fi
        [[ "$line" =~ State:\ (.+) ]]             && ib_state="${BASH_REMATCH[1]// /}"
        [[ "$line" =~ Rate:\ (.+) ]]              && ib_rate="${BASH_REMATCH[1]// /}"
        [[ "$line" =~ Firmware\ version:\ (.+) ]] && ib_fw="${BASH_REMATCH[1]// /}"
    done < "$ibstat_file"

    # Last entry
    if [[ -n "$current_ca" ]]; then
        rate_full="${ib_rate_str[$current_ca]:-${ib_rate}Gb/sec}"
        err_count="${ib_errors[$current_ca]:-0}"
        err_detail="${ib_error_detail[$current_ca]:-}"
        port_flag="OK"
        (( err_count > 0 )) && port_flag="WARNING" && ib_flag="WARNING"
        [[ "$ib_state" != "Active" ]] && port_flag="CRITICAL" && ib_flag="CRITICAL"

        [[ $ib_first -eq 0 ]] && ib_json_arr+=","
        ib_json_arr+=$(printf '{"ca":"%s","state":"%s","rate":"%s","firmware":"%s","error_count":%d,"error_detail":"%s","flag":"%s"}' \
            "$(json_escape "$current_ca")" "$(json_escape "$ib_state")" \
            "$(json_escape "$rate_full")"  "$(json_escape "$ib_fw")" \
            "$err_count" "$(json_escape "$err_detail")" "$port_flag")
        ib_txt+=$(printf "  %-10s  State:%-8s Rate:%-22s FW:%-14s Errors:%d [%s]\n" \
            "$current_ca" "$ib_state" "$rate_full" "$ib_fw" "$err_count" "$port_flag")
        [[ -n "$err_detail" ]] && ib_txt+=$(printf "             %s\n" "$err_detail")
        ib_txt+=$'\n'
    fi
fi
ib_json_arr+="]"

# ─── Write JSON ───────────────────────────────────────────────────────────────
cat > "$OUT/network.json" <<EOF
{
  "hostname": "$(json_escape "$hostname")",
  "default_gateway": "$(json_escape "$default_gw")",
  "dns_servers": "$(json_escape "$dns_servers")",
  "ib_flag": "$ib_flag",
  "interfaces": $iface_json_arr,
  "infiniband": $ib_json_arr
}
EOF

# ─── Write text summary ───────────────────────────────────────────────────────
cat > "$OUT/network.txt" <<EOF
=== NETWORK ===
  Default GW:  ${default_gw:-N/A}
  DNS:         ${dns_servers:-N/A}
  IB Flag:     $ib_flag

-- IP Interfaces --
${iface_txt:-  (none found)}
-- InfiniBand --
${ib_txt:-  (no ibstat data)}
EOF

iface_count=$(echo "$iface_txt" | grep -c '\.')
ib_count=$(echo "$ib_txt" | grep -c 'State')
log_ok "Network parsed: $iface_count interfaces, IB: $ib_count ports [$ib_flag]"
