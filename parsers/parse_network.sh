#!/usr/bin/env bash
# parsers/parse_network.sh — IP interfaces, routes, InfiniBand
# Usage: parse_network.sh <sos_root> <output_dir>

source "$(dirname "$0")/../lib/common.sh"

SOS="$1"
OUT="$2"

[[ -z "$SOS" || -z "$OUT" ]] && { log_error "Usage: $0 <sos_root> <output_dir>"; exit 1; }
mkdir -p "$OUT"

# ─── IP Interfaces (from ip_addr) ────────────────────────────────────────────
# Format: "<idx>: <iface>    inet <ip>/<prefix> ..."
iface_json_arr="["
iface_txt=""
first=1
declare -A seen_ifaces

while IFS= read -r line; do
    # Match lines with inet (IPv4)
    if [[ "$line" =~ inet[[:space:]]([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+/[0-9]+) ]]; then
        ip_cidr="${BASH_REMATCH[1]}"
        # Interface name is the last token before \
        iface=$(echo "$line" | awk -F'\\\\' '{print $1}' | awk '{print $NF}' | tr -d ' ')
        [[ -z "$iface" ]] && continue
        [[ "$iface" == "lo" ]] && continue

        scope=$(echo "$line" | grep -oP 'scope \K\S+')
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

# ─── InfiniBand ──────────────────────────────────────────────────────────────
ib_json_arr="["
ib_txt=""
ib_first=1
ibstat_file="$SOS/sos_commands/infiniband/ibstat"
if [[ -f "$ibstat_file" ]]; then
    current_ca=""
    ib_state=""; ib_rate=""; ib_fw=""
    while IFS= read -r line; do
        if [[ "$line" =~ ^CA\ \'(.+)\' ]]; then
            # Save previous entry
            if [[ -n "$current_ca" ]]; then
                [[ $ib_first -eq 0 ]] && ib_json_arr+=","
                ib_json_arr+=$(printf '{"ca": "%s", "state": "%s", "rate": "%s", "firmware": "%s"}' \
                    "$(json_escape "$current_ca")" "$(json_escape "$ib_state")" \
                    "$(json_escape "$ib_rate")"    "$(json_escape "$ib_fw")")
                ib_txt+=$(printf "  %-10s  State:%-8s Rate:%-6s FW:%s\n" \
                    "$current_ca" "$ib_state" "$ib_rate" "$ib_fw")
                ib_txt+=$'\n'
                ib_first=0
            fi
            current_ca="${BASH_REMATCH[1]}"
            ib_state=""; ib_rate=""; ib_fw=""
        fi
        [[ "$line" =~ State:\ (.+) ]]            && ib_state="${BASH_REMATCH[1]// /}"
        [[ "$line" =~ Rate:\ (.+) ]]             && ib_rate="${BASH_REMATCH[1]// /}"
        [[ "$line" =~ Firmware\ version:\ (.+) ]] && ib_fw="${BASH_REMATCH[1]// /}"
    done < "$ibstat_file"
    # Last entry
    if [[ -n "$current_ca" ]]; then
        [[ $ib_first -eq 0 ]] && ib_json_arr+=","
        ib_json_arr+=$(printf '{"ca": "%s", "state": "%s", "rate": "%s", "firmware": "%s"}' \
            "$(json_escape "$current_ca")" "$(json_escape "$ib_state")" \
            "$(json_escape "$ib_rate")"    "$(json_escape "$ib_fw")")
        ib_txt+=$(printf "  %-10s  State:%-8s Rate:%-6s FW:%s\n" \
            "$current_ca" "$ib_state" "$ib_rate" "$ib_fw")
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
  "interfaces": $iface_json_arr,
  "infiniband": $ib_json_arr
}
EOF

# ─── Write text summary ───────────────────────────────────────────────────────
cat > "$OUT/network.txt" <<EOF
=== NETWORK ===
  Default GW:  ${default_gw:-N/A}
  DNS:         ${dns_servers:-N/A}

-- IP Interfaces --
${iface_txt:-  (none found)}
-- InfiniBand --
${ib_txt:-  (no ibstat data)}
EOF

iface_count=$(echo "$iface_txt" | grep -c '\.')
log_ok "Network parsed: $iface_count interfaces, IB: $(echo "$ib_txt" | grep -c 'State')"
