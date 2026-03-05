#!/usr/bin/env bash
# aggregate.sh — merge all per-node JSON into cluster-wide diff view
# Usage: aggregate.sh <results_dir> <output_dir>
#   results_dir: directory containing per-node subdirs with *.json files
#   output_dir:  where to write cluster_diff.json and cluster_summary.txt

source "$(dirname "$0")/lib/common.sh"

RESULTS="$1"
OUT="$2"

[[ -z "$RESULTS" || -z "$OUT" ]] && { log_error "Usage: $0 <results_dir> <output_dir>"; exit 1; }
mkdir -p "$OUT"

nodes=()
node_dirs=()
for d in "$RESULTS"/*/; do
    [[ -f "$d/identity.json" ]] || continue
    node=$(basename "$d")
    nodes+=("$node")
    node_dirs+=("$d")
done

node_count=${#nodes[@]}
log_info "Aggregating $node_count nodes..."

# ─── Helper: read a field from a JSON file (simple key:"value" pattern) ──────
jget() {
    local file="$1" key="$2"
    grep -oP "\"${key}\":\s*\K(\"[^\"]*\"|-?[0-9.]+)" "$file" 2>/dev/null | head -1 | tr -d '"'
}

# ─── Build cluster JSON ───────────────────────────────────────────────────────
{
echo '{'
echo "  \"cluster_summary\": {"
echo "    \"node_count\": $node_count,"
echo "    \"generated\": \"$(date -u '+%Y-%m-%dT%H:%M:%SZ')\""
echo "  },"

# ── identity section ──
echo '  "nodes": ['
first_node=1
for d in "${node_dirs[@]}"; do
    id="$d/identity.json"
    res="$d/resources.json"
    svc="$d/services.json"
    net="$d/network.json"
    log="$d/logs.json"
    lus="$d/lustre.json"

    [[ $first_node -eq 0 ]] && echo "    ,"
    echo "    {"

    # Identity
    hostname=$(jget "$id" hostname)
    echo "      \"hostname\": \"$hostname\","
    echo "      \"os\": \"$(jget "$id" os_name) $(jget "$id" os_version)\","
    echo "      \"kernel\": \"$(jget "$id" kernel)\","
    echo "      \"cpu_count\": \"$(jget "$id" cpu_count)\","
    echo "      \"uptime_days\": $(jget "$id" uptime_days),"
    echo "      \"collection_date\": \"$(jget "$id" collection_date)\","

    # Resources
    if [[ -f "$res" ]]; then
        echo "      \"mem_used_pct\": $(jget "$res" used_pct),"
        echo "      \"mem_flag\": \"$(jget "$res" flag)\","
        echo "      \"cpu_idle_pct\": \"$(jget "$res" idle_pct)\","
    fi

    # Services
    if [[ -f "$svc" ]]; then
        echo "      \"failed_services\": $(jget "$svc" failed_count),"
        echo "      \"services_flag\": \"$(jget "$svc" flag)\","
    fi

    # Logs
    if [[ -f "$log" ]]; then
        echo "      \"log_critical\": $(jget "$log" critical_count),"
        echo "      \"log_warnings\": $(jget "$log" warning_count),"
        echo "      \"log_flag\": \"$(jget "$log" flag)\","
    fi

    # Lustre
    if [[ -f "$lus" ]]; then
        echo "      \"lustre_flag\": \"$(jget "$lus" flag)\","
        echo "      \"ost_count\": $(jget "$lus" ost_count),"
        echo "      \"ost_critical\": $(jget "$lus" ost_critical),"
        echo "      \"devices_down\": $(jget "$lus" devices_down),"
    fi

    # Overall node flag (worst of all flags)
    node_flags=()
    [[ -f "$res" ]] && node_flags+=("$(jget "$res" flag)")
    [[ -f "$svc" ]] && node_flags+=("$(jget "$svc" flag)")
    [[ -f "$log" ]] && node_flags+=("$(jget "$log" flag)")
    [[ -f "$lus" ]] && node_flags+=("$(jget "$lus" flag)")
    overall="OK"
    for f in "${node_flags[@]}"; do
        [[ "$f" == "CRITICAL" ]] && overall="CRITICAL" && break
        [[ "$f" == "WARNING"  ]] && overall="WARNING"
    done
    echo "      \"overall_flag\": \"$overall\""

    echo -n "    }"
    first_node=0
done
echo ""
echo "  ]"
echo "}"
} > "$OUT/cluster_diff.json"

# ─── Build cluster summary text ───────────────────────────────────────────────
{
    echo "╔══════════════════════════════════════════════════════════════════════════╗"
    echo "║              CLUSTER SOS REPORT SUMMARY                                ║"
    echo "╠══════════════════════════════════════════════════════════════════════════╣"
    printf "║  Nodes: %-5s    Generated: %-35s       ║\n" "$node_count" "$(date -u '+%Y-%m-%d %H:%M UTC')"
    echo "╠═══════════════╦════════╦═══════════╦══════════╦══════════╦═════════════╣"
    printf "║ %-13s ║ %-6s ║ %-9s ║ %-8s ║ %-8s ║ %-11s ║\n" \
        "HOSTNAME" "MEM%" "FAILED SVC" "LOG FLAG" "LUSTRE" "STATUS"
    echo "╠═══════════════╬════════╬═══════════╬══════════╬══════════╬═════════════╣"

    for d in "${node_dirs[@]}"; do
        id="$d/identity.json"; res="$d/resources.json"
        svc="$d/services.json"; log="$d/logs.json"; lus="$d/lustre.json"

        hostname=$(jget "$id" hostname)
        mem_pct=$(jget "$res" used_pct 2>/dev/null || echo "?")
        failed_svc=$(jget "$svc" failed_count 2>/dev/null || echo "?")
        log_flag=$(jget "$log" flag 2>/dev/null || echo "?")
        lus_flag=$(jget "$lus" flag 2>/dev/null || echo "N/A")

        # Overall
        overall="OK"
        for f in "$(jget "$res" flag)" "$(jget "$svc" flag)" "$(jget "$log" flag)" "$(jget "$lus" flag)"; do
            [[ "$f" == "CRITICAL" ]] && overall="CRITICAL" && break
            [[ "$f" == "WARNING"  ]] && overall="WARNING"
        done

        printf "║ %-13s ║ %-6s ║ %-9s ║ %-8s ║ %-8s ║ %-11s ║\n" \
            "${hostname:0:13}" "${mem_pct}%" "$failed_svc" "$log_flag" "$lus_flag" "$overall"
    done
    echo "╚═══════════════╩════════╩═══════════╩══════════╩══════════╩═════════════╝"
    echo ""

    # Flagged issues
    crit_nodes=(); warn_nodes=()
    for d in "${node_dirs[@]}"; do
        id="$d/identity.json"
        hostname=$(jget "$id" hostname)
        res="$d/resources.json"; svc="$d/services.json"
        log="$d/logs.json";      lus="$d/lustre.json"
        for f in "$(jget "$res" flag)" "$(jget "$svc" flag)" "$(jget "$log" flag)" "$(jget "$lus" flag)"; do
            [[ "$f" == "CRITICAL" ]] && crit_nodes+=("$hostname") && break
        done
        for f in "$(jget "$res" flag)" "$(jget "$svc" flag)" "$(jget "$log" flag)" "$(jget "$lus" flag)"; do
            [[ "$f" == "WARNING" ]] && warn_nodes+=("$hostname") && break
        done
    done

    if (( ${#crit_nodes[@]} > 0 )); then
        echo "!! CRITICAL NODES: ${crit_nodes[*]}"
    fi
    if (( ${#warn_nodes[@]} > 0 )); then
        echo "!  WARNING NODES:  ${warn_nodes[*]}"
    fi
    if (( ${#crit_nodes[@]} == 0 && ${#warn_nodes[@]} == 0 )); then
        echo "   All nodes: OK"
    fi
} > "$OUT/cluster_summary.txt"

log_ok "Aggregation complete: $node_count nodes → $OUT/cluster_diff.json"
cat "$OUT/cluster_summary.txt"
