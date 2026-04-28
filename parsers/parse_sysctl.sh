#!/usr/bin/env bash
# parsers/parse_sysctl.sh — kernel sysctl parameter extraction and drift detection
# Usage: parse_sysctl.sh <sos_root> <output_dir>
#
# Extracts a curated set of sysctl parameters relevant to Lustre/HPC storage:
#   - vm.*       : memory management tuning
#   - net.core.* : network buffer sizing
#   - net.ipv4.* : TCP tuning and interface security settings
#   - kernel.*   : hung task, NUMA, scheduler settings
#
# Parameters that vary by design (runtime state, per-CPU counters, UUIDs,
# hostnames, random seeds) are excluded from the drift-relevant output.

source "$(dirname "$0")/../lib/common.sh"

SOS="$1"
OUT="$2"

[[ -z "$SOS" || -z "$OUT" ]] && { log_error "Usage: $0 <sos_root> <output_dir>"; exit 1; }
mkdir -p "$OUT"

SYSCTL_FILE="$SOS/sos_commands/kernel/sysctl_-a"
if [[ ! -f "$SYSCTL_FILE" ]]; then
    log_warn "sysctl_-a not found at $SYSCTL_FILE"
    cat > "$OUT/sysctl.json" <<EOF
{"available": false, "flag": "UNKNOWN", "params": {}, "drift_flags": []}
EOF
    printf "=== SYSCTL ===\n\nNot available.\n" > "$OUT/sysctl.txt"
    exit 0
fi

# ─── Parameters to extract ────────────────────────────────────────────────────
# Curated list: meaningful for Lustre/HPC storage node configuration.
# Excludes: runtime state (dentry-state, inode-nr, quota counters),
#           per-CPU sched domain costs, UUIDs, hostnames, entropy pools.

PARAMS=(
    # VM / memory management
    vm.dirty_ratio
    vm.dirty_background_ratio
    vm.dirty_bytes
    vm.dirty_background_bytes
    vm.dirty_expire_centisecs
    vm.dirty_writeback_centisecs
    vm.swappiness
    vm.overcommit_memory
    vm.overcommit_ratio
    vm.min_free_kbytes
    vm.vfs_cache_pressure
    vm.zone_reclaim_mode
    vm.numa_stat
    vm.nr_hugepages
    vm.max_map_count
    vm.panic_on_oom

    # Network core buffers
    net.core.rmem_max
    net.core.wmem_max
    net.core.rmem_default
    net.core.wmem_default
    net.core.netdev_max_backlog
    net.core.somaxconn
    net.core.optmem_max

    # TCP tuning
    net.ipv4.tcp_rmem
    net.ipv4.tcp_wmem
    net.ipv4.tcp_mem
    net.ipv4.tcp_congestion_control
    net.ipv4.tcp_timestamps
    net.ipv4.tcp_sack
    net.ipv4.tcp_low_latency
    net.ipv4.tcp_slow_start_after_idle
    net.ipv4.tcp_syncookies
    net.ipv4.tcp_fin_timeout
    net.ipv4.tcp_keepalive_time
    net.ipv4.tcp_keepalive_intvl
    net.ipv4.tcp_keepalive_probes
    net.ipv4.tcp_max_syn_backlog
    net.ipv4.udp_mem

    # Interface security (these SHOULD be uniform — drift is a finding)
    net.ipv4.conf.all.accept_redirects
    net.ipv4.conf.all.secure_redirects
    net.ipv4.conf.all.send_redirects
    net.ipv4.conf.default.accept_redirects
    net.ipv4.conf.default.secure_redirects
    net.ipv4.conf.mgmt0.accept_redirects
    net.ipv4.conf.mgmt0.secure_redirects
    net.ipv4.conf.mlxib0.accept_redirects
    net.ipv4.conf.mlxib0.secure_redirects
    net.ipv4.conf.mlxib1.accept_redirects
    net.ipv4.conf.mlxib1.secure_redirects

    # Kernel
    kernel.hung_task_timeout_secs
    kernel.hung_task_warnings
    kernel.numa_balancing
    kernel.nmi_watchdog
    kernel.panic
    kernel.panic_on_oops
    kernel.pid_max
    kernel.threads-max
    kernel.sysrq
    kernel.perf_event_paranoid
)

# ─── Recommended values for Lustre OSS/MDS nodes (flag deviations) ───────────
# Format: param=recommended_value  (empty = no recommendation, just extract)
declare -A RECOMMENDED
RECOMMENDED[vm.swappiness]="10"
RECOMMENDED[vm.zone_reclaim_mode]="0"
RECOMMENDED[vm.numa_stat]="1"
RECOMMENDED[kernel.numa_balancing]="0"
RECOMMENDED[net.ipv4.tcp_timestamps]="0"
RECOMMENDED[net.ipv4.tcp_low_latency]="1"
RECOMMENDED[net.ipv4.tcp_slow_start_after_idle]="0"
RECOMMENDED[net.ipv4.conf.mgmt0.accept_redirects]="0"
RECOMMENDED[net.ipv4.conf.mgmt0.secure_redirects]="0"
RECOMMENDED[net.ipv4.conf.mlxib0.accept_redirects]="0"
RECOMMENDED[net.ipv4.conf.mlxib0.secure_redirects]="0"
RECOMMENDED[net.ipv4.conf.mlxib1.accept_redirects]="0"
RECOMMENDED[net.ipv4.conf.mlxib1.secure_redirects]="0"
RECOMMENDED[kernel.hung_task_warnings]="10"

# ─── Extract values ───────────────────────────────────────────────────────────
declare -A VALUES
for param in "${PARAMS[@]}"; do
    val=$(grep "^${param} " "$SYSCTL_FILE" 2>/dev/null | sed 's/^[^ ]* = //')
    VALUES[$param]="${val:-NOT_FOUND}"
done

# ─── Flag deviations from recommendations ────────────────────────────────────
drift_flags=()
for param in "${!RECOMMENDED[@]}"; do
    rec="${RECOMMENDED[$param]}"
    actual="${VALUES[$param]}"
    if [[ "$actual" == "NOT_FOUND" ]]; then
        continue
    fi
    if [[ "$actual" != "$rec" ]]; then
        drift_flags+=("$param")
    fi
done

# Determine overall flag
overall_flag="OK"
if [[ ${#drift_flags[@]} -gt 0 ]]; then
    # Check if any drift flags are on security-sensitive params
    for f in "${drift_flags[@]}"; do
        if [[ "$f" == *"accept_redirects"* || "$f" == *"secure_redirects"* ]]; then
            overall_flag="WARNING"
            break
        fi
    done
    [[ "$overall_flag" == "OK" ]] && overall_flag="INFO"
fi

# ─── Write JSON ───────────────────────────────────────────────────────────────
# Build params object
params_json="{"
first=1
for param in "${PARAMS[@]}"; do
    val="${VALUES[$param]}"
    [[ $first -eq 0 ]] && params_json+=","
    params_json+="\"$(json_escape "$param")\": \"$(json_escape "$val")\""
    first=0
done
params_json+="}"

# Build drift_flags array
drift_json="["
first=1
for f in "${drift_flags[@]}"; do
    [[ $first -eq 0 ]] && drift_json+=","
    rec="${RECOMMENDED[$f]}"
    actual="${VALUES[$f]}"
    drift_json+="{\"param\": \"$(json_escape "$f")\", \"actual\": \"$(json_escape "$actual")\", \"recommended\": \"$(json_escape "$rec")\"}"
    first=0
done
drift_json+="]"

cat > "$OUT/sysctl.json" <<EOF
{
  "available": true,
  "flag": "$overall_flag",
  "drift_count": ${#drift_flags[@]},
  "params": $params_json,
  "drift_flags": $drift_json
}
EOF

# ─── Write text summary ───────────────────────────────────────────────────────
{
printf "=== SYSCTL ===\n\n"
printf "Overall flag: %s  |  Drift findings: %d\n\n" "$overall_flag" "${#drift_flags[@]}"

printf "-- Memory Management --\n"
for p in vm.dirty_ratio vm.dirty_background_ratio vm.dirty_expire_centisecs \
          vm.dirty_writeback_centisecs vm.swappiness vm.overcommit_memory \
          vm.min_free_kbytes vm.vfs_cache_pressure vm.zone_reclaim_mode \
          vm.nr_hugepages vm.panic_on_oom; do
    val="${VALUES[$p]}"
    rec="${RECOMMENDED[$p]:-}"
    marker=""
    [[ -n "$rec" && "$val" != "$rec" ]] && marker="  ← recommended: $rec"
    printf "  %-45s = %s%s\n" "$p" "$val" "$marker"
done

printf "\n-- Network Buffers --\n"
for p in net.core.rmem_max net.core.wmem_max net.core.rmem_default \
          net.core.wmem_default net.core.netdev_max_backlog net.core.somaxconn \
          net.ipv4.tcp_rmem net.ipv4.tcp_wmem net.ipv4.tcp_congestion_control; do
    val="${VALUES[$p]}"
    printf "  %-45s = %s\n" "$p" "$val"
done

printf "\n-- TCP Tuning --\n"
for p in net.ipv4.tcp_timestamps net.ipv4.tcp_low_latency \
          net.ipv4.tcp_slow_start_after_idle net.ipv4.tcp_sack \
          net.ipv4.tcp_fin_timeout net.ipv4.tcp_keepalive_time \
          net.ipv4.tcp_syncookies; do
    val="${VALUES[$p]}"
    rec="${RECOMMENDED[$p]:-}"
    marker=""
    [[ -n "$rec" && "$val" != "$rec" ]] && marker="  ← recommended: $rec"
    printf "  %-45s = %s%s\n" "$p" "$val" "$marker"
done

printf "\n-- Kernel --\n"
for p in kernel.hung_task_timeout_secs kernel.hung_task_warnings \
          kernel.numa_balancing kernel.nmi_watchdog \
          kernel.panic kernel.panic_on_oops kernel.pid_max kernel.threads-max; do
    val="${VALUES[$p]}"
    rec="${RECOMMENDED[$p]:-}"
    marker=""
    [[ -n "$rec" && "$val" != "$rec" ]] && marker="  ← recommended: $rec"
    printf "  %-45s = %s%s\n" "$p" "$val" "$marker"
done

if [[ ${#drift_flags[@]} -gt 0 ]]; then
    printf "\n-- Drift Findings --\n"
    for f in "${drift_flags[@]}"; do
        printf "  %-45s actual=%-15s recommended=%s\n" \
            "$f" "${VALUES[$f]}" "${RECOMMENDED[$f]}"
    done
fi

} > "$OUT/sysctl.txt"

log_ok "Sysctl parsed (${#PARAMS[@]} params, ${#drift_flags[@]} drift findings) [$overall_flag]"
