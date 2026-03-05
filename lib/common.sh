#!/usr/bin/env bash
# lib/common.sh — shared utilities for sos-analyzer

# ─── Color codes ────────────────────────────────────────────────────────────
RED='\033[0;31m'; YELLOW='\033[1;33m'; GREEN='\033[0;32m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

# ─── Logging ────────────────────────────────────────────────────────────────
log_info()  { echo -e "${CYAN}[INFO]${RESET}  $*"; }
log_ok()    { echo -e "${GREEN}[OK]${RESET}    $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${RESET}  $*"; }
log_error() { echo -e "${RED}[ERROR]${RESET} $*" >&2; }

# ─── JSON helpers ───────────────────────────────────────────────────────────
# Escape a string for safe inclusion in JSON
json_escape() {
    local s="$1"
    s="${s//\\/\\\\}"   # backslash
    s="${s//\"/\\\"}"   # double quote
    s="${s//$'\n'/\\n}" # newline
    s="${s//$'\t'/\\t}" # tab
    echo "$s"
}

# Write a simple key:value JSON string field
json_str() {
    local key="$1" val
    val=$(json_escape "$2")
    printf '"%s": "%s"' "$key" "$val"
}

# Write a simple key:value JSON number field
json_num() {
    printf '"%s": %s' "$1" "$2"
}

# ─── SOS report root finder ─────────────────────────────────────────────────
# Given a tarball path or already-extracted dir, return the report root dir
find_sos_root() {
    local input="$1"
    if [[ -f "$input" ]]; then
        # It's a tarball — extract to a temp area and return extracted path
        local work_dir
        work_dir=$(dirname "$input")
        tar -xf "$input" -C "$work_dir" 2>/dev/null
        # Find the extracted directory (first sosreport-* dir)
        find "$work_dir" -maxdepth 1 -type d -name "sosreport-*" | head -1
    elif [[ -d "$input" ]]; then
        echo "$input"
    else
        log_error "Cannot find SOS report: $input"
        return 1
    fi
}

# ─── Threshold flags ─────────────────────────────────────────────────────────
DISK_WARN_PCT=70
DISK_CRIT_PCT=85
MEM_WARN_PCT=80
MEM_CRIT_PCT=90
LOAD_WARN_MULTIPLIER=2   # load avg > N * cpu_count = warning

flag_disk() {
    local pct="$1"
    if   (( pct >= DISK_CRIT_PCT )); then echo "CRITICAL"
    elif (( pct >= DISK_WARN_PCT )); then echo "WARNING"
    else echo "OK"
    fi
}

flag_mem() {
    local pct="$1"
    if   (( pct >= MEM_CRIT_PCT )); then echo "CRITICAL"
    elif (( pct >= MEM_WARN_PCT )); then echo "WARNING"
    else echo "OK"
    fi
}
