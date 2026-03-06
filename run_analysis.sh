#!/usr/bin/env bash
# run_analysis.sh — main entry point for SOS report analysis
#
# Usage:
#   ./run_analysis.sh /path/to/sos-reports/          # analyze all tarballs in a directory
#   ./run_analysis.sh report1.tar.xz report2.tar.xz  # analyze specific tarballs
#   ./run_analysis.sh /path/to/extracted/dir/         # analyze already-extracted dir
#
# Options:
#   --output <dir>        Output directory (default: ./sos-output)
#   --format <fmt>        Export format: csv|html|pdf|all (default: all)
#   --jobs <n>            Parallel parser jobs (default: nproc)
#   --keep-extracted      Don't delete extracted report dirs after parsing
#   --no-export           Skip export step (only generate JSON/txt per node)

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"

# ─── Defaults ─────────────────────────────────────────────────────────────────
OUTPUT_DIR="./sos-output"
EXPORT_FORMAT="all"
KEEP_EXTRACTED=0
NO_EXPORT=0
JOBS=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)
INPUTS=()

# ─── Parse arguments ──────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --output)         OUTPUT_DIR="$2"; shift 2 ;;
        --format)         EXPORT_FORMAT="$2"; shift 2 ;;
        --jobs)           JOBS="$2"; shift 2 ;;
        --keep-extracted) KEEP_EXTRACTED=1; shift ;;
        --no-export)      NO_EXPORT=1; shift ;;
        --help|-h)
            echo "Usage: $0 [options] <sos-report(s) or directory>"
            echo ""
            echo "Options:"
            echo "  --output <dir>        Output base directory (default: ./sos-output)"
            echo "  --format <fmt>        csv|html|pdf|all (default: all)"
            echo "  --jobs <n>            Parallel parser jobs (default: nproc)"
            echo "  --keep-extracted      Retain extracted SOS directories"
            echo "  --no-export           Skip CSV/HTML/PDF generation"
            exit 0 ;;
        *) INPUTS+=("$1"); shift ;;
    esac
done

[[ ${#INPUTS[@]} -eq 0 ]] && { log_error "No input files or directories specified."; exit 1; }

RESULTS_DIR="$OUTPUT_DIR/nodes"
CLUSTER_DIR="$OUTPUT_DIR/cluster"
TMP_DIR="$OUTPUT_DIR/.tmp"
mkdir -p "$RESULTS_DIR" "$CLUSTER_DIR" "$TMP_DIR"

# ─── Helper: extract tarball, return sos report root path ────────────────────
extract_tarball() {
    local tb="$1"
    local extract_to="$2"
    # Create a unique subdir per tarball so find doesn't see other tarballs' files
    local tb_name
    tb_name=$(basename "$tb" | sed 's/\.tar\.xz$//;s/\.tar\.gz$//;s/\.tar\.bz2$//;s/\.tgz$//')
    local dest="$extract_to/$tb_name"
    mkdir -p "$dest"
    log_info "Extracting: $(basename "$tb") ..."
    tar -xf "$tb" -C "$dest" --no-same-owner 2>/dev/null
    # tar creates sosreport-*/ inside dest, find version.txt at depth 2
    find "$dest" -maxdepth 2 -name "version.txt" -exec dirname {} \; 2>/dev/null | head -1
}

# ─── Collect SOS report directories ──────────────────────────────────────────
sos_dirs=()
extract_to="$OUTPUT_DIR/extracted"
mkdir -p "$extract_to"

for input in "${INPUTS[@]}"; do
    if [[ -f "$input" && "$input" =~ \.(tar\.xz|tar\.gz|tar\.bz2|tgz)$ ]]; then
        extracted=$(extract_tarball "$input" "$extract_to")
        if [[ -n "$extracted" ]]; then
            sos_dirs+=("$extracted")
        else
            log_warn "Could not find sos report root inside $input — skipping"
        fi
    elif [[ -d "$input" ]]; then
        if [[ -f "$input/version.txt" || -f "$input/hostname" ]]; then
            sos_dirs+=("$input")
        else
            # Tarballs in directory
            while IFS= read -r tb; do
                extracted=$(extract_tarball "$tb" "$extract_to")
                if [[ -n "$extracted" ]]; then
                    sos_dirs+=("$extracted")
                else
                    log_warn "Could not find sos report root inside $tb — skipping"
                fi
            done < <(find "$input" -maxdepth 1 -type f \( \
                -name "*.tar.xz" -o -name "*.tar.gz" \
                -o -name "*.tgz"  -o -name "*.tar.bz2" \))
            # Already-extracted subdirs
            while IFS= read -r d; do
                sos_dirs+=("$d")
            done < <(find "$input" -maxdepth 1 -type d \( \
                -name "sosreport-*" \) 2>/dev/null | sort)
        fi
    else
        log_warn "Skipping unrecognized input: $input"
    fi
done

if [[ ${#sos_dirs[@]} -eq 0 ]]; then
    log_error "No valid SOS report directories found."
    exit 1
fi

log_info "Found ${#sos_dirs[@]} SOS report(s) to process (parallel jobs: $JOBS)"
echo ""

# ─── Per-node processing function ────────────────────────────────────────────
chmod +x "$SCRIPT_DIR/parsers/"*.sh

process_node() {
    local sos="$1"
    local results_dir="$2"
    local tmp_dir="$3"
    local script_dir="$4"

    local hostname
    hostname=$(cat "$sos/hostname" 2>/dev/null | tr -d '[:space:]')
    [[ -z "$hostname" ]] && hostname=$(cat "$sos/proc/sys/kernel/hostname" 2>/dev/null | tr -d '[:space:]')
    [[ -z "$hostname" ]] && hostname=$(basename "$sos")

    local node_out="$results_dir/$hostname"
    mkdir -p "$node_out"

    echo -e "${BOLD}── Processing: $hostname ──────────────────────────────────────────────${RESET}"

    local ok=1
    for parser in identity resources services network logs rpms lustre sfa; do
        local py_script="$script_dir/parsers/parse_${parser}.py"
        local sh_script="$script_dir/parsers/parse_${parser}.sh"
        if [[ -x "$py_script" ]]; then
            if ! python3 "$py_script" "$sos" "$node_out" 2>/dev/null; then
                log_warn "Parser $parser (python) failed for $hostname"
                ok=0
            fi
        elif [[ -x "$sh_script" ]]; then
            if ! bash "$sh_script" "$sos" "$node_out" 2>/dev/null; then
                log_warn "Parser $parser failed for $hostname"
                ok=0
            fi
        fi
    done

    # Combine per-node txt into a single node summary
    {
        cat "$node_out/identity.txt"  2>/dev/null; echo ""
        cat "$node_out/resources.txt" 2>/dev/null; echo ""
        cat "$node_out/services.txt"  2>/dev/null; echo ""
        cat "$node_out/network.txt"   2>/dev/null; echo ""
        cat "$node_out/logs.txt"      2>/dev/null; echo ""
        cat "$node_out/lustre.txt"    2>/dev/null; echo ""
        cat "$node_out/rpms.txt"      2>/dev/null; echo ""
        cat "$node_out/sfa.txt"       2>/dev/null
    } > "$node_out/node_summary.txt"

    # Signal completion via tmp file
    if (( ok == 1 )); then
        touch "$tmp_dir/ok_${hostname}"
    else
        touch "$tmp_dir/fail_${hostname}"
    fi
    echo ""
}

export -f process_node
export BOLD RESET

# ─── Run nodes in parallel ────────────────────────────────────────────────────
# Use xargs for portable parallelism (works on Linux and macOS)
printf '%s\n' "${sos_dirs[@]}" | \
    xargs -P "$JOBS" -I{} bash -c \
        'process_node "$1" "$2" "$3" "$4"' _ \
        {} "$RESULTS_DIR" "$TMP_DIR" "$SCRIPT_DIR"

# ─── Count results ────────────────────────────────────────────────────────────
parsed=$(find "$TMP_DIR" -name 'ok_*'   2>/dev/null | wc -l | tr -d ' ')
failed=$(find "$TMP_DIR" -name 'fail_*' 2>/dev/null | wc -l | tr -d ' ')
rm -rf "$TMP_DIR"

echo ""
log_info "Parsed: $parsed nodes  |  Failed: $failed"
echo ""

# ─── Aggregate cluster-wide view ──────────────────────────────────────────────
log_info "Running cluster aggregation..."
bash "$SCRIPT_DIR/aggregate.sh" "$RESULTS_DIR" "$CLUSTER_DIR"
echo ""

# ─── Export ───────────────────────────────────────────────────────────────────
if [[ $NO_EXPORT -eq 0 ]]; then
    log_info "Generating exports (format: $EXPORT_FORMAT)..."
    bash "$SCRIPT_DIR/export.sh" "$RESULTS_DIR" "$CLUSTER_DIR" "$EXPORT_FORMAT"
fi

# ─── Cleanup extracted dirs ───────────────────────────────────────────────────
if [[ $KEEP_EXTRACTED -eq 0 && -d "$OUTPUT_DIR/extracted" ]]; then
    chmod -R u+rwx "$OUTPUT_DIR/extracted" 2>/dev/null
    rm -rf "$OUTPUT_DIR/extracted"
fi

# ─── Final summary ────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}═══════════════════════════════════════════════════════════${RESET}"
echo -e "${BOLD} Output Directory: $OUTPUT_DIR${RESET}"
echo -e "${BOLD}═══════════════════════════════════════════════════════════${RESET}"
echo ""
echo "  Per-node JSON + text:  $RESULTS_DIR/<hostname>/"
echo "  Cluster summary:       $CLUSTER_DIR/cluster_summary.txt"
echo "  Cluster JSON diff:     $CLUSTER_DIR/cluster_diff.json"
if [[ $NO_EXPORT -eq 0 ]]; then
    echo "  CSV exports:           $CLUSTER_DIR/exports/*.csv"
    echo "  HTML report:           $CLUSTER_DIR/reports/cluster_report.html"
    echo "  PDF report:            $CLUSTER_DIR/reports/cluster_report.pdf"
fi
echo ""
