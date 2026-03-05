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
INPUTS=()

# ─── Parse arguments ──────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --output)         OUTPUT_DIR="$2"; shift 2 ;;
        --format)         EXPORT_FORMAT="$2"; shift 2 ;;
        --keep-extracted) KEEP_EXTRACTED=1; shift ;;
        --no-export)      NO_EXPORT=1; shift ;;
        --help|-h)
            echo "Usage: $0 [options] <sos-report(s) or directory>"
            echo ""
            echo "Options:"
            echo "  --output <dir>        Output base directory (default: ./sos-output)"
            echo "  --format <fmt>        csv|html|pdf|all (default: all)"
            echo "  --keep-extracted      Retain extracted SOS directories"
            echo "  --no-export           Skip CSV/HTML/PDF generation"
            exit 0 ;;
        *) INPUTS+=("$1"); shift ;;
    esac
done

[[ ${#INPUTS[@]} -eq 0 ]] && { log_error "No input files or directories specified."; exit 1; }

RESULTS_DIR="$OUTPUT_DIR/nodes"
CLUSTER_DIR="$OUTPUT_DIR/cluster"
mkdir -p "$RESULTS_DIR" "$CLUSTER_DIR"

# ─── Helper: extract tarball, return sos report root path ────────────────────
extract_tarball() {
    local tb="$1"
    local extract_to="$2"
    log_info "Extracting: $(basename "$tb") ..."
    tar -xf "$tb" -C "$extract_to" --no-same-owner 2>/dev/null
    # tar creates its own sosreport-* dir inside extract_to — find version.txt at depth 2
    find "$extract_to" -maxdepth 2 -name "version.txt" -exec dirname {} \; 2>/dev/null | head -1
}

# ─── Collect SOS report directories ──────────────────────────────────────────
sos_dirs=()
extract_to="$OUTPUT_DIR/extracted"
mkdir -p "$extract_to"

for input in "${INPUTS[@]}"; do
    if [[ -f "$input" && "$input" =~ \.(tar\.xz|tar\.gz|tar\.bz2|tgz)$ ]]; then
        # Single tarball
        extracted=$(extract_tarball "$input" "$extract_to")
        if [[ -n "$extracted" ]]; then
            sos_dirs+=("$extracted")
        else
            log_warn "Could not find sos report root inside $input — skipping"
        fi

    elif [[ -d "$input" ]]; then
        if [[ -f "$input/version.txt" || -f "$input/hostname" ]] ; then
            # Already-extracted single report
            sos_dirs+=("$input")
        else
            # Directory containing multiple tarballs — extract each one
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

log_info "Found ${#sos_dirs[@]} SOS report(s) to process"
echo ""

# ─── Parse each node ──────────────────────────────────────────────────────────
chmod +x "$SCRIPT_DIR/parsers/"*.sh

parsed=0
failed=0
for sos in "${sos_dirs[@]}"; do
    # Support both standard sos layout (hostname) and raw layout (proc/sys/kernel/hostname)
    hostname=$(cat "$sos/hostname" 2>/dev/null | tr -d '[:space:]')
    [[ -z "$hostname" ]] && hostname=$(cat "$sos/proc/sys/kernel/hostname" 2>/dev/null | tr -d '[:space:]')
    [[ -z "$hostname" ]] && hostname=$(basename "$sos")

    node_out="$RESULTS_DIR/$hostname"
    mkdir -p "$node_out"

    echo -e "${BOLD}── Processing: $hostname ──────────────────────────────────────────────${RESET}"

    ok=1
    for parser in identity resources services network logs rpms lustre; do
        script="$SCRIPT_DIR/parsers/parse_${parser}.sh"
        if [[ -x "$script" ]]; then
            if ! bash "$script" "$sos" "$node_out" 2>/dev/null; then
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
        cat "$node_out/rpms.txt"      2>/dev/null
    } > "$node_out/node_summary.txt"

    if (( ok == 1 )); then
        (( parsed++ )) || true
    else
        (( failed++ )) || true
    fi
    echo ""
done

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
