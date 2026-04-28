"""
sos_analyzer/cli.py — main entry point

Usage:
    sos-analyzer --input <dir-of-sos-reports> --output <output-dir> [options]
    python3 -m sos_analyzer --input ... --output ...

Input can be:
  - A directory containing SOS tarballs (sosreport-*.tar.xz)
  - A directory containing extracted SOS directories (sosreport-*/hostname exists)
  - Mixed — both tarballs and extracted dirs are handled

Output structure:
  <output>/
    nodes/<hostname>/*.json, *.txt
    cluster/
      cluster_diff.json
      cluster_summary.txt
      cluster_report_ai.html
      exports/*.csv
"""
from __future__ import annotations

import argparse
import concurrent.futures
import sys
import time
from pathlib import Path

from .common import discover_sos_reports, hostname_from_sos
from . import parsers
from .aggregate import aggregate
from .export import export_csv


def parse_node(sos_root: Path, out_base: Path, conf_dir: Path) -> tuple[str, bool]:
    """Parse one SOS report. Returns (hostname, success)."""
    hostname = hostname_from_sos(sos_root)
    out_dir  = out_base / hostname

    try:
        parsers.identity.parse(sos_root, out_dir)
        parsers.resources.parse(sos_root, out_dir)
        parsers.services.parse(sos_root, out_dir, conf_dir)
        parsers.network.parse(sos_root, out_dir)
        parsers.logs.parse(sos_root, out_dir)
        parsers.rpms.parse(sos_root, out_dir)
        parsers.lustre.parse(sos_root, out_dir)
        parsers.sfa.parse(sos_root, out_dir)
        parsers.sysctl.parse(sos_root, out_dir)

        # Node summary text
        summary_parts = []
        for name in ["identity", "resources", "services", "network",
                      "logs", "lustre", "rpms", "sfa", "sysctl"]:
            txt = out_dir / f"{name}.txt"
            if txt.exists():
                summary_parts.append(txt.read_text())
        (out_dir / "node_summary.txt").write_text("\n".join(summary_parts))

        return hostname, True
    except Exception as e:
        print(f"  [ERROR] {hostname}: {e}", file=sys.stderr)
        return hostname, False


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="sos-analyzer",
        description="Analyze DDN Lustre cluster SOS reports and generate an HTML dashboard.",
    )
    parser.add_argument("--input",  "-i", required=True,
                        help="Directory of SOS tarballs or extracted reports")
    parser.add_argument("--output", "-o", default=None,
                        help="Output directory for results (default: ./reports/<timestamp>)")
    parser.add_argument("--jobs",   "-j", type=int, default=0,
                        help="Parallel jobs for node parsing (0=auto)")
    parser.add_argument("--no-report", action="store_true",
                        help="Skip HTML report generation")
    parser.add_argument("--no-csv",    action="store_true",
                        help="Skip CSV export")
    parser.add_argument("--no-llm",   action="store_true",
                        help="Skip LLM narrative summary in HTML report")
    parser.add_argument("--debug",     action="store_true",
                        help="Write debug files")
    args = parser.parse_args(argv)

    from datetime import datetime
    input_path  = Path(args.input).expanduser().resolve()
    if args.output is None:
        timestamp   = datetime.now().strftime("%Y%m%d-%H%M%S")
        output_path = Path.cwd() / "reports" / timestamp
        print(f"[*] No --output specified, using: {output_path}")
    else:
        output_path = Path(args.output).expanduser().resolve()
    script_dir  = Path(__file__).parent
    conf_dir    = script_dir / "conf"

    # ── Discover SOS reports ──
    print(f"[*] Discovering SOS reports in: {input_path}")
    sos_roots = discover_sos_reports(input_path)
    if not sos_roots:
        print(f"[ERROR] No SOS reports found in {input_path}", file=sys.stderr)
        return 1
    print(f"[*] Found {len(sos_roots)} SOS reports")

    # ── Set up output directories ──
    nodes_dir   = output_path / "nodes"
    cluster_dir = output_path / "cluster"
    exports_dir = cluster_dir / "exports"
    nodes_dir.mkdir(parents=True, exist_ok=True)
    cluster_dir.mkdir(parents=True, exist_ok=True)

    # ── Parse nodes in parallel ──
    t0      = time.monotonic()
    n_jobs  = args.jobs or min(len(sos_roots), 8)
    success = 0
    failed  = 0

    print(f"[*] Parsing {len(sos_roots)} nodes ({n_jobs} parallel workers)...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=n_jobs) as executor:
        futures = {
            executor.submit(parse_node, root, nodes_dir, conf_dir): root
            for root in sos_roots
        }
        for future in concurrent.futures.as_completed(futures):
            hostname, ok = future.result()
            if ok:
                success += 1
                print(f"  ✓ {hostname}")
            else:
                failed += 1
                print(f"  ✗ {hostname} (failed)")

    parse_time = time.monotonic() - t0
    print(f"[*] Parsed {success}/{len(sos_roots)} nodes in {parse_time:.1f}s")

    if success == 0:
        print("[ERROR] All nodes failed to parse", file=sys.stderr)
        return 1

    # ── Aggregate ──
    print(f"[*] Aggregating cluster data...")
    cluster_diff = aggregate(nodes_dir, cluster_dir)
    n_nodes = cluster_diff["cluster_summary"]["node_count"]
    print(f"    {n_nodes} nodes aggregated → {cluster_dir / 'cluster_diff.json'}")

    # ── CSV exports ──
    if not args.no_csv:
        print(f"[*] Generating CSV exports...")
        export_csv(nodes_dir, exports_dir)
        csvs = list(exports_dir.glob("*.csv"))
        print(f"    {len(csvs)} CSV files → {exports_dir}/")

    # ── HTML report ──
    if not args.no_report:
        print(f"[*] Generating HTML dashboard...")
        from .report import build_report
        build_report(
            output_path,
            no_llm=args.no_llm,
            debug=args.debug,
        )

    # ── Summary ──
    total_time = time.monotonic() - t0
    print(f"\n{'═'*55}")
    print(f" Output:          {output_path}")
    print(f" Nodes:           {nodes_dir}/")
    print(f" Cluster summary: {cluster_dir}/cluster_summary.txt")
    print(f" Cluster diff:    {cluster_dir}/cluster_diff.json")
    if not args.no_csv:
        print(f" CSV exports:     {exports_dir}/")
    if not args.no_report:
        print(f" HTML report:     {cluster_dir}/cluster_report_ai.html")
    print(f" Total time:      {total_time:.1f}s")
    print(f"{'═'*55}")

    return 0 if failed == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
