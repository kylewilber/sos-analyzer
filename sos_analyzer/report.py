"""
sos_analyzer/report.py — HTML dashboard generation
Thin wrapper that calls generate_report.py logic as a library function.
"""
from __future__ import annotations
from pathlib import Path


def build_report(report_dir: Path, no_llm: bool = False, debug: bool = False) -> Path:
    """
    Generate the HTML dashboard for a completed report directory.
    Returns path to the generated HTML file.
    """
    import sys
    import importlib.util

    # Try to import generate_report from the project root
    # (it lives alongside the sos_analyzer package)
    pkg_parent = Path(__file__).parent.parent
    gr_path    = pkg_parent / "generate_report.py"

    if gr_path.exists():
        spec   = importlib.util.spec_from_file_location("generate_report", gr_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        # Replicate the main() logic inline
        from datetime import datetime, timezone
        generated    = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        cluster_diff, nodes = module.load_cluster(report_dir)

        correlations = module.compute_correlations(nodes)
        print(f"    {len(correlations)} correlation groups: {', '.join(correlations.keys())}")

        narrative = ""
        if not no_llm:
            narrative = module.generate_narrative(correlations, nodes)
            if narrative:
                print(f"    LLM narrative: {len(narrative.split())} words")

        html = module.build_html(report_dir, nodes, correlations, generated, narrative)
        output_path = report_dir / "cluster" / module.OUTPUT_FILE
        output_path.write_text(html)
        size_kb = len(html) // 1024
        print(f"    Dashboard: {output_path} ({size_kb} KB)")
        print(f"    Open: file://{output_path}")
        return output_path

    else:
        # generate_report.py not found — skip HTML generation
        print("    [WARN] generate_report.py not found — skipping HTML dashboard", file=sys.stderr)
        return report_dir / "cluster" / "cluster_report_ai.html"
