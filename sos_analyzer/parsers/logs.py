"""
sos_analyzer/parsers/logs.py — wrapper around parse_logs.py
"""
from pathlib import Path
import sys
import os

def parse(sos_root: Path, out_dir: Path) -> dict:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    
    # Run the original script as a subprocess to avoid module-level execution issues
    import subprocess
    impl = Path(__file__).parent / "_logs_impl.py"
    result = subprocess.run(
        [sys.executable, str(impl), str(sos_root), str(out_dir)],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"  [WARN] logs parser: {result.stderr.strip()}", file=sys.stderr)
    
    # Load and return the generated JSON
    from ..common import load_json
    return load_json(out_dir / "logs.json")
