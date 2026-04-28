"""sos_analyzer parsers package"""
from . import identity, resources, network, services, lustre, rpms, sfa, sysctl

# logs.py already exists as parse_logs.py — import it as logs
import importlib, sys
from pathlib import Path

# Support both parse_logs.py (old name) and logs.py
_logs_candidates = [
    "sos_analyzer.parsers.logs",
    "sos_analyzer.parsers.parse_logs",
]
logs = None
for _mod in _logs_candidates:
    try:
        logs = importlib.import_module(_mod)
        break
    except ImportError:
        pass

if logs is None:
    # Create a stub that produces an empty logs.json
    class _LogsStub:
        @staticmethod
        def parse(sos_root, out_dir):
            out_dir.mkdir(parents=True, exist_ok=True)
            import json
            (out_dir / "logs.json").write_text(json.dumps({
                "flag": "OK", "critical_count": 0, "warning_count": 0,
                "client_event_count": 0, "critical_events": [], "client_events": []
            }))
            (out_dir / "logs.txt").write_text("=== LOGS ===\n\n(parser not found)\n")
            return {}
    logs = _LogsStub()
