"""
exascaler.py — Parse /etc/ddn/exascaler.toml and compare against
               lctl params-all to detect configuration drift.
"""
from pathlib import Path
import re

try:
    import tomllib
except ImportError:
    try:
        import tomli as tomllib
    except ImportError:
        tomllib = None

from sos_analyzer.common import write_json, read_lines


def _toml_rpc_size_to_pages(val) -> int:
    """Convert EMF RPC size string (e.g. '16M') to Lustre page count."""
    s = str(val).strip().upper()
    if s.endswith('M'):
        return int(float(s[:-1]) * 1024 * 1024) // 4096
    if s.endswith('K'):
        return int(float(s[:-1]) * 1024) // 4096
    try:
        return int(s) // 4096
    except ValueError:
        return 0


def _normalize_param_name(name: str) -> str:
    """Strip hex addresses and _UUID suffixes from lctl param names."""
    # osc.ntglfs01-OST0000-osc-ffff8bde9e4da000 -> osc.ntglfs01-OST0000-osc
    name = re.sub(r'-[0-9a-f]{12,}$', '', name)
    # ldlm.namespaces.filter-ntglfs01-OST0000_UUID -> ldlm.namespaces.filter-ntglfs01-OST0000
    name = re.sub(r'_UUID$', '', name)
    return name


def _wildcard_match(pattern: str, name: str) -> bool:
    """Match a TOML wildcard pattern (e.g. 'osc.ntglfs01*') against a param name."""
    # Convert glob-style * to regex
    regex = re.escape(pattern).replace(r'\*', '.*')
    return bool(re.match(f'^{regex}$', name))


def _parse_params_all(params_file: Path) -> dict[str, str]:
    """Parse lctl params-all into a normalized dict."""
    params = {}
    if not params_file.exists():
        return params
    for line in read_lines(params_file):
        # Skip multi-line stat blocks
        if line.startswith(' ') or '\t' in line:
            continue
        # Handle both key=value and key: value formats
        if '=' in line:
            key, _, val = line.partition('=')
        elif ': ' in line and not line.startswith('#'):
            key, _, val = line.partition(': ')
        else:
            continue
        key = _normalize_param_name(key.strip())
        val = val.strip()
        if key and val:
            params[key] = val
    return params


def parse(sos_root: Path, out_dir: Path, hostname: str = "") -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)

    result = {
        "available": False,
        "version": None,
        "filesystems": [],
        "ha_groups": [],
        "set_param_tunings": {},
        "param_drift": [],
        "flag": "OK",
    }

    toml_path = sos_root / "etc" / "ddn" / "exascaler.toml"
    if not toml_path.exists() or tomllib is None:
        write_json(out_dir / "exascaler.json", result)
        return result

    try:
        with open(toml_path, "rb") as f:
            cfg = tomllib.load(f)
    except Exception as e:
        result["error"] = str(e)
        write_json(out_dir / "exascaler.json", result)
        return result

    result["available"] = True
    result["version"] = cfg.get("version")
    result["filesystems"] = list(cfg.get("fs", {}).keys())
    result["ha_groups"] = cfg.get("HA", {}).get("groups", [])

    # Extract set_param_tunings
    tunings = cfg.get("set_param_tunings", {})
    result["set_param_tunings"] = {str(k): str(v) for k, v in tunings.items()}

    # Parse running lctl params from multiple sources
    running = {}
    for params_file in [
        sos_root / "sos_commands" / "lustre" / "params-all",
        sos_root / "sos_commands" / "lustre" / "params-osc_client",
        sos_root / "sos_commands" / "lustre" / "params-osd",
    ]:
        running.update(_parse_params_all(params_file))

    # Compare tunings against running params
    drift = []
    for toml_key, toml_val in tunings.items():
        toml_key = str(toml_key)
        toml_val_str = str(toml_val)
        # Skip llite.* params if no client mounts detected
        if toml_key.startswith('llite.') and not any(
            k.startswith('llite.') for k in running
        ):
            continue

        # Find matching running params
        matched = {k: v for k, v in running.items()
                   if _wildcard_match(toml_key, k)}

        # Fallback: check bare param name (last component after last dot)
        if not matched:
            bare = toml_key.split('.')[-1]
            if bare in running:
                matched = {toml_key: running[bare]}

        if not matched:
            # Param not found in running config
            drift.append({
                "param": toml_key,
                "expected": toml_val_str,
                "actual": "not found",
                "flag": "WARNING",
            })
            continue

        for run_key, run_val in matched.items():
            # Normalize for comparison
            expected = toml_val_str
            actual = run_val

            # Handle page-count conversion for RPC size params
            if 'pages_per_rpc' in run_key:
                try:
                    expected = str(_toml_rpc_size_to_pages(toml_val))
                except Exception:
                    pass

            # Normalize numeric strings
            try:
                if int(expected) == int(actual):
                    continue
            except (ValueError, TypeError):
                pass

            if expected != actual:
                drift.append({
                    "param": run_key,
                    "expected": expected,
                    "actual": actual,
                    "flag": "WARNING",
                })

    result["param_drift"] = drift
    result["drift_count"] = len(drift)
    result["flag"] = "WARNING" if drift else "OK"

    write_json(out_dir / "exascaler.json", result)
    return result
