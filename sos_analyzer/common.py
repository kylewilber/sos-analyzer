"""
sos_analyzer/common.py — shared utilities, thresholds, and flag logic
"""
from __future__ import annotations
import json
import re
from pathlib import Path
from typing import Any

# ─── Flag thresholds ──────────────────────────────────────────────────────────

DISK_WARN_PCT  = 70
DISK_CRIT_PCT  = 85
MEM_WARN_PCT   = 80
MEM_CRIT_PCT   = 90


def flag_disk(pct: int) -> str:
    if pct >= DISK_CRIT_PCT:  return "CRITICAL"
    if pct >= DISK_WARN_PCT:  return "WARNING"
    return "OK"


def flag_mem(pct: int) -> str:
    if pct >= MEM_CRIT_PCT:  return "CRITICAL"
    if pct >= MEM_WARN_PCT:  return "WARNING"
    return "OK"


def worst_flag(*flags: str) -> str:
    """Return the most severe flag from a list."""
    for f in flags:
        if str(f).upper() == "CRITICAL":
            return "CRITICAL"
    for f in flags:
        if str(f).upper() == "WARNING":
            return "WARNING"
    return "OK"


# ─── File helpers ─────────────────────────────────────────────────────────────

def read_file(path: Path, default: str = "") -> str:
    """Read a text file, return default if missing."""
    try:
        return path.read_text(errors="replace").strip()
    except Exception:
        return default


def read_lines(path: Path) -> list[str]:
    """Read non-empty lines from a file."""
    try:
        return [l.rstrip() for l in path.read_text(errors="replace").splitlines() if l.strip()]
    except Exception:
        return []


def load_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def write_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, indent=2))


# ─── SOS root finder ──────────────────────────────────────────────────────────

def find_sos_root(path: Path) -> Path | None:
    """
    Given a path that is either:
      - an already-extracted SOS directory (contains 'hostname' file)
      - a tarball (.tar.gz, .tar.xz, .tar.bz2)
    Return the path to the SOS root directory, extracting if needed.
    """
    if path.is_dir():
        # Already extracted — verify it looks like a SOS report
        if (path / "hostname").exists() or (path / "uname").exists():
            return path
        # Maybe it's a parent directory containing a single sosreport-* subdir
        candidates = list(path.glob("sosreport-*"))
        if candidates:
            return candidates[0]
        return None

    if path.is_file() and re.search(r'\.(tar\.(gz|xz|bz2)|tgz)$', path.name):
        import tarfile
        extract_dir = path.parent / path.name.replace(".tar.gz", "").replace(".tar.xz", "").replace(".tar.bz2", "").replace(".tgz", "")
        if not extract_dir.exists():
            with tarfile.open(path) as tf:
                tf.extractall(path.parent)
        # Find the extracted root
        candidates = list(path.parent.glob("sosreport-*"))
        if candidates:
            return sorted(candidates)[-1]
        if extract_dir.exists():
            return extract_dir

    return None


def discover_sos_reports(input_path: Path) -> list[Path]:
    """
    Discover SOS report roots in input_path.
    Handles: sos-collector dirs, tarballs, pre-extracted dirs, or mixed.
    Prefers pre-extracted dirs over tarballs when both exist for same report.
    """
    roots: list[Path] = []
    seen_basenames: set[str] = set()

    if input_path.is_file():
        root = find_sos_root(input_path)
        if root:
            roots.append(root)
        return roots

    if input_path.is_dir():
        # Single SOS root?
        if (input_path / "hostname").exists():
            return [input_path]

        # Collect all candidate directories to search
        search_dirs = [input_path]
        for d in sorted(input_path.iterdir()):
            if d.is_dir() and d.name.startswith("sos-collector-"):
                search_dirs.append(d)

        for search_dir in search_dirs:
            # Pass 1: pre-extracted dirs (preferred)
            for d in sorted(search_dir.iterdir()):
                if d.is_dir() and d.name.startswith("sosreport-"):
                    if (d / "hostname").exists() or (d / "uname").exists():
                        if d not in roots:
                            roots.append(d)
                            seen_basenames.add(d.name)

            # Pass 2: tarballs — skip if already have extracted version
            for tarball in sorted(search_dir.glob("sosreport-*.tar.*")):
                base = tarball.name
                for ext in (".tar.xz", ".tar.gz", ".tar.bz2", ".tgz"):
                    base = base.replace(ext, "")
                if base not in seen_basenames:
                    root = find_sos_root(tarball)
                    if root and root not in roots:
                        roots.append(root)
                        seen_basenames.add(base)

    return roots

def hostname_from_sos(sos_root: Path) -> str:
    """Extract hostname from SOS root."""
    h = read_file(sos_root / "hostname")
    if not h:
        h = read_file(sos_root / "proc" / "sys" / "kernel" / "hostname")
    if not h:
        # Try to extract from directory name: sosreport-HOSTNAME-...
        m = re.match(r'sosreport-([^-]+-[^-]+-[^-]+-[^-]+)', sos_root.name)
        if m:
            h = m.group(1)
    return h.strip() or sos_root.name