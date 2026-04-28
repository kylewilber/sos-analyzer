"""
sos_analyzer/parsers/rpms.py — installed RPM packages
"""
from __future__ import annotations
import re
from pathlib import Path
from ..common import read_lines, write_json


def parse(sos_root: Path, out_dir: Path) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)

    rpm_file = sos_root / "installed-rpms"
    if not rpm_file.exists():
        result = {"total": 0, "packages": []}
        write_json(out_dir / "rpms.json", result)
        (out_dir / "rpms.txt").write_text("=== INSTALLED PACKAGES ===\n\nNo installed-rpms file found.\n")
        return result

    packages = []
    for line in read_lines(rpm_file):
        if not line:
            continue
        # Format: name-version-release.arch    date
        parts    = line.split()
        pkg      = parts[0]
        idate    = " ".join(parts[1:]) if len(parts) > 1 else ""

        m = re.match(r'^(.+)-([^-]+)-([^-]+)\.([^.]+)$', pkg)
        if m:
            name, version, release, arch = m.groups()
        else:
            name = pkg; version = release = arch = ""

        packages.append({
            "name":         name,
            "version":      version,
            "release":      release,
            "arch":         arch,
            "install_date": idate,
        })

    result = {"total": len(packages), "packages": packages}
    write_json(out_dir / "rpms.json", result)

    newest = packages[-1]["name"] + "-" + packages[-1]["version"] if packages else "N/A"
    oldest = packages[0]["name"]  + "-" + packages[0]["version"]  if packages else "N/A"

    (out_dir / "rpms.txt").write_text(
        f"=== INSTALLED PACKAGES ===\n"
        f"  Total RPMs:     {len(packages)}\n"
        f"  Newest install: {newest}\n"
        f"  Oldest install: {oldest}\n"
        f"  (Full list in rpms.json)\n"
    )

    return result
