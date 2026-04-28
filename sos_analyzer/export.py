"""
sos_analyzer/export.py — CSV export of cluster data
"""
from __future__ import annotations
import csv
from pathlib import Path
from .common import load_json


def export_csv(nodes_dir: Path, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    node_dirs = sorted(
        [d for d in nodes_dir.iterdir() if d.is_dir() and (d / "identity.json").exists()]
    )

    def load_node(d: Path) -> dict:
        return {
            "ident":   load_json(d / "identity.json"),
            "res":     load_json(d / "resources.json"),
            "svc":     load_json(d / "services.json"),
            "net":     load_json(d / "network.json"),
            "log":     load_json(d / "logs.json"),
            "lus":     load_json(d / "lustre.json"),
            "sfa":     load_json(d / "sfa.json"),
            "rpms":    load_json(d / "rpms.json"),
            "sysctl":  load_json(d / "sysctl.json"),
        }

    # ── cluster_overview.csv ──
    with open(out_dir / "cluster_overview.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["hostname","os","kernel","cpu_count","uptime_days","mem_used_pct",
                    "mem_flag","failed_services","log_flag","lustre_flag","sysctl_flag","overall_flag","collection_date"])
        for d in node_dirs:
            n = load_node(d)
            mem   = n["res"].get("memory", {})
            flags = [mem.get("flag","OK"), n["svc"].get("flag","OK"),
                     n["log"].get("flag","OK"), n["lus"].get("flag","OK")]
            overall = "CRITICAL" if "CRITICAL" in flags else "WARNING" if "WARNING" in flags else "OK"
            w.writerow([
                n["ident"].get("hostname",""),
                f"{n['ident'].get('os_name','')} {n['ident'].get('os_version','')}".strip(),
                n["ident"].get("kernel",""),
                n["ident"].get("cpu_count",""),
                n["ident"].get("uptime_days",0),
                mem.get("used_pct",0),
                mem.get("flag","OK"),
                n["svc"].get("failed_count",0),
                n["log"].get("flag","OK"),
                n["lus"].get("flag","OK"),
                n["sysctl"].get("flag","N/A") if n["sysctl"].get("available") else "N/A",
                overall,
                n["ident"].get("collection_date",""),
            ])

    # ── disk_usage.csv ──
    with open(out_dir / "disk_usage.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["hostname","mount","filesystem","total_gb","used_gb","avail_gb","used_pct","flag"])
        for d in node_dirs:
            n = load_node(d)
            hn = n["ident"].get("hostname","")
            for disk in n["res"].get("disk", []):
                w.writerow([
                    hn, disk.get("mount",""), disk.get("filesystem",""),
                    disk.get("total_gb",0), disk.get("used_gb",0),
                    disk.get("avail_gb",0), disk.get("used_pct",0),
                    disk.get("flag","OK"),
                ])

    # ── failed_services.csv ──
    with open(out_dir / "failed_services.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["hostname","unit","load","active","sub","description"])
        for d in node_dirs:
            n = load_node(d)
            hn = n["ident"].get("hostname","")
            for u in n["svc"].get("failed_units", []):
                w.writerow([hn, u.get("unit",""), u.get("load",""),
                             u.get("active",""), u.get("sub",""), u.get("description","")])

    # ── network_interfaces.csv ──
    with open(out_dir / "network_interfaces.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["hostname","interface","ip_cidr","scope"])
        for d in node_dirs:
            n = load_node(d)
            hn = n["ident"].get("hostname","")
            for iface in n["net"].get("interfaces", []):
                w.writerow([hn, iface.get("interface",""),
                             iface.get("ip_cidr",""), iface.get("scope","")])

    # ── infiniband.csv ──
    with open(out_dir / "infiniband.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["hostname","port","state","rate","firmware","error_count","error_detail","flag"])
        for d in node_dirs:
            n = load_node(d)
            hn = n["ident"].get("hostname","")
            for p in n["net"].get("infiniband", []):
                w.writerow([hn, p.get("ca",""), p.get("state",""), p.get("rate",""),
                             p.get("firmware",""), p.get("error_count",0),
                             p.get("error_detail",""), p.get("flag","OK")])

    # ── lustre_osts.csv ──
    with open(out_dir / "lustre_osts.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["hostname","uuid","total_tb","used_tb","avail_tb","used_pct","flag"])
        for d in node_dirs:
            n = load_node(d)
            hn = n["ident"].get("hostname","")
            for o in n["lus"].get("osts", []):
                w.writerow([hn, o.get("uuid",""), o.get("total_tb",0),
                             o.get("used_tb",0), o.get("avail_tb",0),
                             o.get("used_pct",0), o.get("flag","OK")])

    # ── sysctl_drift.csv ──
    with open(out_dir / "sysctl_drift.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["hostname","param","actual","recommended"])
        for d in node_dirs:
            n = load_node(d)
            hn = n["ident"].get("hostname","")
            for drift in n["sysctl"].get("drift_flags", []):
                w.writerow([hn, drift.get("param",""),
                             drift.get("actual",""), drift.get("recommended","")])

    # ── installed_rpms.csv ──
    with open(out_dir / "installed_rpms.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["hostname","name","version","release","arch","install_date"])
        for d in node_dirs:
            n = load_node(d)
            hn = n["ident"].get("hostname","")
            for p in n["rpms"].get("packages", []):
                w.writerow([hn, p.get("name",""), p.get("version",""),
                             p.get("release",""), p.get("arch",""),
                             p.get("install_date","")])
