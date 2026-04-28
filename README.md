# sos-analyzer

SOS report analysis toolkit for DDN Lustre HPC storage clusters.

Processes a directory of SOS reports from a single cluster and produces:
- Per-node JSON + text summaries
- Cluster-wide diff (`cluster_diff.json`)
- Interactive HTML dashboard with anomaly detection
- CSV exports for all key metrics

## Installation

```bash
git clone https://github.com/kylewilber/sos-analyzer.git
cd sos-analyzer
pip install -e .
```

## Usage

```bash
# Process a directory of SOS tarballs or extracted dirs
sos-analyzer --input ~/sos-reports/customer-cluster/ --output ~/reports/test99/

# Skip LLM narrative (faster, no Ollama required)
sos-analyzer --input ~/sos-reports/ --output ~/reports/test99/ --no-llm

# Skip HTML report generation (just parse + CSV)
sos-analyzer --input ~/sos-reports/ --output ~/reports/test99/ --no-report

# Control parallelism
sos-analyzer --input ~/sos-reports/ --output ~/reports/test99/ --jobs 4
```

Or via Python module:

```bash
python3 -m sos_analyzer --input ~/sos-reports/ --output ~/reports/test99/
```

## Input formats

- Directories of SOS tarballs: `sosreport-hostname-*.tar.xz`
- Pre-extracted SOS directories: `sosreport-hostname-*/`
- Mixed directories (both handled automatically)

## Output structure

```
<output>/
  nodes/
    <hostname>/
      identity.json / .txt
      resources.json / .txt
      network.json / .txt
      services.json / .txt
      logs.json / .txt
      lustre.json / .txt
      sfa.json / .txt
      sysctl.json / .txt
      rpms.json / .txt
      node_summary.txt
  cluster/
    cluster_diff.json        ← per-node metrics, all nodes
    cluster_summary.txt      ← text table
    cluster_report_ai.html   ← interactive HTML dashboard
    exports/
      cluster_overview.csv
      disk_usage.csv
      infiniband.csv
      lustre_osts.csv
      sysctl_drift.csv
      failed_services.csv
      installed_rpms.csv
      network_interfaces.csv
```

## HTML Dashboard

The HTML dashboard is generated entirely in Python (no LLM required for the
structure). Features:

- Visual summary: node status donut, memory/load/OST gauges, load heatmap
- Anomaly & correlation panel: 11 detection groups including LNet cross-node
  IP correlation, log outliers, sysctl drift, IB port errors, uptime splits
- Sortable/filterable cluster overview table
- Per-node cards with collapsible sections: disk, IB, logs, client events,
  SFA, sysctl tuning drift
- Optional LLM narrative summary (requires Ollama with qwen3-coder:30b)

## LLM Configuration

The optional narrative summary uses a local Ollama instance. Configure in
`generate_report.py`:

```python
OLLAMA_URL = "http://your-ollama-host:11434/api/generate"
MODEL      = "qwen3-coder:30b"
```

## Sysctl tuning recommendations

The sysctl parser checks the following parameters against recommended values
for Lustre OSS/MDS nodes:

| Parameter | Recommended |
|-----------|-------------|
| vm.swappiness | 10 |
| vm.zone_reclaim_mode | 0 |
| kernel.numa_balancing | 0 |
| net.ipv4.tcp_timestamps | 0 |
| net.ipv4.tcp_low_latency | 1 |
| net.ipv4.tcp_slow_start_after_idle | 0 |
| net.ipv4.conf.*.accept_redirects | 0 |
| net.ipv4.conf.*.secure_redirects | 0 |
| kernel.hung_task_warnings | 10 |

## Requirements

- Python 3.9+
- No external dependencies (stdlib only)
- Optional: Ollama with qwen3-coder:30b for narrative summaries

## Supported SOS versions

Tested with `sosreport` 4.x on RHEL 8/9, Rocky Linux 8/9.
