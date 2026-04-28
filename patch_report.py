#!/usr/bin/env python3
"""
patch_report.py — Add color-coded cells and critical row pulse animation
to an existing cluster_report_ai.html without re-running the LLM pipeline.

Usage:
    python3 patch_report.py <path-to-cluster_report_ai.html>
"""

import sys
import re
from pathlib import Path

if len(sys.argv) < 2:
    print(f"Usage: {sys.argv[0]} <cluster_report_ai.html>")
    sys.exit(1)

path = Path(sys.argv[1]).expanduser().resolve()
html = path.read_text()

# ── CSS to inject ──────────────────────────────────────────────────────────────
css = """
/* ── Color-coded table cells ── */
td.val-critical { color: #f87171; font-weight: 600; }
td.val-warning  { color: #fbbf24; font-weight: 600; }
td.val-ok       { color: #34d399; }
td.val-muted    { color: #64748b; }

/* ── Critical row pulse animation ── */
@keyframes criticalPulse {
  0%   { box-shadow: inset 0 0 0 0px rgba(248,113,113,0); }
  50%  { box-shadow: inset 0 0 0 2px rgba(248,113,113,0.6); }
  100% { box-shadow: inset 0 0 0 0px rgba(248,113,113,0); }
}
tr[data-status="CRITICAL"] {
  animation: criticalPulse 3s ease-in-out infinite;
}
tr[data-status="CRITICAL"]:hover {
  animation: none;
  background-color: rgba(248,113,113,0.08);
}

/* ── Node card critical pulse ── */
@keyframes cardPulse {
  0%   { border-color: #253048; }
  50%  { border-color: rgba(248,113,113,0.7); }
  100% { border-color: #253048; }
}
.node-card[data-status="CRITICAL"] {
  animation: cardPulse 4s ease-in-out infinite;
}
"""

# ── JS to inject — walks table and color-codes cells by value ─────────────────
js = """
<script>
(function colorCodeTable() {
  // Column indices (0-based): hostname=0 status=1 uptime=2 load=3
  // mem=4 cpuidle=5 logcrit=6 logwarn=7 ib=8 sfa=9
  const LOAD_WARN     = 20;
  const LOAD_CRIT     = 40;
  const MEM_WARN      = 70;
  const MEM_CRIT      = 85;
  const LOGCRIT_WARN  = 5;
  const LOGCRIT_CRIT  = 10;
  const LOGWARN_WARN  = 50;
  const LOGWARN_CRIT  = 200;
  const CPUIDLE_WARN  = 10;   // below this = warning
  const CPUIDLE_CRIT  = 2;    // below this = critical

  function classify(val, warnThresh, critThresh, lowerIsBad) {
    const n = parseFloat(val);
    if (isNaN(n)) return null;
    if (lowerIsBad) {
      if (n <= critThresh) return 'val-critical';
      if (n <= warnThresh) return 'val-warning';
      return 'val-ok';
    } else {
      if (n >= critThresh) return 'val-critical';
      if (n >= warnThresh) return 'val-warning';
      return 'val-ok';
    }
  }

  function classifyStatus(val) {
    if (val === 'CRITICAL') return 'val-critical';
    if (val === 'WARNING')  return 'val-warning';
    if (val === 'OK')       return 'val-ok';
    return null;
  }

  document.querySelectorAll('tbody tr[data-status]').forEach(row => {
    const cells = row.querySelectorAll('td');
    if (cells.length < 10) return;

    // Load (col 3)
    const loadCls = classify(cells[3].textContent, LOAD_WARN, LOAD_CRIT, false);
    if (loadCls) cells[3].classList.add(loadCls);

    // Mem% (col 4)
    const memCls = classify(cells[4].textContent, MEM_WARN, MEM_CRIT, false);
    if (memCls) cells[4].classList.add(memCls);

    // CPU Idle% (col 5) — lower is bad
    const idleCls = classify(cells[5].textContent, CPUIDLE_WARN, CPUIDLE_CRIT, true);
    if (idleCls) cells[5].classList.add(idleCls);

    // Log Crit (col 6)
    const lcCls = classify(cells[6].textContent, LOGCRIT_WARN, LOGCRIT_CRIT, false);
    if (lcCls) cells[6].classList.add(lcCls);
    if (parseInt(cells[6].textContent) === 0) cells[6].classList.add('val-muted');

    // Log Warn (col 7)
    const lwCls = classify(cells[7].textContent, LOGWARN_WARN, LOGWARN_CRIT, false);
    if (lwCls) cells[7].classList.add(lwCls);
    if (parseInt(cells[7].textContent) === 0) cells[7].classList.add('val-muted');

    // IB (col 8)
    const ibCls = classifyStatus(cells[8].textContent.trim());
    if (ibCls) cells[8].classList.add(ibCls);

    // SFA (col 9)
    const sfaCls = classifyStatus(cells[9].textContent.trim());
    if (sfaCls) cells[9].classList.add(sfaCls);
  });
})();
</script>
"""

# ── Inject CSS before </style> ────────────────────────────────────────────────
if "/* color-coded table cells */" not in html.lower() and "val-critical" not in html:
    html = html.replace("</style>", css + "\n</style>", 1)
    print("[*] CSS injected")
else:
    print("[*] CSS already present — skipping")

# ── Inject JS before </body> ──────────────────────────────────────────────────
if "colorCodeTable" not in html:
    html = html.replace("</body>", js + "\n</body>", 1)
    print("[*] JS injected")
else:
    print("[*] JS already present — skipping")

# ── Write ─────────────────────────────────────────────────────────────────────
path.write_text(html)
print(f"[*] Patched: {path}")
print(f"[*] Reload in browser to see changes")
