#!/usr/bin/env python3
"""
patch_layout.py — Fix visual summary header layout and chevron rotation.

Usage:
    python3 patch_layout.py <report_dir>

Example:
    python3 patch_layout.py ~/work/sos-analyzer/reports/test99
"""

import sys
import re
from pathlib import Path

if len(sys.argv) < 2:
    print(f"Usage: {sys.argv[0]} <report_dir>")
    sys.exit(1)

report_dir = Path(sys.argv[1]).expanduser().resolve()
html_path  = report_dir / "cluster" / "cluster_report_ai.html"

if not html_path.exists():
    print(f"[ERROR] Not found: {html_path}")
    sys.exit(1)

html = html_path.read_text()

# ── 1. Replace vsummary CSS with fixed layout ─────────────────────────────────
old_css_pattern = r'\.vsummary \{.*?\}.*?\.heatmap-tip \{.*?\}'
new_css = """
.vsummary {
  display: flex;
  flex-wrap: wrap;
  gap: 12px;
  padding: 16px 20px;
  background: #111827;
  border-bottom: 1px solid #253048;
  align-items: stretch;
}
.vsummary-panel {
  background: #1a2236;
  border: 1px solid #253048;
  border-radius: 8px;
  padding: 12px 16px;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: flex-start;
  min-width: 160px;
  flex: 1 1 160px;
  max-width: 220px;
  box-sizing: border-box;
  overflow: hidden;
}
.vsummary-panel.wide {
  flex: 1 1 280px;
  max-width: 340px;
}
.vsummary-panel h4 {
  margin: 0 0 8px 0;
  font-size: 10px;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  color: #64748b;
  font-weight: 600;
  white-space: nowrap;
  text-align: center;
}
.vsummary-panel svg {
  max-width: 100%;
  height: auto;
}
.vsummary-logcounts {
  display: flex;
  gap: 24px;
  align-items: center;
  justify-content: center;
  flex: 1;
}
.logcount-block {
  text-align: center;
}
.logcount-val {
  font-size: 36px;
  font-weight: 700;
  line-height: 1;
}
.logcount-val.critical { color: #f87171; }
.logcount-val.warning  { color: #fbbf24; }
.logcount-label {
  font-size: 11px;
  color: #64748b;
  margin-top: 4px;
}
.heatmap-tip {
  font-size: 10px;
  color: #475569;
  margin-top: 6px;
  text-align: center;
}"""

html = re.sub(
    r'\.vsummary \{.*?\.heatmap-tip \{[^}]+\}',
    new_css,
    html,
    flags=re.DOTALL,
)
print("[*] vsummary CSS replaced")

# ── 2. Add .wide class to heatmap panel ───────────────────────────────────────
html = html.replace(
    '<div class="vsummary-panel">\n    <h4>Load Heatmap',
    '<div class="vsummary-panel wide">\n    <h4>Load Heatmap',
)
print("[*] Heatmap panel marked as wide")

# ── 3. Fix chevron rotation ───────────────────────────────────────────────────
# Replace the injected toggleSection with a fixed version
old_toggle = re.search(
    r'<script>\s*function toggleSection\(el\).*?</script>',
    html,
    re.DOTALL,
)

new_toggle_js = """<script>
function toggleSection(el) {
  const content = el.nextElementSibling;
  const chevron = el.querySelector('.chevron');
  if (!content) return;
  const isOpen = content.classList.contains('open');
  // Toggle content visibility
  content.classList.toggle('open', !isOpen);
  content.style.display = isOpen ? 'none' : 'block';
  // Rotate chevron: ▶ becomes ▼ when open
  if (chevron) {
    chevron.style.display = 'inline-block';
    chevron.style.transition = 'transform 0.2s ease';
    chevron.style.transform = isOpen ? 'rotate(0deg)' : 'rotate(90deg)';
  }
}
</script>"""

if old_toggle:
    html = html[:old_toggle.start()] + new_toggle_js + html[old_toggle.end():]
    print("[*] toggleSection replaced with fixed version")
else:
    # No existing toggleSection — inject before </body>
    html = html.replace("</body>", new_toggle_js + "\n</body>", 1)
    print("[*] toggleSection injected")

# ── 4. Also fix any chevron CSS that might conflict ───────────────────────────
# Ensure the CSS chevron.open rule uses transform
chevron_css_fix = """
<style>
/* chevron rotation fix */
.chevron {
  display: inline-block;
  transition: transform 0.2s ease;
  margin-left: auto;
}
</style>
"""
if "chevron rotation fix" not in html:
    html = html.replace("</head>", chevron_css_fix + "\n</head>", 1)
    if "</head>" not in html:
        html = html.replace("<body>", chevron_css_fix + "\n<body>", 1)
    print("[*] Chevron CSS fix injected")

# ── 5. Write ──────────────────────────────────────────────────────────────────
html_path.write_text(html)
print(f"[*] Patched: {html_path}")
print(f"[*] Reload browser to see changes")
