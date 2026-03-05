#!/usr/bin/env bash
# parsers/parse_rpms.sh — installed packages
# Usage: parse_rpms.sh <sos_root> <output_dir>

source "$(dirname "$0")/../lib/common.sh"

SOS="$1"
OUT="$2"

[[ -z "$SOS" || -z "$OUT" ]] && { log_error "Usage: $0 <sos_root> <output_dir>"; exit 1; }
mkdir -p "$OUT"

rpm_file="$SOS/installed-rpms"
[[ ! -f "$rpm_file" ]] && { log_warn "No installed-rpms file found"; echo '{"total": 0, "packages": []}' > "$OUT/rpms.json"; exit 0; }

total=0
rpm_json_arr="["
first=1

# Format: "name-version-release.arch    Day Mon DD HH:MM:SS YYYY"
while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    # Split on multiple spaces to get pkg and date
    pkg=$(echo "$line"  | awk '{print $1}')
    idate=$(echo "$line" | awk '{$1=""; print $0}' | sed 's/^ *//')

    # Parse name, version, release, arch from NEVRA
    # name-version-release.arch
    if [[ "$pkg" =~ ^(.+)-([^-]+)-([^-]+)\.([^.]+)$ ]]; then
        name="${BASH_REMATCH[1]}"
        version="${BASH_REMATCH[2]}"
        release="${BASH_REMATCH[3]}"
        arch="${BASH_REMATCH[4]}"
    else
        name="$pkg"; version=""; release=""; arch=""
    fi

    [[ $first -eq 0 ]] && rpm_json_arr+=","
    rpm_json_arr+=$(printf '{"name":"%s","version":"%s","release":"%s","arch":"%s","install_date":"%s"}' \
        "$(json_escape "$name")" "$(json_escape "$version")" \
        "$(json_escape "$release")" "$(json_escape "$arch")" \
        "$(json_escape "$idate")")
    first=0
    (( total++ ))
done < "$rpm_file"
rpm_json_arr+="]"

# ─── Write JSON ───────────────────────────────────────────────────────────────
cat > "$OUT/rpms.json" <<EOF
{
  "total": $total,
  "packages": $rpm_json_arr
}
EOF

# ─── Write text summary (just stats + first/last install dates) ───────────────
newest=$(sort -k2 -M "$rpm_file" 2>/dev/null | tail -1)
oldest=$(sort -k2 -M "$rpm_file" 2>/dev/null | head -1)

cat > "$OUT/rpms.txt" <<EOF
=== INSTALLED PACKAGES ===
  Total RPMs:     $total
  Newest install: $newest
  Oldest install: $oldest
  (Full list in rpms.json and rpms.csv export)
EOF

log_ok "RPMs parsed: $total packages"
