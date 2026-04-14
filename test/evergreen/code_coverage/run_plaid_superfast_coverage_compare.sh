#!/usr/bin/env bash
# Run plaid, superfast, and fast with clean .gcda between modes; gcovr JSON summaries;
# write CSV plus a human-readable comparison report. Intended for nohup on a dev host.
#
# Do not run two compares against the same BUILD at once; mode_coverage_compare.py uses
# a flock on the build directory so the second process blocks until the first finishes.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
BUILD="${WT_BUILDDIR:-$ROOT/cmake_build_cov_palite}"
STAMP="$(date +%Y%m%d_%H%M%S)"
MODES="${COVER_COMPARE_MODES:-plaid,superfast,fast}"
OUT="${COVER_COMPARE_OUT:-$ROOT/cov_compare_suite_modes_${STAMP}}"
JOBS="${COVER_COMPARE_JOBS:-12}"
GCOV="${GCOV_EXECUTABLE:-/opt/mongodbtoolchain/v5/bin/gcov}"

mkdir -p "$OUT"
echo "$OUT" >"$ROOT/cov_compare_suite_modes_BG_OUT.txt"
# Back-compat pointer (same directory as primary BG_OUT).
echo "$OUT" >"$ROOT/cov_compare_plaid_superfast_BG_OUT.txt"
exec >>"$OUT/run.log" 2>&1
echo "=== suite mode coverage compare (${MODES}) ==="
echo "ROOT=$ROOT"
echo "BUILD=$BUILD"
echo "OUT=$OUT"
echo "MODES=$MODES"
echo "JOBS=$JOBS"
echo "GCOV=$GCOV"
echo "START=$(date -Iseconds)"

if [[ ! -f "$BUILD/wt" ]]; then
  echo "ERROR: no wt in BUILD=$BUILD (set WT_BUILDDIR or cmake_build_cov_palite)" >&2
  exit 2
fi

python3 "$ROOT/test/evergreen/code_coverage/mode_coverage_compare.py" \
  --wt-src "$ROOT" \
  --build-dir "$BUILD" \
  --output-dir "$OUT" \
  --modes "$MODES" \
  --j "$JOBS" \
  --gcov-executable "$GCOV" \
  "$@"

python3 - <<PY
import json
from pathlib import Path

out = Path("$OUT")
rep = out / "comparison_report.txt"

def load(mode: str):
    p = out / mode / "gcovr-summary.json"
    if not p.is_file():
        return None
    return json.loads(p.read_text(encoding="utf-8"))

def row(name: str, o: dict | None) -> str:
    if not o:
        return f"{name}: (missing summary)"
    lp = o.get("line_percent")
    c = o.get("line_covered")
    t = o.get("line_total")
    return f"{name}: line_percent={lp}  covered={c}  total={t}"

def fmap(o: dict | None) -> dict[str, tuple[int, int]]:
    if not o:
        return {}
    m = {}
    for f in o.get("files") or []:
        fn = f.get("filename")
        if not fn:
            continue
        m[fn] = (int(f.get("line_covered") or 0), int(f.get("line_total") or 0))
    return m

def top_gains(pm: dict, sm: dict, label: str, top: int = 25) -> list[str]:
    gains = []
    for fn, (sc2, st2) in sm.items():
        if st2 <= 0:
            continue
        pl_c, _ = pm.get(fn, (0, 0))
        g = sc2 - pl_c
        if g > 0:
            gains.append((g, sc2, pl_c, st2, fn))
    gains.sort(reverse=True)
    lines = [
        f"Files with strictly more line coverage in {label}: {len(gains)}",
        "",
        f"Top {top} files by additional lines covered:",
    ]
    for g, sc2, pl_c, st2, fn in gains[:top]:
        lines.append(f"  +{g:5d} lines  ({pl_c:5d} -> {sc2:5d} / {st2:5d})  {fn}")
    return lines

pl = load("plaid")
sf = load("superfast")
fa = load("fast")
lines = []
lines.append("Plaid / superfast / fast — gcovr line coverage (filtered to src/ as in driver)")
lines.append("")
lines.append(row("plaid", pl))
lines.append(row("superfast", sf))
lines.append(row("fast", fa))
lines.append("")

def cov(o):
    if not o:
        return 0, 0
    return int(o.get("line_covered") or 0), int(o.get("line_total") or 0)

pc, pt = cov(pl)
sc, st = cov(sf)
fc, ft = cov(fa)

if pl and sf and pt == st == ft and pt:
    lines.append("Deltas (covered lines); same instrumented line total (%d):" % pt)
    lines.append(f"  superfast − plaid:  {sc - pc:+d}  ({sc/pt*100:.2f}% − {pc/pt*100:.2f}% = {(sc-pc)/pt*100:+.2f} pp)")
    lines.append(f"  fast − plaid:       {fc - pc:+d}  ({fc/pt*100:.2f}% − {pc/pt*100:.2f}% = {(fc-pc)/pt*100:+.2f} pp)")
    lines.append(f"  fast − superfast:   {fc - sc:+d}  ({fc/pt*100:.2f}% − {sc/pt*100:.2f}% = {(fc-sc)/pt*100:+.2f} pp)")
elif pl and sf:
    lines.append(f"Deltas: superfast−plaid covered={sc-pc:+d}; fast−plaid covered={fc-pc:+d} (totals plaid pt={pt} superfast st={st} fast ft={ft})")
lines.append("")

if pl and sf:
    lines.extend(top_gains(fmap(pl), fmap(sf), "superfast vs plaid"))
    lines.append("")
if pl and fa:
    lines.extend(top_gains(fmap(pl), fmap(fa), "fast vs plaid"))
    lines.append("")
if sf and fa:
    lines.extend(top_gains(fmap(sf), fmap(fa), "fast vs superfast"))

text = "\n".join(lines) + "\n"
rep.write_text(text, encoding="utf-8")
print(text)
PY

echo "END=$(date -Iseconds)"
echo "Wrote: $OUT/mode_coverage_comparison.csv"
echo "Wrote: $OUT/comparison_report.txt"
echo "Log:   $OUT/run.log"
ln -sfn "$OUT" "$ROOT/cov_compare_suite_modes_LATEST"
ln -sfn "$OUT" "$ROOT/cov_compare_plaid_superfast_LATEST"
echo "Symlinks: $ROOT/cov_compare_suite_modes_LATEST -> $OUT"
echo "          $ROOT/cov_compare_plaid_superfast_LATEST -> $OUT (compat)"
