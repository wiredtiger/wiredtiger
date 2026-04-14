#!/usr/bin/env python3
#
# Compare gcov/gcovr coverage across suite modes.
#
# Runs each mode, collects gcovr JSON summary, and writes a single comparison CSV.
#
# Notes:
# - For parallel (-j) suite runs, set WT_GCOV_PREFIX_BASE so each worker writes .gcda to a per-pid
#   directory, avoiding profile corruption.
#

from __future__ import annotations

import argparse
import csv
import fcntl
import json
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ModeResult:
    mode: str
    suite_rc: int
    suite_seconds: float | None
    summary_path: Path
    line_rate: float | None
    lines_covered: int | None
    lines_total: int | None


def _run(cmd: list[str], *, cwd: Path, env: dict[str, str] | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=str(cwd), env=env, text=True, capture_output=True)


def _rm_tree(p: Path) -> None:
    if p.exists():
        shutil.rmtree(p)


def _parse_suite_seconds(stdout: str) -> float | None:
    # "Ran 13035 tests in 348.154s"
    for line in stdout.splitlines():
        line = line.strip()
        if line.startswith("Ran ") and " tests in " in line and line.endswith("s"):
            try:
                tail = line.split(" tests in ", 1)[1]
                return float(tail.rstrip("s"))
            except Exception:
                return None
    return None


def _read_gcovr_summary(path: Path) -> tuple[float | None, int | None, int | None]:
    try:
        o = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None, None, None
    # gcovr json summary uses "line_percent" in some versions and "line_rate" in others.
    s = o.get("line_percent")
    if s is None:
        s = o.get("line_rate")
        if isinstance(s, (int, float)):
            s = float(s) * 100.0
    try:
        percent = float(s) if s is not None else None
    except Exception:
        percent = None
    # gcovr --json-summary uses line_covered / line_total (and line_percent).
    lc = o.get("lines_covered")
    if lc is None:
        lc = o.get("line_covered")
    lt = o.get("lines_total")
    if lt is None:
        lt = o.get("line_total")
    return (
        percent,
        int(lc) if lc is not None else None,
        int(lt) if lt is not None else None,
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="Compare coverage across suite modes (gcovr json summary).")
    ap.add_argument("--wt-src", type=Path, required=True, help="WiredTiger source root")
    ap.add_argument("--build-dir", type=Path, required=True, help="Coverage build directory (CMAKE_BUILD_TYPE=Coverage)")
    ap.add_argument("--output-dir", type=Path, required=True, help="Where to put per-mode outputs")
    ap.add_argument("--j", type=int, default=24, help="Parallelism for run.py")
    ap.add_argument(
        "--modes",
        default="plaid,superfast,fast,full,long",
        help="Comma-separated modes: plaid,superfast,fast,full,long",
    )
    # Prefer module invocation so we don't rely on PATH containing ~/.local/bin.
    ap.add_argument("--gcovr", default="python3 -m gcovr", help="gcovr executable (string, may include args)")
    # gcovr's --filter is a regex matched from the start of the path (re.match),
    # so use an absolute-path-friendly default.
    ap.add_argument("--filter", default=r"^.*/src/.*", help="gcovr --filter regex")
    ap.add_argument(
        "--gcov-executable",
        default="/opt/mongodbtoolchain/v5/bin/llvm-cov gcov",
        help="gcov executable for gcovr (string, may include args). For clang builds prefer 'llvm-cov gcov'.",
    )
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    wt_src = args.wt_src.resolve()
    build_dir = args.build_dir.resolve()
    out_dir = args.output_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    run_py = wt_src / "test" / "suite" / "run.py"
    if not run_py.is_file():
        print(f"Missing run.py at {run_py}", file=sys.stderr)
        return 2

    modes = [m.strip() for m in str(args.modes).split(",") if m.strip()]
    results: list[ModeResult] = []

    lock_path = build_dir / ".wt_coverage_compare.lock"
    lock_fp = open(lock_path, "a+", encoding="utf-8")

    def clear_gcda(tree: Path) -> None:
        # Avoid using external find; keep it portable and deterministic.
        # Use os.walk so we can tolerate concurrent directory deletions under WT_TEST.
        for root, dirs, files in os.walk(tree, topdown=True):
            for fn in files:
                if not fn.endswith(".gcda"):
                    continue
                p = Path(root) / fn
                try:
                    p.unlink()
                except OSError:
                    pass

    try:
        fcntl.flock(lock_fp.fileno(), fcntl.LOCK_EX)
    except OSError:
        lock_fp.close()
        raise

    try:
        for mode in modes:
            mode_dir = out_dir / mode
            _rm_tree(mode_dir)
            mode_dir.mkdir(parents=True, exist_ok=True)

            # Ensure coverage data does not carry over between modes.
            clear_gcda(build_dir)

            env = os.environ.copy()

            # Run suite mode.
            if mode == "full":
                cmd = [sys.executable, str(run_py), "--mode", "full", "-j", str(args.j), "-v", "0"]
            elif mode == "long":
                cmd = [sys.executable, str(run_py), "--long", "-j", str(args.j), "-v", "0"]
            else:
                cmd = [sys.executable, str(run_py), "--mode", mode, "-j", str(args.j), "-v", "0"]

            if args.verbose:
                print(f"[run] {mode}: {' '.join(cmd)}", flush=True)
            r = _run(cmd, cwd=build_dir, env=env)
            (mode_dir / "suite.stdout").write_text(r.stdout, encoding="utf-8")
            (mode_dir / "suite.stderr").write_text(r.stderr, encoding="utf-8")
            suite_seconds = _parse_suite_seconds(r.stdout + "\n" + r.stderr)

            # Run gcovr summary.
            summary = mode_dir / "gcovr-summary.json"
            gcovr_cmd = str(args.gcovr).split() + [
                "--root",
                str(wt_src),
                str(build_dir),
                "--gcov-ignore-parse-errors",
                "--gcov-ignore-errors=no_working_dir_found",
                "--gcov-executable",
                str(args.gcov_executable),
                "--json-summary",
                str(summary),
                "-j",
                str(os.cpu_count() or 8),
                "--filter",
                args.filter,
                "--exclude-directories",
                "test/3rdparty",
            ]
            if args.verbose:
                print(f"[gcovr] {mode}: {' '.join(gcovr_cmd)}", flush=True)
            gr = _run(gcovr_cmd, cwd=build_dir, env=env)
            (mode_dir / "gcovr.stdout").write_text(gr.stdout, encoding="utf-8")
            (mode_dir / "gcovr.stderr").write_text(gr.stderr, encoding="utf-8")
            if gr.returncode != 0:
                print(f"gcovr failed for mode {mode} (rc={gr.returncode})", file=sys.stderr)
            line_rate, cov, total = _read_gcovr_summary(summary) if summary.exists() else (None, None, None)
            results.append(
                ModeResult(
                    mode=mode,
                    suite_rc=r.returncode,
                    suite_seconds=suite_seconds,
                    summary_path=summary,
                    line_rate=line_rate,
                    lines_covered=cov,
                    lines_total=total,
                )
            )

        # Write comparison CSV.
        csv_path = out_dir / "mode_coverage_comparison.csv"
        with csv_path.open("w", newline="", encoding="utf-8") as fp:
            w = csv.DictWriter(
                fp,
                fieldnames=[
                    "mode",
                    "suite_rc",
                    "suite_seconds",
                    "line_percent",
                    "lines_covered",
                    "lines_total",
                    "summary_path",
                ],
            )
            w.writeheader()
            for rr in results:
                w.writerow(
                    {
                        "mode": rr.mode,
                        "suite_rc": rr.suite_rc,
                        "suite_seconds": rr.suite_seconds if rr.suite_seconds is not None else "",
                        "line_percent": rr.line_rate if rr.line_rate is not None else "",
                        "lines_covered": rr.lines_covered if rr.lines_covered is not None else "",
                        "lines_total": rr.lines_total if rr.lines_total is not None else "",
                        "summary_path": str(rr.summary_path),
                    }
                )

        print(f"Wrote comparison CSV -> {csv_path}")
        for rr in results:
            lp = f"{rr.line_rate:.2f}%" if rr.line_rate is not None else "?"
            secs = f"{rr.suite_seconds:.3f}s" if rr.suite_seconds is not None else "?"
            print(f"{rr.mode}\twall={secs}\tlines={lp}")
    finally:
        try:
            fcntl.flock(lock_fp.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass
        lock_fp.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
