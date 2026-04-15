#!/usr/bin/env python3
"""
BFG debug pipeline: orchestrate all three steps end-to-end.

Steps (run in order; use --skip-* flags to bypass completed ones):
  1. generate  — fetch BFG list from BuildBaron       → bfgs.csv
  2. fetch     — pull CONFIG files from Evergreen     → bfg-configs/
  3. run       — build WT and run format tests        → bfg-debug-logs-fix/

Usage:
    # Full pipeline (BuildBaron auth prompt will open in browser):
    python bfg_pipeline.py develop --bf-key WT-17002

    # With a debug patch:
    python bfg_pipeline.py develop --bf-key WT-17002 --patch fix.patch

    # bfgs.csv already exists, skip the BuildBaron step:
    python bfg_pipeline.py develop --skip-generate

    # CONFIGs already downloaded too, just run the tests:
    python bfg_pipeline.py develop --skip-generate --skip-fetch

    # Only generate the CSV and download configs, don't run tests:
    python bfg_pipeline.py develop --bf-key WT-17002 --skip-run
"""

import argparse
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def banner(title: str) -> None:
    line = "=" * 60
    print(f"\n{line}")
    print(f"  {title}")
    print(f"{line}\n")


def elapsed(start: float) -> str:
    secs = int(time.time() - start)
    m, s = divmod(secs, 60)
    return f"{m}m{s:02d}s" if m else f"{s}s"


# ---------------------------------------------------------------------------
# Step 1: generate bfgs.csv
# ---------------------------------------------------------------------------

def step_generate(bf_key: str) -> None:
    """Import generate_bfgs_cvs and run it with the given BF key."""
    banner(f"Step 1 — generate bfgs.csv  (BF key: {bf_key})")

    if "generate_bfgs_cvs" in sys.modules:
        del sys.modules["generate_bfgs_cvs"]

    # Temporarily inject --bf-key so generate_bfgs_cvs's argparse picks it up.
    _orig_argv = sys.argv
    sys.argv = [sys.argv[0], "--bf-key", bf_key]
    try:
        import generate_bfgs_cvs as gen
        gen.main()
    finally:
        sys.argv = _orig_argv


# ---------------------------------------------------------------------------
# Step 2: fetch CONFIG files
# ---------------------------------------------------------------------------

def step_fetch() -> None:
    """Import fetch_bfg_configs and run it."""
    banner("Step 2 — fetch CONFIG files from Evergreen")

    if "fetch_bfg_configs" in sys.modules:
        del sys.modules["fetch_bfg_configs"]

    import fetch_bfg_configs as fc
    fc.main()


# ---------------------------------------------------------------------------
# Step 3: run BFG format tests
# ---------------------------------------------------------------------------

def step_run(branch: str, patch: Path | None) -> None:
    """Run run_bfg_debug.py as a subprocess."""
    banner(f"Step 3 — run BFG format tests  (branch: {branch})")

    cmd = [sys.executable, str(ROOT / "run_bfg_debug.py"), branch]
    if patch:
        cmd += ["--patch", str(patch)]

    result = subprocess.run(cmd)
    if result.returncode != 0:
        sys.exit(f"run_bfg_debug.py exited with code {result.returncode}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="End-to-end BFG debug pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "branch",
        nargs="?",
        help="Git branch to build and test against (required unless --skip-run)",
    )
    parser.add_argument(
        "--bf-key",
        default=None,
        help="Jira/BuildBaron BF key (e.g. WT-17002); required unless --skip-generate",
    )
    parser.add_argument(
        "--patch",
        type=Path,
        default=None,
        help="Optional patch file to apply on top of the checkout before building",
    )
    parser.add_argument(
        "--skip-generate",
        action="store_true",
        help="Skip Step 1 (assume bfgs.csv already exists)",
    )
    parser.add_argument(
        "--skip-fetch",
        action="store_true",
        help="Skip Step 2 (assume bfg-configs/ already populated)",
    )
    parser.add_argument(
        "--skip-run",
        action="store_true",
        help="Skip Step 3 (only generate CSV / fetch configs)",
    )
    args = parser.parse_args()

    # Validate required args for the steps that will actually run.
    if not args.skip_generate and not args.bf_key:
        parser.error("--bf-key is required unless --skip-generate is set")
    if not args.skip_run and not args.branch:
        parser.error("branch is required unless --skip-run is set")

    total_start = time.time()

    # Step 1
    if args.skip_generate:
        csv_path = ROOT / "bfgs.csv"
        if not csv_path.exists():
            sys.exit(f"ERROR: bfgs.csv not found at {csv_path}. Remove --skip-generate to generate it.")
        print(f"[skip] Step 1 — using existing {csv_path}")
    else:
        t = time.time()
        step_generate(args.bf_key)
        print(f"Step 1 done in {elapsed(t)}")

    # Step 2
    if args.skip_fetch:
        print("[skip] Step 2 — skipping config fetch")
    else:
        t = time.time()
        step_fetch()
        print(f"Step 2 done in {elapsed(t)}")

    # Step 3
    if args.skip_run:
        print("[skip] Step 3 — skipping test run")
    else:
        t = time.time()
        step_run(args.branch, args.patch)
        print(f"Step 3 done in {elapsed(t)}")

    print(f"\nPipeline complete in {elapsed(total_start)}.")
    print(f"Logs: {ROOT / 'bfg-debug-logs-fix'}")


if __name__ == "__main__":
    main()
