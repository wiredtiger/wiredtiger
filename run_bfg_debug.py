#!/usr/bin/env python3
"""
Run a set of BFG (bug) format tests in parallel against a given branch.

Usage:
    python run_bfg_debug.py <branch>
    python run_bfg_debug.py <branch> --patch fix.patch

The script:
  1. Checks out <branch> once and builds it (reuses an existing build if present).
  2. Optionally applies a debug patch on top.
  3. For each BFG in bfgs.csv, copies the whole built tree, drops in the BFG's
     CONFIG file, runs ./t, and writes a log to bfg-debug-logs-fix/.
  4. Up to MAX_WORKERS BFGs run in parallel.

A bfg_whitelist.txt file can be used to restrict which BFGs are run.
"""

import argparse
import csv
import os
import shutil
import subprocess
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths and settings — adjust these if your layout differs
# ---------------------------------------------------------------------------

ROOT         = Path(__file__).resolve().parent
BFG_CSV      = ROOT / "bfgs.csv"           # columns: bfg_id, task_id, commit
CONFIG_DIR   = ROOT / "bfg-configs"        # one CONFIG.<bfg_id> file per BFG
LOG_DIR      = ROOT / "bfg-debug-logs-fix" # one <bfg_id>.log written per run
WHITELIST    = ROOT / "bfg_whitelist.txt"  # optional: only run listed BFG ids

BUILD_CMD    = ["ninja"]
T_REL        = Path("test") / "format"     # path to ./t inside the cmake build dir
MAX_WORKERS  = 4                           # parallel BFGs; tune to your core count

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Lock so that log lines from different threads don't interleave on stdout.
_print_lock = threading.Lock()

def log(bfg_id, msg):
    with _print_lock:
        print(f"[{bfg_id}] {msg}")

def run(cmd, cwd, env=None):
    """Run a shell command, capturing stdout+stderr. Returns the CompletedProcess."""
    return subprocess.run(
        cmd,
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

def load_whitelist():
    """
    Read bfg_whitelist.txt and return the set of BFG ids to run.
    Returns None if the file doesn't exist, meaning run everything.
    Lines ending in '.log' have that suffix stripped automatically.
    """
    if not WHITELIST.exists():
        return None

    ids = set()
    for line in WHITELIST.read_text().splitlines():
        s = line.strip()
        if not s:
            continue
        if s.endswith(".log"):
            s = s[:-4]
        ids.add(s)

    print(f"Loaded whitelist with {len(ids)} BFGs from {WHITELIST}")
    return ids

# ---------------------------------------------------------------------------
# Build step (runs once, result is reused by all BFGs)
# ---------------------------------------------------------------------------

def ensure_shared_build(branch, patch=None):
    """
    Check out <branch> as a git worktree and build it with cmake + ninja.
    The result lives at  ../wt-bfg-build-<branch>/  and is never deleted,
    so subsequent runs skip straight to the copy step.

    If --patch was given, it is applied on top of the checkout before building.

    Returns the path to the root of the built worktree.
    """
    shared_dir = ROOT.parent / f"wt-bfg-build-{branch}"
    build_dir  = shared_dir / "cmake_build"
    tf_dir     = build_dir / T_REL

    # If the test binary is already there, nothing to do.
    if (tf_dir / "t").exists():
        print(f"Reusing existing build at {shared_dir}")
        return shared_dir

    print(f"Building {branch} in {shared_dir}")

    # Create the worktree if it hasn't been set up yet.
    # git worktree add writes a .git file (not a directory) into the new tree.
    if not (shared_dir / ".git").exists():
        r = run(["git", "worktree", "add", "--detach", str(shared_dir), branch], cwd=ROOT)
        if r.returncode != 0:
            raise RuntimeError(f"git worktree add failed:\n{r.stdout}")

    # Optionally layer a debug patch on top of the checkout.
    if patch is not None:
        print(f"Applying patch {patch}")
        r = run(["git", "apply", str(patch)], cwd=shared_dir)
        if r.returncode != 0:
            raise RuntimeError(f"git apply failed:\n{r.stdout}")

    # Configure with cmake (skip if already done from a previous partial run).
    build_dir.mkdir(exist_ok=True)
    if not (build_dir / "build.ninja").exists():
        print("Running cmake configure")
        r = run(
            [
                "cmake",
                "-DCMAKE_TOOLCHAIN_FILE=../cmake/toolchains/mongodbtoolchain_stable_gcc.cmake",
                "-G", "Ninja",
                "../.",
            ],
            cwd=build_dir,
        )
        if r.returncode != 0:
            raise RuntimeError(f"cmake configure failed:\n{r.stdout}")

    # Build.
    print("Building")
    r = run(BUILD_CMD, cwd=build_dir)
    if r.returncode != 0:
        raise RuntimeError(f"build failed:\n{r.stdout}")

    return shared_dir

# ---------------------------------------------------------------------------
# Per-BFG worker (runs in a thread)
# ---------------------------------------------------------------------------

def process_bfg(row, shared_dir):
    """
    Run a single BFG:
      1. Copy the whole built WT tree to a fresh temp directory so that
         ./t's RUNDIR and any output files don't conflict with other BFGs.
      2. Drop the BFG's CONFIG file into the test/format directory.
      3. Run ./t and write a log file.
      4. Clean up the temp directory.
    """
    bfg_id = row["bfg_id"]
    sha    = row["commit"].strip()

    log(bfg_id, f"starting @ {sha}")

    # Make sure we have a CONFIG file for this BFG.
    cfg_src = CONFIG_DIR / f"CONFIG.{bfg_id}"
    if not cfg_src.exists():
        log(bfg_id, f"missing CONFIG file {cfg_src}, skipping")
        return

    # Skip BFGs that have already been run (log file exists).
    log_path = LOG_DIR / f"{bfg_id}.log"
    if log_path.exists():
        log(bfg_id, "skipping, already run previously")
        return

    # Create a private copy of the whole WT directory for this BFG.
    # This gives ./t its own sandbox so parallel runs don't step on each other.
    work_dir = Path(tempfile.mkdtemp(prefix=f"wt-{bfg_id}-", dir=str(ROOT.parent)))
    try:
        wt_dir = work_dir / "wt"
        shutil.copytree(shared_dir, wt_dir)

        tf_dir = wt_dir / "cmake_build" / T_REL

        # Place the BFG's CONFIG file where ./t expects it.
        cfg_dest = tf_dir / f"CONFIG.{bfg_id}"
        shutil.copyfile(cfg_src, cfg_dest)

        # Run the format test.
        log(bfg_id, "running ./t")
        r = run(["./t", "-c", cfg_dest.name], cwd=tf_dir, env=os.environ.copy())

        # Write the log.
        log_path.write_text(
            f"BFG: {bfg_id}\nCOMMIT: {sha}\nCONFIG: {cfg_dest}\n"
            f"EXIT_CODE: {r.returncode}\n"
            + "=" * 80 + "\n"
            + r.stdout
        )
        log(bfg_id, f"done (exit {r.returncode})")

        # Archive RUNDIR (pagedumps etc) for this BFG if needed.
        # RUNDIR_ARCHIVE_DIR.mkdir(exist_ok=True)
        # src_rundir = tf_dir / "RUNDIR"
        # if src_rundir.exists():
        #     dest_dir = RUNDIR_ARCHIVE_DIR / bfg_id
        #     if dest_dir.exists():
        #         shutil.rmtree(dest_dir)
        #     shutil.copytree(src_rundir, dest_dir)
        #     log(bfg_id, f"archived RUNDIR to {dest_dir}")

    finally:
        # Always remove the temp directory, even if the test crashed.
        shutil.rmtree(work_dir, ignore_errors=True)

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Run BFG format tests against a branch")
    parser.add_argument("branch", help="git branch to build and test against")
    parser.add_argument("--patch", type=Path, default=None,
                        help="optional patch file to apply on top of the checkout")
    args = parser.parse_args()

    LOG_DIR.mkdir(exist_ok=True)
    whitelist = load_whitelist()

    # Build once; every BFG worker gets a copy of this.
    shared_dir = ensure_shared_build(args.branch, args.patch)

    # Load the BFG list, filtering by whitelist if one is present.
    with open(BFG_CSV) as f:
        rows = [
            row for row in csv.DictReader(f)
            if whitelist is None or row["bfg_id"] in whitelist
        ]

    print(f"Running {len(rows)} BFGs with {MAX_WORKERS} workers")

    # Run BFGs in parallel. as_completed lets us catch and report per-BFG
    # exceptions without killing the whole run.
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_bfg, row, shared_dir): row["bfg_id"] for row in rows}
        for future in as_completed(futures):
            bfg_id = futures[future]
            try:
                future.result()
            except Exception as e:
                log(bfg_id, f"unhandled exception: {e}")

if __name__ == "__main__":
    main()
