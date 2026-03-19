#!/usr/bin/env python3
import csv
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent
BFG_CSV = ROOT / "bfgs.csv"          # bfg_id,task_id,commit
CONFIG_DIR = ROOT / "bfg-configs"
LOG_DIR = ROOT / "bfg-debug-logs-fix"
PATCH = ROOT / "fix.patch"  # your debug patch commit in patch form
WHITELIST_FILE = ROOT / "bfg_whitelist.txt"  # optional

BUILD_CMD = ["ninja"]                # adjust if needed
T_REL = Path("test") / "format"      # location of ./t relative to build dir
RUNDIR_ARCHIVE_DIR = ROOT / "bfg-rundirs"

def run(cmd, cwd, env=None):
    print(f"  $ {' '.join(cmd)}")
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
    If bfg_whitelist.txt exists, return a set of BFG ids to run.
    Otherwise return None (run all).
    """
    if not WHITELIST_FILE.exists():
        return None
    ids = set()
    for line in WHITELIST_FILE.read_text().splitlines():
        s = line.strip()
        if not s:
            continue
        # strip any trailing ".log" if present
        if s.endswith(".log"):
            s = s[:-4]
        ids.add(s)
    print(f"Loaded whitelist with {len(ids)} BFGs from {WHITELIST_FILE}")
    return ids

def main():
    LOG_DIR.mkdir(exist_ok=True)
    whitelist = load_whitelist()

    with open(BFG_CSV) as f:
        reader = csv.DictReader(f)
        for row in reader:
            bfg_id = row["bfg_id"]
            if whitelist is not None and bfg_id not in whitelist:
                continue
            sha = row["commit"].strip()

            print(f"\n=== {bfg_id} @ {sha} ===")

            cfg_src = CONFIG_DIR / f"CONFIG.{bfg_id}"
            if not cfg_src.exists():
                print(f"  missing CONFIG file {cfg_src}, skipping")
                continue

            worktree_dir = Path(
                tempfile.mkdtemp(prefix=f"wt-{bfg_id}-", dir=str(ROOT.parent))
            )

            log = LOG_DIR / f"{bfg_id}.log"
            if log.exists():
                print(f"  skipping {bfg_id} as it was already run previously")
                continue

            try:
                # 1. Worktree at this commit
                r = run(["git", "worktree", "add", "--detach", str(worktree_dir), "develop"], cwd=ROOT)
                if r.returncode != 0:
                    (LOG_DIR / f"{bfg_id}.log").write_text(
                        f"git worktree add failed for {sha}:\n{r.stdout}"
                    )
                    continue

                # 2. Apply debug patch
                r = run(["git", "apply", str(PATCH)], cwd=worktree_dir)
                if r.returncode != 0:
                    (LOG_DIR / f"{bfg_id}.log").write_text(
                        f"git apply failed at {sha}:\n{r.stdout}"
                    )
                    continue

                # 3. Configure + build
                build_dir = worktree_dir / "cmake_build"
                build_dir.mkdir(exist_ok=True)

                if not (build_dir / "build.ninja").exists():
                    r = run(
                        [
                            "cmake",
                            "-DCMAKE_TOOLCHAIN_FILE=../cmake/toolchains/mongodbtoolchain_stable_gcc.cmake",
                            "-G",
                            "Ninja",
                            "../.",
                        ],
                        cwd=build_dir,
                    )
                    if r.returncode != 0:
                        (LOG_DIR / f"{bfg_id}.log").write_text(
                            f"cmake configure failed at {sha}:\n{r.stdout}"
                        )
                        continue

                r = run(BUILD_CMD, cwd=build_dir)
                if r.returncode != 0:
                    (LOG_DIR / f"{bfg_id}.log").write_text(
                        f"build failed at {sha}:\n{r.stdout}"
                    )
                    continue

                # 4. Put this BFG's CONFIG next to ./t
                tf_dir = build_dir / T_REL
                tf_dir.mkdir(parents=True, exist_ok=True)
                cfg_dest = tf_dir / f"CONFIG.{bfg_id}"
                shutil.copyfile(cfg_src, cfg_dest)

                # 5. Run ./t with that CONFIG
                env = os.environ.copy()
                r = run(["./t", "-c", cfg_dest.name], cwd=tf_dir, env=env)

                out = []
                out.append(f"BFG: {bfg_id}\nCOMMIT: develop \nCONFIG: {cfg_dest}\n")
                out.append(f"EXIT_CODE: {r.returncode}\n")
                out.append("=" * 80 + "\n")
                out.append(r.stdout)

                (LOG_DIR / f"{bfg_id}.log").write_text("".join(out))

                # 6. Archive RUNDIR (pagedumps etc) for this BFG
                # RUNDIR_ARCHIVE_DIR.mkdir(exist_ok=True)
                # src_rundir = tf_dir / "RUNDIR"
                # if src_rundir.exists():
                #     dest_dir = RUNDIR_ARCHIVE_DIR / bfg_id
                #     if dest_dir.exists():
                #         shutil.rmtree(dest_dir)
                #     shutil.copytree(src_rundir, dest_dir)
                #     print(f"  archived RUNDIR to {dest_dir}")
                # else:
                #     print("  no RUNDIR directory found to archive")

            finally:
                subprocess.run(
                    ["git", "worktree", "remove", "--force", str(worktree_dir)],
                    cwd=ROOT,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                shutil.rmtree(worktree_dir, ignore_errors=True)

if __name__ == "__main__":
    main()