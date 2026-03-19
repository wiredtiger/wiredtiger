#!/usr/bin/env python3
import csv
import subprocess
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent
BFG_CSV = ROOT / "bfgs.csv"          # bfg_id,task_id,commit
OUT_DIR = ROOT / "bfg-configs"
EVERGREEN_BIN = "evergreen"          # change if it lives elsewhere

def fetch_task_log_via_cli(task_id: str) -> str:
    """
    Call the Evergreen CLI to get the task log for this task_id.
    Equivalent to:
      evergreen task build TaskLogs --task_id <task_id>
    but we capture stdout instead of redirecting to a file.
    """
    cmd = [EVERGREEN_BIN, "task", "build", "TaskLogs", "--task_id", task_id]
    print(f"  $ {' '.join(cmd)}")
    proc = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"evergreen CLI failed (rc={proc.returncode})\n{proc.stdout}")
    return proc.stdout

def extract_config(log_text: str) -> str:
    """
    Extract only the CONFIG block:

      - '############################################'
      - '#  RUN PARAMETERS: ...'
      - '#  TABLE PARAMETERS: table N'
      - 'key=value' lines

    Stop at the first non-empty line that doesn't look like one of the above.
    """
    lines = log_text.splitlines()

    # 1. Find the '#  RUN PARAMETERS' line
    run_idx = None
    for i, line in enumerate(lines):
        if "#  RUN PARAMETERS" in line:
            run_idx = i
            break
    if run_idx is None:
        raise ValueError("Could not find '#  RUN PARAMETERS' in log")

    start = max(0, run_idx - 2)  # include the hash banner above

    cfg_lines = []
    in_block = False

    param_re = re.compile(r"^[A-Za-z0-9_.]+\s*=")

    for line in lines[start:]:
        # Once we start, decide whether each line is part of the CONFIG
        if not in_block:
            # We enter the block when we see the RUN header or surrounding hashes
            if (
                "RUN PARAMETERS" in line
                or line.strip().startswith("#  TABLE PARAMETERS")
                or line.strip().startswith("############################################")
            ):
                in_block = True
                cfg_lines.append(line)
            continue
        else:
            # We are inside the block: accept only "config-like" lines.
            stripped = line.strip()

            # Empty lines inside the config: keep them (there usually aren't any, but safe)
            if stripped == "":
                cfg_lines.append(line)
                continue

            # Header lines and table headers
            if stripped.startswith("############################################") or stripped.startswith("#  "):
                cfg_lines.append(line)
                continue

            # key=value parameter lines
            if param_re.match(stripped):
                cfg_lines.append(line)
                continue

            # Anything else means we've reached the end of the CONFIG block
            break

    if not cfg_lines:
        raise ValueError("CONFIG block not found / empty")

    # Strip Evergreen timestamps like "[2026/01/27 ...] " if present
    cleaned = []
    for l in cfg_lines:
        if l.startswith("[20") and "]" in l:
            l = l.split("]", 1)[1].lstrip()
        cleaned.append(l)

    text = "\n".join(cleaned).rstrip() + "\n"
    if "#  RUN PARAMETERS" not in text:
        raise ValueError("RUN PARAMETERS not present after cleaning")
    return text

def main():
    OUT_DIR.mkdir(exist_ok=True)

    with open(BFG_CSV) as f:
        reader = csv.DictReader(f)
        for row in reader:
            bfg_id = row["bfg_id"]
            task_id = row["task_id"]
            commit = row["commit"].strip()

            out_path = OUT_DIR / f"CONFIG.{bfg_id}"
            if out_path.exists():
                print(f"{bfg_id}: CONFIG already exists, skipping")
                continue

            print(f"{bfg_id}: task_id={task_id} @ {commit}")
            try:
                log_text = fetch_task_log_via_cli(task_id)
                cfg = extract_config(log_text)
                out_path.write_text(cfg)
                print(f"  wrote {out_path}")
            except Exception as e:
                print(f"  ERROR: {e}")
                out_path.write_text(f"# FAILED to extract CONFIG: {e}\n")

if __name__ == "__main__":
    main()