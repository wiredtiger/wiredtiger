---
name: wiredtiger-test-format
description: Run WiredTiger test/format (the ./t binary) — single or parallel runs, repro intermittent failures, tracing, verbose logging, and inspecting trace output.
---

# WiredTiger Format Tracing

## Purpose

Use this skill when the user wants to run test/format with tracing enabled or inspect trace output.

## Run Location

Run from the WiredTiger test/format build directory for the same repo/worktree where the user is currently working.

Do not default to a hardcoded absolute path if the user is in a worktree — always resolve from the current repo root.

## Required Preflight (Locate Build Dir + Binary)

Before running test/format, resolve the build directory from the current repo root and verify `./t` exists there.

```bash
# 1) Detect current repo/worktree root.
repo_root="$(git rev-parse --show-toplevel)"
echo "repo_root=$repo_root"

# 2) Find format build dir in this repo by locating ./t.
format_build_dir=""
for d in "$repo_root"/cmake-build*/test/format; do
  [ -x "$d/t" ] && format_build_dir="$d" && break
done

[ -n "$format_build_dir" ] || { echo "No test/format build dir with ./t found under $repo_root"; exit 1; }
echo "format_build_dir=$format_build_dir"

# 3) Verify config path in the same repo.
config="$repo_root/test/format/CONFIG.stress"
[ -f "$config" ] || { echo "Missing config: $config"; exit 1; }
```

## Primary Commands

Run `t` directly with tracing (after `cd "$format_build_dir"` from preflight):

**Default trace set (operations + transaction context):**
```bash
./t -c "$repo_root/test/format/CONFIG.stress" -h RUNDIR.TRACE -T ""
```

**Enable all trace categories:**
```bash
./t -c "$repo_root/test/format/CONFIG.stress" -h RUNDIR.TRACE -Tall
```

**Enable selected categories:**
```bash
./t -c "$repo_root/test/format/CONFIG.stress" -h RUNDIR.TRACE -T "bulk,read,timestamp,retain=25"
```

**Run via format.sh:**
```bash
./format.sh -c "$repo_root/test/format/CONFIG.stress" -T -n 1 -j 1
./format.sh -c "$repo_root/test/format/CONFIG.stress" -T "all,retain=25" -n 1 -j 1
```

## Reproducing Intermittent Failures (Run Until Fail)

If a bug is flaky, increase repro probability by running many jobs in parallel and looping until the first failure.

**Recommended fast path (single command loop, built-in parallelism):**
```bash
repo_root="$(git rev-parse --show-toplevel)"
while true; do
  ./format.sh -c "$repo_root/test/format/CONFIG.stress" -j 8 -n 64 -F
  rc=$?
  [ $rc -ne 0 ] && break
done
echo "stopped on failure"
```

Notes:
- `-j 8` runs 8 jobs in parallel (adjust to CPU/RAM)
- `-n 64` limits each batch size, then reruns another batch in the loop
- `-F` stops the batch on first failure
- Use `-T` (and optionally `-- -C 'verbose=(...)'`) when you want trace + verbose data on repro

**Recommended: repro_format_tmux.sh**

Use the companion script to spawn parallel tmux workers with isolated run homes, organized logs, and automatic cleanup.

```bash
# Run until first failure (default):
bash repro_format_tmux.sh

# Run a fixed number of iterations then stop (exit 0 if all pass):
bash repro_format_tmux.sh -j 5 -n 20 -c /path/to/CONFIG.stress

# Typical custom run:
bash repro_format_tmux.sh \
  -j 8 \
  -d "$(git rev-parse --show-toplevel)/cmake-build-debug/test/format" \
  -c "$(git rev-parse --show-toplevel)/test/format/CONFIG.stress"
```

Options:
| Flag | Default | Meaning |
|---|---|---|
| `-j workers` | 4 | Parallel tmux workers |
| `-d format_dir` | auto-detect | Build test/format dir |
| `-c config` | CONFIG.stress | Config path for `./t` |
| `-n total_runs` | 0 (unlimited) | Stop after N total passes |
| `-p prefix` | fmtrepro | tmux session prefix |
| `-k` | — | Keep successful run homes (default: delete) |

After failure, inspect artifacts:
```bash
ls -lt repro_tmux/<RUN_ID>/logs/ | head
rg -n "run FAILED|aborting WiredTiger library|WT_PANIC" repro_tmux/<RUN_ID>/logs/*.log
```

Check the failing run home for `leader.out`, `follower/follower.out`, and `OPS.TRACE`.

## Verbose Logging

Use `-C 'verbose=(...)'` with `-T` to include WiredTiger verbose messages in trace output.

**Direct `t` examples:**
```bash
./t -c CONFIG.stress -h RUNDIR.TRACE -Tall -C 'verbose=(block)'
./t -c CONFIG.stress -h RUNDIR.TRACE -Tall -C 'verbose=(block,checkpoint,transaction)'
```

**format.sh example (pass `-C` through after `--`):**
```bash
./format.sh -T "all" -n 1 -j 1 -- -C 'verbose=(block)'
```

When tracing is enabled, verbose messages are written to the `OPS.TRACE` logging home alongside operation trace messages.

## Trace Categories

Supported options for `-T`:

| Category | Description |
|---|---|
| `all` | All categories |
| `bulk` | Bulk load operations |
| `cursor` | Cursor operations |
| `mirror_fail` | Mirror failure events |
| `read` | Read operations |
| `timestamp` | Timestamp operations |
| `txn` | Transaction operations |
| `retain=N` | Retain last N records per thread |

## Trace Output Location

Tracing writes to a separate WiredTiger home:
```
<run-home>/OPS.TRACE
```

For example, if `-h RUNDIR.TRACE` is used, trace logs are in:
```
RUNDIR.TRACE/OPS.TRACE
```

## Dump and Inspect Trace Logs

**Dump the trace log:**
```bash
../../wt -h RUNDIR.TRACE/OPS.TRACE printlog -um > LOG
```

**Inspect:**
```bash
rg "run FAILED|<record-id>|commit|update|remove" LOG
```

**Filter to one table:**
```bash
rg 'T00002:' LOG
rg 'T00002:' LOG > LOG.T00002
```

**List all table URIs in the log:**
```bash
rg -o 'table:[A-Za-z0-9_]+' LOG | sort -u
```

**Keep full operation context for one table** (includes transaction begin/commit lines that lack table URIs):
```bash
awk '
/table:T00002:/ {
  if (match($0, /\[[0-9]+:0x[0-9A-Fa-f]+\].*: [0-9]+ /))
    keep[substr($0, RSTART, RLENGTH)] = 1
}
{
  if (match($0, /\[[0-9]+:0x[0-9A-Fa-f]+\].*: [0-9]+ /) &&
      keep[substr($0, RSTART, RLENGTH)])
    print
}' LOG
```
