# Reproduce format / flaky failures

Use this path for:
- `test/format`
- "run this until it fails"
- intermittent WT failures
- repeated repro with logging
- quick validation after a fix

# Workflow

## Step 1: Identify repro target

Collect:
- branch or commit
- config file or seed (typically `CONFIG` or a `.wtperf` file under `test/format/`)
- how many parallel workers to run
- whether this is investigation mode or validation mode

If the user does not specify a config, start with the default: `test/format/CONFIG`.

## Step 2: Choose mode

**Investigation mode** — stop at first failure:
- launch workers
- watch for any non-zero exit code
- preserve logs from the failing worker
- kill remaining workers
- report the failing worker directory

**Validation mode** — bounded repetitions:
- run a fixed number of repetitions
- count failures across all runs
- summarize pass/fail rate at the end

## Step 3: Use the helper script

Use `scripts/repro_format_tmux.sh` rather than an ad hoc loop.

```sh
# Investigation mode (stop-on-fail):
bash scripts/repro_format_tmux.sh \
  /data/wiredtiger \
  /data/wiredtiger/test/format/CONFIG \
  <workers> \
  /tmp/wt_repro \
  --stop-on-fail

# Validation mode (no --stop-on-fail flag):
bash scripts/repro_format_tmux.sh \
  /data/wiredtiger \
  /data/wiredtiger/test/format/CONFIG \
  <workers> \
  /tmp/wt_repro
```

Monitor with:
```sh
tmux ls
cat /tmp/wt_repro/worker_*/exit_code.txt
```

## Step 4: Preserve artifacts

Capture and report:
- command line used
- worker count
- config file path (and a copy of its contents)
- first failing run directory
- log paths (`stdout.log`, `stderr.log`)
- summary of the failure signature (first error line in stderr)

# Output format

## Repro summary
- mode:
- workers:
- config:
- reproduced: yes/no
- first failure: (worker dir or "none")
- logs: (paths)

## Working theory
One short paragraph.

## Next checks
1. ...
2. ...
