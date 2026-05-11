# Build Path

Use this path to reproduce a WiredTiger failure locally, measure a flaky test's failure
rate, or propose and verify a fix.

## When to enter this path

- Need to confirm a hypothesis by reproducing the failure
- Measuring failure rate for a flaky/intermittent test
- Have a fix candidate and need to verify it resolves the failure

## Tools

- `evg_get_raw_task_logs` — extract the exact failing command from Evergreen
- `jira_get_issue`, `jira_get_issue_comments` — read failure context and prior repro attempts
- `jira_add_comment` — post repro findings (always confirm with user before posting)

## Workflow

### Step 1: Extract the exact repro command

From Evergreen task logs (`evg_get_raw_task_logs`, `log_type=agent`), find the command
the CI runner used. Look for lines like:
```
python3 ../test/suite/run.py test_checkpoint ...
./t -C <config> ...
ctest -R <test-name> ...
```

Also extract:
- CMake build flags used (e.g., `-DHAVE_DIAGNOSTIC=1`, `-DCMAKE_BUILD_TYPE=Debug`)
- Any env vars set before the test command

### Step 2: Choose repro mode

**Investigation mode** — stop on first failure, preserve artifacts:
```bash
# For test/format:
bash scripts/repro_format_tmux.sh \
  /data/wiredtiger \
  /data/wiredtiger/test/format/CONFIG \
  <workers> \
  /tmp/wt_repro \
  --stop-on-fail

# For Python suite:
python3 ../test/suite/run.py <test_name> -j1

# For ctest:
ctest --test-dir build -R <test_regex> --repeat until-fail:<N>
```

**Validation mode** — bounded repetitions to measure failure rate:
```bash
# test/format without --stop-on-fail:
bash scripts/repro_format_tmux.sh /data/wiredtiger CONFIG <workers> /tmp/wt_repro
```

See @repro-format.md for full format repro guidance.
See @wt-cli.md for inspecting the WT data directory after a failure.

### Step 3: Capture artifacts

Always record:
- Exact command line and config
- Build flags and branch/commit
- Worker count (for format runs)
- First failure: worker dir, `stdout.log`, `stderr.log`
- First error line in stderr

### Step 4: Propose a fix

State the narrowest change that addresses the root cause. Include:
- File and approximate line range
- What changes and why it fixes the invariant that was violated
- Any risk of side effects

### Step 5: Verify the fix

Re-run in validation mode with the fix applied:
- For format: use 4–8 workers, 20–50 iterations
- For Python suite: `python3 ../test/suite/run.py <test> -j4 --repeat 20`
- Report pass/fail rate before and after

## Output format

### Repro
- mode: (investigation | validation)
- command:
- config:
- workers:
- result: reproduced | not reproduced | inconclusive
- failure rate: X/N runs
- first failure dir / log path:

### Fix proposal
One paragraph describing the change and why it addresses the root cause.

### Verification
Pass/fail rate after fix applied.

### Next steps
1. ...
