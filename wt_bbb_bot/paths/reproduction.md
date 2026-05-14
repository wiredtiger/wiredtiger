# Reproduction

Subagent path for local reproduction. Called from @paths/investigate.md Step 8.

For fix proposal and verification after a root cause is confirmed, see @paths/build.md.

## Inputs (from investigate.md)

- Ticket key and one-line summary
- Test name / command (from Step 3)
- Build variant (from Step 3: ASan / debug / release)
- Suspect commit / location (from Steps 6–7, for context only)
- Min iterations (from Step 5: 10 / 20 / 30)

## Subagent

```
Agent(
  subagent_type="general-purpose",
  prompt="""
Reproduce a WiredTiger BF locally. No fix proposals. No code changes.

Inputs:
- Ticket: WT-XXXXX — <summary>
- Test name / command: <from investigate.md Step 3>
- Build variant: <ASan / debug / release>
- Suspect commit / location: <from Steps 6–7, context only>
- Min iterations: <10 / 20 / 30>

Before writing a new Python test, search test/suite/ for analogous tests that
exercise the same scenario (e.g. prepared transactions, disagg role switching, RTS).
Copy their setup pattern exactly rather than deriving from first principles.

Steps (follow @paths/build.md — "Reproducing a BF Failure" section):
1. Build the matching variant if the build dir does not exist.
2. **If the failing test is a test/format run:** before running locally, extract the
   failing CONFIG from the Evergreen log.
   - Look for a block in the log beginning with "CONFIG" or a file attachment named
     `CONFIG` / `CONFIG.stress` in the Evergreen task artifacts.
   - Save it locally (e.g. `/tmp/FORMAT_CONFIG`).
   - Pass it via `-c /tmp/FORMAT_CONFIG` to `./t` or `format.sh`.
   - **Do NOT shorten `runs.timer` or any other parameter.** Use exact values from
     the extracted CONFIG. Shortening reduces fidelity and may prevent reproduction.
   - If no CONFIG is found in the log, record "config: not extracted" and run with
     the default `CONFIG.stress` — note this reduces reproduction fidelity.
3. Spawn two reproduction attempts **in parallel** — launch both before waiting for either:

   **3a. Format-based reproduction — synchronous parallel execution:**
   Do NOT use `run_in_background=true`. Use synchronous parallel shell execution so the
   Bash tool itself blocks until all instances exit. Set Bash `timeout` to
   `(runs.timer + 4) * 60 * 1000` ms (e.g. runs.timer=11 → timeout=900000).

   **Exhaustive reproduction policy — follow this loop:**

   - **Round 1:** Launch all 4 `./t` instances in a **single Bash call** using `&` + `wait`.
     Each instance gets its own RUNDIR and redirects stderr to a log file:
     ```bash
     cd /data/bbb-bot/wiredtiger/build/test/format
     for i in 0 1 2 3; do
       mkdir -p /data/bbb-bot/wiredtiger/RUNDIR_r1_$i
       ./t -h /data/bbb-bot/wiredtiger/RUNDIR_r1_$i -c /tmp/FORMAT_CONFIG \
         2>/data/bbb-bot/wiredtiger/RUNDIR_r1_$i/stderr.out &
     done
     wait  # blocks until ALL 4 exit
     # Collect results
     for i in 0 1 2 3; do
       grep -m1 "FAILED\|Abort\|Segmentation" \
         /data/bbb-bot/wiredtiger/RUNDIR_r1_$i/stderr.out 2>/dev/null \
         && echo "FAILED in RUNDIR_r1_$i" || echo "PASS in RUNDIR_r1_$i"
     done
     ```
     The Bash call does not return until all 4 processes exit. After it returns, read
     the collected stderr.out files to find the first error line and failing RUNDIR.
     Track total iterations and total failures across all instances.
   - **If any instance reproduced:** stop. Record the failing RUNDIR and first error line.
   - **If not reproduced or inconclusive:** continue to Round 2 — do NOT stop here.

   - **Round 2:** Derive a stress variant of the CONFIG guided by the suspect subsystem:
     - If the failure involves **prepared transactions**: increase `ops.prepare` and
       `runs.threads` to maximize prepare-conflict frequency.
     - If the failure involves **disagg/layered**: reduce `disagg.drain_threads` and
       increase `runs.threads` to tighten the switch-mode race window.
     - If the failure involves **RTS / checkpoint**: increase checkpoint frequency and
       reduce cache size to force more eviction pressure.
     - If the subsystem is unknown: increase `runs.threads` to max available CPUs.
     Launch 4 parallel instances with this stress CONFIG using the same `& wait` pattern
     (RUNDIR_r2_0 through RUNDIR_r2_3). Collect results from stderr.out after wait.
   - **If any instance reproduced:** stop. Record result and log path.
   - **If still not reproduced:** continue to Round 3.

   - **Round 3:** Strip unrelated operations from the CONFIG — keep only the ops directly
     implicated by the investigation (e.g. if the failure is in the prepare-conflict path,
     set `ops.insert=1 ops.update=1 ops.prepare=1` and zero out unrelated ops like
     `ops.truncate`, `ops.salvage`). This concentrates the workload on the hot path.
     Launch 4 parallel instances using the same `& wait` pattern (RUNDIR_r3_0 through
     RUNDIR_r3_3). Collect results from stderr.out after wait.
   - After Round 3, record result as `not reproduced` only if all three rounds failed
     across all instances. Record `inconclusive` if any instance hit a related failure
     (different assert or error in the same subsystem).

   **Do not give up after one pass.** Intermittent failures require sustained pressure.
   The user explicitly accepts long wait times. Report total iterations and total failures
   across all rounds and all instances regardless of when the first hit occurred.

   **3b. Python test reproduction — launch before format runs, collect after:**
   1. Search `test/suite/` for existing tests that exercise the same scenario
      (same subsystem and operation type — e.g. prepared transactions + layered cursor,
      disagg role switching, RTS). Read the most relevant one.
   2. In the **same Bash call as Round 1**, launch the Python test as a shell-level
      background process before the format loop, then collect its result after `wait`:
      ```bash
      cd /data/bbb-bot/wiredtiger/build
      # Launch Python test in background (shell-level &, not Bash run_in_background)
      python3 ../test/suite/run.py <test_name> \
        > /tmp/py_wt<ticket>.out 2>&1 &
      PY_PID=$!

      # Run format instances synchronously in parallel
      cd test/format
      for i in 0 1 2 3; do
        mkdir -p /data/bbb-bot/wiredtiger/RUNDIR_r1_$i
        ./t -h /data/bbb-bot/wiredtiger/RUNDIR_r1_$i -c /tmp/FORMAT_CONFIG \
          2>/data/bbb-bot/wiredtiger/RUNDIR_r1_$i/stderr.out &
      done
      wait  # waits for all format instances

      # Collect Python result
      wait $PY_PID; PY_EXIT=$?
      echo "Python exit: $PY_EXIT"; tail -30 /tmp/py_wt<ticket>.out
      ```
   3. If no matching test exists: write a minimal `test/suite/test_repro_<ticket_lower>.py`
      inline (base it on the closest analogous test's setUp/tearDown pattern), then use
      the same pattern above.

   The single Bash call does not return until both the format runs and the Python test
   have exited. Merge results — if either reproduces, record `reproduced` with which
   method succeeded.

Return only:
- mode: investigation
- format command: <exact command run>
- build variant: <value>
- format config: <"extracted from log" | "default CONFIG.stress" | "n/a — not a format run">
- format iterations run: N
- format result: reproduced | not reproduced | inconclusive
- format failure rate: X/N
- format first failure log: <path or "n/a">
- format first error line: <exact quoted string or "n/a">
- python test: <test file used or "n/a">
- python iterations run: N
- python result: reproduced | not reproduced | inconclusive | skipped — <reason>
- python failure rate: X/N
- python first error line: <exact quoted string or "n/a">
- overall result: reproduced | not reproduced | inconclusive  (reproduced if either method succeeded)
"""
)
```

## Returns to investigate.md

- **Overall result:** `reproduced | not reproduced | inconclusive | skipped — <reason>`
- **Format command:** `<exact command run>`
- **Build variant:** `<value>`
- **Format config:** `extracted from log | default CONFIG.stress | n/a — not a format run`
- **Format iterations run:** N
- **Format failure rate:** X/N
- **Format first failure log:** `<path, or "n/a">`
- **Format first error line:** `<exact quoted string, or "n/a">`
- **Python test:** `<test file used, or "n/a">`
- **Python iterations run:** N
- **Python result:** `reproduced | not reproduced | inconclusive | skipped — <reason>`
- **Python first error line:** `<exact quoted string, or "n/a">`
