# Timestamp Aggregate Validation Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Prevent `WT_TS_MAX`, the “no comparable stop timestamp” sentinel, from
being rejected by aggregate parent validation while preserving validation for
ordinary stop timestamps.

**Architecture:** Keep the change localized to
`__time_aggregate_validate_parent` in `src/support/timestamp.c`. The existing
`test_rollback_to_stable34` scenario is the regression test because it already
reproduces the exact child/parent aggregate combination seen in Evergreen;
run its failing scenario directly before and after the code change rather than
duplicating the expensive integration setup.

**Tech Stack:** WiredTiger C internals, Python WiredTiger test suite, CMake/Ninja,
`dist/s_fast`.

---

### Task 1: Reproduce the failing rollback-to-stable scenario

**Files:**
- Test: `test/suite/test_rollback_to_stable34.py`
- Source under test: `src/support/timestamp.c`

- [ ] **Step 1: Run the focused reproducer**

Run from the configured build directory:

```bash
python3 ../test/suite/run.py test_rollback_to_stable34 -s 1
```

Expected before the fix: failure in `test_rollback_to_stable34` with an
aggregate validation error reporting `newest_stop_ts` after its parent.

- [ ] **Step 2: Record the invariant**

Use the failure output to confirm the failing values are equivalent to:

```text
child newest_stop_ts = WT_TS_MAX
parent newest_stop_ts = 35
```

No production or test code changes are made in this task.

### Task 2: Ignore the non-comparable sentinel in parent validation

**Files:**
- Modify: `src/support/timestamp.c:213`

- [ ] **Step 1: Change only the stop-timestamp parent check**

Replace:

```c
    if (ta->newest_stop_ts > parent->newest_stop_ts)
        WT_TIME_VALIDATE_RET(session,
          "aggregate time window has the newest stop time after its parent's; time aggregate %s, "
          "parent %s",
          __wt_time_aggregate_to_string(ta, time_string[0]),
          __wt_time_aggregate_to_string(parent, time_string[1]));
```

with:

```c
    if (ta->newest_stop_ts != WT_TS_MAX && ta->newest_stop_ts > parent->newest_stop_ts)
        WT_TIME_VALIDATE_RET(session,
          "aggregate time window has the newest stop time after its parent's; time aggregate %s, "
          "parent %s",
          __wt_time_aggregate_to_string(ta, time_string[0]),
          __wt_time_aggregate_to_string(parent, time_string[1]));
```

Do not change the durable timestamp, page-stop durable timestamp, oldest-start
timestamp, transaction, or stop-transaction checks.

- [ ] **Step 2: Run the focused reproducer**

Run:

```bash
python3 ../test/suite/run.py test_rollback_to_stable34 -s 1
```

Expected: PASS.

- [ ] **Step 3: Run the reproducer repeatedly**

Run:

```bash
for i in $(seq 10); do
    python3 ../test/suite/run.py test_rollback_to_stable34 -s 1 || break
done
```

Expected: all 10 runs pass without aggregate validation or crash failures.

### Task 3: Validate formatting and the affected build target

**Files:**
- Verify: `src/support/timestamp.c`
- Regression coverage: `test/suite/test_rollback_to_stable34.py` (existing
  scenario; no test-source change required)

- [ ] **Step 1: Run the fast repository validation**

Run:

```bash
cd dist && ./s_fast
```

Expected: validation completes successfully without modifying unrelated files.

- [ ] **Step 2: Build the affected targets**

Run:

```bash
cmake --build build --target wt
```

Expected: the `wt` target builds successfully.

- [ ] **Step 3: Review the final diff**

Run:

```bash
git diff --check
git diff -- src/support/timestamp.c test/suite/test_rollback_to_stable34.py
```

Expected: only the sentinel guard is present in production code; the existing
rollback-to-stable regression scenario remains unchanged unless the test
runner requires a narrowly scoped test selection adjustment.

- [ ] **Step 4: Commit the implementation**

```bash
git add src/support/timestamp.c
git commit -m "Fix timestamp aggregate parent validation" -m \
  "Co-authored-by: Copilot App <223556219+Copilot@users.noreply.github.com>"
```
