# Checkpoint Cleanup Page Removal Regression Test Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a statistic and regression assertion for on-disk pages selected for reading by checkpoint cleanup.

**Architecture:** Add a generated `checkpoint_cleanup_pages_read` statistic for every on-disk page selected by checkpoint cleanup after skip checks. Assert its delta in the existing logged-table reclaim-space test, reusing `test_cc_base.wait_for_cc_to_run()` for synchronization.

**Tech Stack:** WiredTiger C statistics plumbing, WiredTiger Python test suite, `wttest`, `dist/stat_data.py` generated statistic bindings.

---

### Task 1: Add the page-removal statistic

**Files:**
- Modify: `dist/stat_data.py`
- Modify: `src/btree/bt_sync_obsolete.c`
- Generated: `src/include/stat.h`, `src/include/wiredtiger.h.in`, `src/support/stat.c`

- [ ] **Step 1: Define the statistic**

Add the following entry next to the existing checkpoint-cleanup page-read
statistics in `dist/stat_data.py`:

```python
CheckpointCleanupStat('checkpoint_cleanup_pages_read',
    'pages read into cache by checkpoint cleanup'),
```

- [ ] **Step 2: Count selected page reads**

In `__checkpoint_cleanup_page_skip`, increment the new statistic after all
skip checks select the on-disk page for reading. Include obsolete-time-window
and reclaim-space reads while leaving their specialized counters unchanged.

- [ ] **Step 3: Regenerate generated statistic files**

Run:

```bash
cd dist && ./s_all
```

Expected: the generated declarations, definitions, reset logic, aggregation
logic, and public statistic bindings include `checkpoint_cleanup_pages_read`.

### Task 2: Assert page reads in the reclaim-space test

**Files:**
- Modify: `test/suite/test_cc08.py`
- Test: `test/suite/test_cc08.py`

- [ ] **Step 1: Assert the generic page-read delta**

Capture the new statistic around the existing cleanup trigger:

```python
pages_read_before = self.get_stat(stat.conn.checkpoint_cleanup_pages_read)
self.wait_for_cc_to_run()
pages_read_after = self.get_stat(stat.conn.checkpoint_cleanup_pages_read)
if self.cc_aggressive:
    self.assertGreater(pages_read_after - pages_read_before, 0)
```

The existing `cc_method_reclaim_space` scenario supplies the positive case,
while the `cc_method_none` scenario verifies that no reclaim-space pages are
selected.

- [ ] **Step 2: Run the focused test before implementation changes**

Run:

```bash
cd build && python3 ../test/suite/run.py test_cc09
```

Expected: the test fails before the statistic/production change because the
new statistic is unavailable; after the change it reports a positive read
delta for reclaim-space cleanup.

- [ ] **Step 3: Run the focused test after the test-only change**

Run:

```bash
cd build && python3 ../test/suite/run.py test_cc09
```

Expected: the existing `test_cc08` scenarios and the new assertion pass, with a
positive generic read delta only for reclaim-space cleanup.

- [ ] **Step 4: Commit the regression test**

```bash
git add dist/stat_data.py src/btree/bt_sync_obsolete.c src/include/stat.h \
    src/include/wiredtiger.h.in src/support/stat.c test/suite/test_cc08.py
git commit -m "WT-13076 track checkpoint cleanup page reads"
```

The commit must include the repository's required Copilot co-author trailer.
