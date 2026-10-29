# Dirty-index Drain Bounds Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bound dirty-index slot scans per btree visit, make urgent queue submissions count as productive drain work, and verify the drain runs during split-retirement stress.

**Architecture:** Keep ordinary queue-slot consumption separate from drain productivity. Limit ring slots examined to four times the queue slots still available for this tree, while retaining uncapped-by-budget occupancy for the ring high-water statistic. Count successful ordinary and urgent submissions as productive for the adaptive empty-drain scheduler.

**Tech Stack:** WiredTiger C eviction code, Catch2, Python WiredTiger test suite.

---

## File map

- `src/evict/evict_private.h`: define the drain scan multiplier beside the existing adaptive-drain constants.
- `src/evict/evict_walk.c`: calculate the per-visit scan bound, preserve full occupancy accounting, and pass productive submission counts independently of ordinary queue slots.
- `test/suite/test_eviction08.py`: assert that drain scanning advances while split-producing writes are in progress, polling with a deadline.

## Task 1: Bound dirty-index slot examinations per visit

**Files:**
- Modify: `src/evict/evict_private.h`
- Modify: `src/evict/evict_walk.c`

- [ ] **Step 1: Add the scan multiplier**

Add next to the adaptive-drain constants in `src/evict/evict_private.h`:

```c
#define WTI_DIRTY_INDEX_SCAN_MULTIPLIER 4u
```

- [ ] **Step 2: Preserve occupancy and cap scans**

In `__evict_dirty_index_drain_ring`, keep ring occupancy separate from the per-visit scan limit:

```c
uint64_t occupancy, pos, scan_limit, seq;
```

Replace the current `scan_limit` initialization and occupancy-stat update with:

```c
pos = __wt_atomic_load_uint64_relaxed(&idx->tail);
occupancy =
  WT_MIN(__wt_atomic_load_uint64_acquire(&idx->head) - pos, idx->capacity);
scan_limit = WT_MIN(occupancy,
  (uint64_t)(max_entries - *slotp) * WTI_DIRTY_INDEX_SCAN_MULTIPLIER);
if (WT_STAT_ENABLED(session))
    __wt_atomic_stats_max_uint64(
      &S2C(session)->evict->dirty_index_ring_peak_occupancy, occupancy);
```

The loop remains bounded by `scanned < scan_limit`; `occupancy` must remain the input to the
high-water statistic so the new work cap does not hide ring pressure.

- [ ] **Step 3: Check the cap against available slots**

Run:

```bash
git diff --check
```

Expected: no whitespace errors. Verify the loop still consumes `NULL`/stale entries and stops after
at most four times the remaining ordinary queue slots, even when the ring contains 256K entries.

## Task 2: Treat urgent submissions as productive drain work

**Files:**
- Modify: `src/evict/evict_walk.c`

- [ ] **Step 1: Add an independent productive-count output**

Add `u_int *productivep` to the static signatures of `__evict_dirty_index_drain` and
`__evict_dirty_index_drain_ring`. Initialize the output to zero on every early-return path in the
outer helper; at the end of the ring helper set:

```c
*drainedp = drained;
*productivep = queued_total;
```

`drained` counts ordinary candidates consuming queue slots. `queued_total` already increments for
either `queued` or `urgent_queued`, so it is the productive count.

- [ ] **Step 2: Use productivity only for adaptive scheduling**

Add `drain_productive` to `__evict_walk_tree` and pass it through both helpers. Keep
`drain_queued` for queue-budget decisions and update the adaptive switch to use the productive count:

```c
if (drain_productive > 0) {
    __wt_atomic_store_uint32(&btree->drain_consecutive_empty, 0);
    if (__wt_atomic_load_bool_relaxed(&btree->drain_disabled))
        __wt_atomic_store_bool(&btree->drain_disabled, false);
} else if (!WT_BTREE_SYNCING(btree) &&
  __wt_atomic_add_uint32(&btree->drain_consecutive_empty, 1) >=
    WTI_DRAIN_EMPTY_THRESHOLD) {
    __wt_atomic_store_bool(&btree->drain_disabled, true);
    __wt_atomic_store_uint64_relaxed(
      &btree->drain_next_probe_gen, pass_gen + WTI_DRAIN_PROBE_INTERVAL);
}
```

Do not use `drain_productive` in `drain_queued >= target_pages` or in walker queue-budget accounting:
urgent submissions use a different queue and do not consume the ordinary per-tree slots.

- [ ] **Step 3: Verify call sites and declarations**

Run:

```bash
git diff --check
```

Expected: the static declarations, definitions, and calls of both drain helpers agree on their
arguments. Confirm that an ordinary submission increments both counts, an urgent submission
increments only productivity, and a filtered/stale/hazard-blocked slot increments neither.

## Task 3: Assert drain progress during split retirement

**Files:**
- Modify: `test/suite/test_eviction08.py`

- [ ] **Step 1: Capture the baseline and poll while writing**

In `test_dirty_index_split_retirement`, capture the starting drain scan count before the workload:

```python
drain_scanned = stat.dsrc.cache_eviction_dirty_index_drain_scanned
initial_scanned = self.get_stat(drain_scanned, uri)
drain_ran_during_writes = False
```

Write the split-producing rows in batches using the existing `_write_batch` helper, checking the
statistic after each committed batch:

```python
cursor = self.session.open_cursor(uri)
deadline = time.time() + 120
for pass_num in range(3):
    start = pass_num * self.nrows
    for batch_index, batch_start in enumerate(
      range(start, start + self.nrows, self.batch_size)):
        self._write_batch(
            cursor,
            batch_start,
            min(batch_start + self.batch_size, start + self.nrows),
            value)
        if batch_index % 10 == 0 or batch_start + self.batch_size >= start + self.nrows:
            self.assertLess(
                time.time(), deadline, 'dirty-index drain did not run during split writes')
            if self.get_stat(drain_scanned, uri) > initial_scanned:
                drain_ran_during_writes = True
cursor.close()
```

- [ ] **Step 2: Assert split and drain evidence**

Keep the existing in-memory split assertion and add:

```python
self.assertTrue(drain_ran_during_writes)
```

The test must still verify that the in-memory split counter increased, so a passing drain assertion
cannot mask a workload that failed to exercise ref retirement.

- [ ] **Step 3: Run the focused test**

Run from the configured build directory:

```bash
python3 ../test/suite/run.py test_eviction08
```

Expected: all `test_eviction08` cases pass, including the split-retirement case. If the drain does
not run before the deadline, the test should fail with the explicit timeout message rather than
passing without observing concurrent drain activity.

- [ ] **Step 4: Run formatting and generated-code validation**

Run:

```bash
cd dist && ./s_fast
```

Expected: validation succeeds without unrelated generated-file changes.

## Self-review

- The four-times scan bound affects only examination work; ordinary queue entries remain governed by
  the existing queue budget.
- The existing ring occupancy gauge continues to report actual occupancy, capped only at capacity.
- The adaptive scheduler uses successful ordinary or urgent submissions, not filtered/stale slots.
- The split test requires both actual in-memory splits and drain progress observed during its writes.
