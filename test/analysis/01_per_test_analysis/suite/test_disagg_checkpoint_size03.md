# test_disagg_checkpoint_size03 — Checkpoint size leak regressions in delta-page reconciliation

**File:** `test/suite/test_disagg_checkpoint_size03.py`
**Storage mode:** Disagg (via `@disagg_test_class` decorator; always disagg)
**Components under test:** src/btree/rec_write.c, src/block_disagg/block_disagg_read.c, src/checkpoint, src/conn/conn_layered*.c, src/stat

## Infrastructure notes

`test_disagg_checkpoint_size03` is decorated with `@disagg_test_class`, providing the
same infrastructure as other `@disagg_test_class` tests (see test_disagg_checkpoint_size01.md).

`conn_config` is set at the class level to enable aggressive delta emission:
```
'disaggregated=(role="leader",lose_all_my_data=true),
 page_delta=(delta_pct=90,internal_page_delta=true,leaf_page_delta=true,max_consecutive_delta=5)'
```
`delta_pct=90` means a delta is emitted whenever it is smaller than 90% of the full page
size, making delta creation very likely. `max_consecutive_delta=5` caps the delta chain
length before a full page is forced.

Individual tests may call `self.conn.reconfigure('page_delta=(delta_pct=N)')` to change
the delta threshold mid-test.

The helper `get_checkpoint_size()` reads the `size=N` field from the last checkpoint entry
in the `file:<uri_base>.wt_stable` metadata cursor, identical in structure to
test_disagg_checkpoint_size01.

There is no `make_scenarios` — single scenario per environment.

## Test Cases

### `test_disagg_checkpoint_size03.test_bytes_total_leak`
- **What it tests:** Regression test for a `bytes_total` accounting leak in single-page
  reconciliation. Inserts 1 row, checkpoints (baseline), then rewrites that row 12 times
  (one rewrite per checkpoint cycle). With `delta_pct=20` (reconfigured at test start to
  force full-page writes every cycle), verifies (1) that the `rec_page_delta_leaf` stat is
  exactly 0 (no deltas emitted), and (2) that the final checkpoint `size` is less than
  2× the baseline — i.e. it has not grown unboundedly across 12 rewrite cycles.
- **Components:** `src/btree/rec_write.c` (`disagg_page_free_required` flag,
  `disagg_free_block` in `__wt_ref_block_free`), `src/block_disagg` (old block freeing
  during single-page reconciliation), `src/checkpoint`, `src/stat`
- **Notes:**
  - The test comment explicitly names the suspected leak site:
    `disagg_page_free_required` in `rec_write.c` and `disagg_free_block` in
    `__wt_ref_block_free()`.
  - `delta_pct` is reconfigured from 90% (class default) to 20% at test start; any
    leftover class-level reconfiguration does not affect other tests.
  - Failure (final ≥ 2× baseline) would indicate that each full-page rewrite accumulates
    stale block references in `bytes_total` without freeing them.

### `test_disagg_checkpoint_size03.test_bytes_total_leak_delta`
- **What it tests:** Regression test for the same `bytes_total` leak specifically in the
  delta-write path. Inserts 10 rows (baseline checkpoint), then over 7 cycles updates
  every other key (5 of 10 keys) and checkpoints. Uses the class-level `delta_pct=90` to
  ensure deltas are emitted. After all cycles, asserts (1) that at least one delta was
  recorded in `rec_page_delta_leaf`, and (2) that the final size is less than 2× baseline.
- **Components:** `src/btree/rec_write.c` (delta chain termination handling),
  `src/block_disagg`, `src/checkpoint`, `src/stat`
- **Notes:**
  - Unlike `test_bytes_total_leak`, this test does not reconfigure `delta_pct`, relying on
    the class-level 90% setting to guarantee delta production.
  - The comment names the relevant fix location: "delta chain termination handling in
    `rec_write.c`".
  - Failure means delta writes do not properly free superseded block references, causing
    the reported checkpoint size to grow proportionally to the number of delta cycles.

### `test_disagg_checkpoint_size03.test_bytes_total_leak_delta_normal_ops`
- **What it tests:** A variation of the delta leak test using a "normal operations" pattern:
  inserts 10 rows, then over 5 cycles updates every other key and checkpoints. After each
  cycle, asserts that `rec_page_delta_leaf` is strictly greater than zero, confirming that
  the workload actually exercises the delta path in every cycle. Does not check the
  final size ratio — this test's purpose is to validate delta creation and per-cycle stat
  accounting rather than leak magnitude.
- **Components:** `src/btree/rec_write.c`, `src/block_disagg`, `src/checkpoint`, `src/stat`
- **Notes:**
  - The per-cycle stat assertion (inside the loop) catches regressions where deltas are
    created in early cycles but silently fall back to full-page writes in later cycles.
  - Uses class-level `delta_pct=90` (no reconfigure).

### `test_disagg_checkpoint_size03.test_size_leak_after_rec_result_page_clean`
- **What it tests:** Regression test for a leak triggered when `rec_result` is set to
  `WT_PAGE_CLEAN`. Inserts 20 rows × 200 bytes (baseline ~4 KB). Generates a delta by
  updating 4 of the 20 keys. Then explicitly evicts the leaf page via a debug cursor
  (`debug=(release_evict)`). Then reconfigures `delta_pct=1` (forces full-page writes),
  rewrites all 20 rows, and checkpoints. Asserts that the final size is less than 1.2×
  baseline — the page should not carry both the old page's bytes and the new page's bytes
  in `bytes_total`.
- **Components:** `src/btree/rec_write.c` (handling of `WT_PAGE_CLEAN` after eviction read-in),
  `src/evict` (debug eviction cursor), `src/block_disagg`, `src/checkpoint`
- **Notes:**
  - The sequence — write → delta → evict → full rewrite — is specifically crafted to
    trigger the code path where a page is read back from the page log after eviction and
    then reconciled as a full page, which previously leaked the full page's `bytes_total`
    even though the old disagg block was still being counted.
  - `session.breakpoint()` is called before the eviction step (likely a debugging aid left
    in; does not affect test logic in normal runs).
  - Failure means the old and new page sizes are both counted, doubling the size to ~8 KB
    instead of staying near the original ~4 KB.

### `test_disagg_checkpoint_size03.test_cumulative_size_leak_after_eviction`
- **What it tests:** Regression test for a bug in `block_disagg_read.c` where
  `cumulative_size` was set to only the most recent delta's raw size instead of the true
  cumulative total of base + all preceding deltas. Runs 10 cycles, each consisting of:
  (1) create a delta (update 4 of 20 rows, checkpoint), (2) evict the leaf page via debug
  cursor, (3) reconfigure `delta_pct=1` to force a full-page rewrite and checkpoint.
  After each step, verifies that new deltas were actually created. After all 10 cycles,
  asserts the final size is less than 2× baseline.
- **Components:** `src/block_disagg/block_disagg_read.c` (`cumulative_size` computation
  on page read-in), `src/btree/rec_write.c`, `src/evict`, `src/checkpoint`, `src/stat`
- **Notes:**
  - The per-cycle `assertGreater(new_deltas, 0)` check ensures the test exercises the delta
    path in every cycle, not just the first.
  - The specific bug targeted: after eviction, when the page is read back, the block layer
    previously set `cumulative_size` to the size of the last delta alone, discarding the
    accumulated base + earlier deltas. On the subsequent full rewrite the freed size was
    therefore understated, causing the old blocks to leak into `bytes_total`.
  - The 10-cycle repetition amplifies any residual leak to make it detectable above the 2×
    threshold.
  - Failure means each evict-then-full-rewrite cycle accumulates leaked bytes linearly,
    growing the checkpoint size approximately `n_cycles × delta_size` beyond the true data volume.
