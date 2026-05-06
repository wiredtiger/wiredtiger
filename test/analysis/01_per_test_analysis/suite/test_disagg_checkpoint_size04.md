# test_disagg_checkpoint_size04 — Table drop reclaims database size in checkpoint completion record

**File:** `test/suite/test_disagg_checkpoint_size04.py`
**Storage mode:** Disagg (via `@disagg_test_class` decorator; always disagg)
**Components under test:** src/checkpoint, src/schema (table drop), src/block_disagg, ext/page_log/palite, src/conn/conn_layered*.c

## Infrastructure notes

`test_disagg_checkpoint_size04` is decorated with `@disagg_test_class`, providing the
same infrastructure as other `@disagg_test_class` tests (see test_disagg_checkpoint_size01.md):
- Mixes in `DisaggConfigMixin`.
- Sets up `follower/` and `kv_home/` directories via `early_setup`.
- Loads the page log extension via `conn_extensions`.
- Suppresses `WT_VERB_RTS` at shutdown.

`conn_config` is set at the class level:
`'disaggregated=(role="leader",lose_all_my_data=true)'`

The helper `get_database_size()` parses `database_size=N` from the page log's checkpoint
completion record using `self.disagg_get_complete_checkpoint_meta()` (from
`DisaggConfigMixin`), the same approach as `test_disagg_checkpoint_size02`.

There is no `make_scenarios` — single scenario per environment.

## Test Cases

### `test_disagg_checkpoint_size04.test_drop_reduces_database_size`
- **What it tests:** Creates one layered table, records the empty-database size, inserts
  1000 rows × 500-byte values, checkpoints (records `size_with_data` and the data delta
  `data_size`), then calls `session.drop(uri)` and takes another checkpoint. Asserts that:
  (1) `size_after_drop < size_with_data` — the drop actually reduced the reported size;
  (2) `size_after_drop < size_empty + data_size * 0.1` — at least 90% of the dropped
  table's bytes are reclaimed, leaving only a small shared-metadata overhead.
- **Components:** `src/schema` (table drop, queued for next checkpoint),
  `src/checkpoint` (processes the drop and updates the completion record),
  `src/block_disagg` (frees blocks belonging to the dropped table in `bytes_total`),
  `ext/page_log/palite`
- **Notes:**
  - The drop is explicitly described as queued — it takes effect at the next checkpoint,
    not immediately. This is why a second `session.checkpoint()` call is required after
    `session.drop()` before the size is read.
  - The 10% slack (`data_size * 0.1`) accommodates residual shared metadata changes (e.g.
    the metadata table itself may grow slightly after the drop record is written).
  - Failure means either the drop does not reduce the size at all (blocks remain counted)
    or the reduction is too small (partial accounting), indicating a leak in the drop-to-
    checkpoint path for disaggregated storage.

### `test_disagg_checkpoint_size04.test_drop_one_of_multiple_tables`
- **What it tests:** Creates two layered tables (`test_keep` and `test_drop`), inserts
  1000 rows × 500-byte values into each, and checkpoints. Records `total_data` (combined
  size growth). Drops only the second table (`test_drop`) and checkpoints again. Asserts:
  (1) the amount of space reclaimed (`size_both - size_after_drop`) is greater than 30%
  of `total_data` — the dropped table's data was actually freed;
  (2) the surviving data (`size_after_drop - size_empty`) is also greater than 30% of
  `total_data` — the first table's data is still accounted for.
- **Components:** `src/schema`, `src/checkpoint`, `src/block_disagg`,
  `src/conn/conn_layered*.c` (multi-table size tracking after selective drop),
  `ext/page_log/palite`
- **Notes:**
  - The 30% threshold (rather than ~50%) is conservative to tolerate asymmetry in B-tree
    internal overhead between the two tables.
  - This test is the complement of `test_drop_reduces_database_size`: it verifies that
    dropping one table in a multi-table database does not incorrectly reclaim space from
    (or fail to preserve space for) the surviving table.
  - Failure in the "removed > 30% of total_data" direction means the drop had no effect.
    Failure in the "surviving > 30% of total_data" direction means the drop accidentally
    reclaimed pages from the kept table, indicating a bug in per-table block accounting.
