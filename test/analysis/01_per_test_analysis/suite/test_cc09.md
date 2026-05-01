# test_cc09 — Checkpoint cleanup reads on-disk pages to remove obsolete time window info

**File:** `test/suite/test_cc09.py`
**Storage mode:** General (skips tiered)
**Components under test:** checkpoint cleanup subsystem, disk I/O path, heuristic controls, statistics

## Test Cases

### `test_cc09.test_cc09`
- **What it tests:** Verifies that CC correctly reads pages from disk to clear obsolete time window (TW) information, and that both the `checkpoint_cleanup_pages_read_obsolete_tw` and `checkpoint_cleanup_pages_obsolete_tw` stats are bounded by the configured heuristic page limit.
- **Components:** `src/btree/bt_read.c`, `src/btree/`, `src/conn/conn_sweep.c`
- **Notes:** Skip: `@wttest.skip_for_hook("tiered", ...)`. Cross-product of 5 page-limit scenarios × 3 CC trigger conditions: `newest_stop_durable_ts` (delete present), `obsolete_ts` (oldest advanced to nrows), `none` (neither). 100 000 rows of 1 KB each are populated, checkpointed, then the connection is reopened (forcing all pages to disk). CC is triggered via `wait_for_cc_to_run()`. Expected outcome: when `expected_cleanup=True` AND a valid CC condition applies (`has_delete` or `bump_oldest_ts`), both read stat and dirty stat are positive, with dirty stat ≤ `cc_obsolete_tw_max`. In all other combinations both stats must be 0.
