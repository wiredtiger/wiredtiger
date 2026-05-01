# test_cc07 — Heuristic page limits for obsolete time window cleanup

**File:** `test/suite/test_cc07.py`
**Storage mode:** General (skips tiered)
**Components under test:** checkpoint cleanup subsystem, heuristic controls, statistics

## Test Cases

### `test_cc07.test_cc07`
- **What it tests:** Verifies that `heuristic_controls` knobs `obsolete_tw_btree_max` and `checkpoint_cleanup_obsolete_tw_pages_dirty_max` correctly limit (or disable) the number of pages with obsolete time windows that CC will dirty per btree per checkpoint cycle.
- **Components:** `src/btree/`, `src/conn/conn_sweep.c`
- **Notes:** Skip: `if self.runningHook('tiered')`. Five scenarios varying the per-btree page limit: `no_btrees` (btree_max=0 → no cleanup), `no_pages` (pages_dirty_max=0 → no cleanup), `50_pages`, `100_pages`, `500_pages`. Inserts 10 rounds of 1 000 rows (1 KB each) with advancing stable/oldest timestamps so all data eventually becomes obsolete. After CC runs, reads `dsrc.checkpoint_cleanup_pages_obsolete_tw` and `conn.checkpoint_cleanup_pages_obsolete_tw`. For no-cleanup scenarios asserts both equal 0; for others asserts `btree_stat <= obsolete_tw_max` and `conn_stat > 0`.
