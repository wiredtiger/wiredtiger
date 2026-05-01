# test_hs29 — History store: three concurrent HS cursors during reconciliation

**File:** `test/suite/test_hs29.py`
**Storage mode:** General
**Components under test:** history store, reconciliation, checkpoint, non-timestamped updates, tombstone handling

## Test Cases

### `test_hs29.test_3_hs_cursors`
- **What it tests:** Exercises the code path that opens up to three concurrent history store cursors during a single reconciliation pass. Sequence:
  1. Creates two keys with two versions each (ts=2/3 for key '1', ts=10/20 for key '2').
  2. Evicts both keys using a `debug=(release_evict=true)` cursor.
  3. Opens an old reader at ts=2.
  4. Removes key '1' without a timestamp (triggers `wt_rec_hs_clear_on_tombstone`).
  5. Updates key '2' without a timestamp (triggers `hs_delete_reinsert_from_pos`).
  6. Runs a checkpoint at stable=20.
  7. Closes the connection (triggering a final checkpoint).
  
  The test asserts no crash occurs when reconciliation opens all three HS cursors simultaneously: the main reconciliation cursor, the `hs_delete_reinsert_from_pos` cursor, and the `wt_rec_hs_clear_on_tombstone` cursor.
- **Components:** `src/history/`, `src/reconcile/`, `src/checkpoint/`
- **Notes:** No scenarios; single test method. The three code paths are triggered simultaneously by the specific combination of a non-ts tombstone, a non-ts update, and an active old reader that is not globally visible.
