# test_hs31 — History store: non-timestamped tombstone clears HS records (file-level)

**File:** `test/suite/test_hs31.py`
**Storage mode:** General
**Components under test:** history store, eviction, checkpoint, non-timestamped tombstone, statistics (cache_hs_key_truncate_onpage_removal, rec_hs_wrapup_next_prev_calls)

## Test Cases

### `test_hs31.test_mm_tombstone_clear_hs`
- **What it tests:** Applies timestamped updates (ts=10–14) on a `file:` table. Checkpoints and evicts all pages. Opens a long-running transaction to pin oldest ID. Removes all keys without a timestamp (OOO tombstones). Optionally checkpoints again (to write stop time window). Verifies the long-running reader can still see the pre-tombstone value (value1). Rolls back the long-running transaction. Advances oldest=10 and checkpoints. Evicts and verifies all keys are now WT_NOTFOUND. Inserts new values at ts=20. Reads at each ts=10–14 and asserts keys are WT_NOTFOUND (HS content was blown away by the non-ts tombstone). Asserts `cache_hs_key_truncate_onpage_removal > 0` and `rec_hs_wrapup_next_prev_calls > 0`.
- **Components:** `src/history/`, `src/evict/`, `src/checkpoint/`, `src/reconcile/`
- **Notes:** Scenarios: key_format ∈ {`r`, `i`, `S`} × globally_visible_before_ckpt ∈ {T,F} = 6. Uses `file:` URI (not `table:`). The `globally_visible_before_ckpt=False` branch skips the intermediate checkpoint. Checks both the truncation stat and the wrapup next/prev iteration stat.
