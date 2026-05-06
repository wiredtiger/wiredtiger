# test_hs11 — History store: non-timestamped updates clear HS records; timestamped updates do not

**File:** `test/suite/test_hs11.py`
**Storage mode:** General
**Components under test:** history store, eviction, checkpoint, update without timestamp, modify, statistics (cache_hs_key_truncate_onpage_removal)

## Test Cases

### `test_hs11.test_non_ts_updates_clears_hs`
- **What it tests:** Applies timestamped updates at timestamps 1–4. Optionally applies a modify at ts=5. Optionally starts a long-running reader. Applies a non-timestamped update/deletion on every other row. Checkpoints and optionally runs eviction. Verifies that at each earlier timestamp (1–5), rows with the non-timestamped operation now see the non-timestamped value/deletion (HS records were blown away). For deletions, asserts `cache_hs_key_truncate_onpage_removal > 0`.
- **Components:** `src/history/`, `src/evict/`, `src/checkpoint/`, `src/modify/`
- **Notes:** Scenarios: key_format (column/integer-row/string-row) × deletion/update × long-running/no-long-running × modify/no-modify × small/large nrows × insert-list/update-list (location of update in chain). 192 total scenarios.

### `test_hs11.test_ts_updates_donot_clears_hs`
- **What it tests:** Applies timestamped updates at timestamps 1–4, optionally a modify at ts=5. Removes every other row at ts=10 (with a timestamp, not OOO). Advances oldest=10. Checkpoints. Applies updates at ts=20. Reads at ts=10 and verifies: deleted rows return WT_NOTFOUND, non-deleted rows return the expected value. Asserts `cache_hs_key_truncate_onpage_removal == 0` (timestamped tombstones do not clear HS records).
- **Components:** `src/history/`, `src/checkpoint/`
- **Notes:** Same scenario dimensions as above. The key difference from the first method: the removal is done with a timestamp so does not invalidate the HS history.
