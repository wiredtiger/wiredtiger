# test_hs32 — History store: non-timestamped tombstone/update clears HS records (table-level, with optional long-running txn)

**File:** `test/suite/test_hs32.py`
**Storage mode:** General
**Components under test:** history store, eviction, checkpoint, non-timestamped updates/tombstones, statistics (cache_hs_key_truncate)

## Test Cases

### `test_hs32.test_non_ts_updates_tombstone_clears_hs`
- **What it tests:** Applies 4 rounds of timestamped updates (ts=1–4). Checkpoints and evicts all pages. Optionally starts a long-running transaction (read_ts=5) and adds another update at ts=5. Applies a non-ts update or deletion on every other row. Optionally checkpoints and evicts again (when long-running txn is active). Rolls back the long-running txn if present. Applies a final update at ts=10. Checkpoints. Reads at each ts=1–4 and verifies: odd-numbered rows see value1 (unchanged), even-numbered rows see either WT_NOTFOUND (deletion) or value2 (update), confirming that the non-ts operation blew away the old HS content. For deletions, asserts `cache_hs_key_truncate > 0`.
- **Components:** `src/history/`, `src/evict/`, `src/checkpoint/`
- **Notes:** Scenarios: key_format ∈ {`r`, `i`, `S`} × deletion/update × no-long-run/long-run = 12. Uses `table:` URI. nrows=10,000; cache_size=500MB. Very similar to test_hs11 but tests the on-page truncation path (`cache_hs_key_truncate`) rather than `cache_hs_key_truncate_onpage_removal`. Also covers the optional long-running transaction path not present in test_hs11.
