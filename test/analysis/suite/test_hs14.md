# test_hs14 — History store: performance — invisible HS records not much slower than visible

**File:** `test/suite/test_hs14.py`
**Storage mode:** General
**Components under test:** history store, cursor read performance, timestamps

## Test Cases

### `test_hs14.test_hs14`
- **What it tests:** Writes 10,000 rows with multiple values at ts=2 (two updates), ts=3, and ts=4. Checkpoints to push older versions to HS. Times a full-table scan at ts=3 (all HS records visible) to get `visible_hs_latency`. Then removes all rows at ts=5 and re-inserts at ts=10. Checkpoints again. Times a full-table scan at ts=9 (all HS records invisible because the key was deleted at ts=5) to get `invisible_hs_latency`. Asserts `invisible_hs_latency < visible_hs_latency * 10` — the "invisible" case (where we must look but not find) should not be an order of magnitude worse than the "visible" case.
- **Components:** `src/history/`, `src/cursor/`
- **Notes:** Scenarios: key_format ∈ {`r`, `S`}; value_format=S; `cache_size=500MB`. Addresses a performance regression where skipping invisible HS records was as expensive as reading visible ones. The 10x bound is chosen to be a generous safety threshold while still catching significant regressions.
