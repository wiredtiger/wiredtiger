# test_timestamp25 — Backward-compatible short names for timestamp queries

**File:** `test/suite/test_timestamp25.py`
**Storage mode:** General
**Components under test:** `query_timestamp` short name aliases (`oldest`, `stable`)

## Test Cases

### `test_timestamp25.test_short_names`
- **What it tests:** Sets `oldest_timestamp=100` and queries it with both `get=oldest` and `get=oldest_timestamp`; confirms both return the same value. Repeats for `stable_timestamp` with `get=stable` and `get=stable_timestamp`.
- **Components:** `txn_timestamp.c`, `conn.c`
- **Notes:** Tests backward-compatible aliases for `query_timestamp` so existing code using short names continues to work.
