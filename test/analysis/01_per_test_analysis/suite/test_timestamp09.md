# test_timestamp09 — Timestamp API: string variant constraints

**File:** `test/suite/test_timestamp09.py`
**Storage mode:** General
**Components under test:** `timestamp_transaction` (string API), oldest/stable ordering constraints

## Test Cases

### `test_timestamp09.test_timestamp_api`
- **What it tests:** Exercises the string-based `timestamp_transaction` API: commit timestamp older than first commit in same txn rejected; commit < oldest rejected; oldest+stable combined set enforces ordering (oldest <= stable); setting stable backward rejected; setting oldest beyond stable rejected; commit timestamp at-or-below stable rejected. Read timestamp < oldest rejected (with MongoDB-specific error message in non-standalone builds). Force-setting oldest backwards and verifying `oldest_reader` after transactions.
- **Components:** `txn_timestamp.c`, `txn.c`
- **Notes:** Largely parallel to test_timestamp08 but using string API. Tests the interaction between `oldest_timestamp` and `stable_timestamp` constraints in detail.
