# test_prepare06 — roundup_timestamps=(prepared=true) allows prepare timestamp rounding

**File:** `test/suite/test_prepare06.py`
**Storage mode:** General
**Components under test:** prepared transactions, roundup_timestamps, timestamp API

## Test Cases

### `test_prepare06.test_timestamp_api`
- **What it tests:** Verifies that when a transaction is begun with `roundup_timestamps=(prepared=true)`, WiredTiger automatically rounds up the `prepare_timestamp` if it is less than or equal to `stable_timestamp` or `oldest_timestamp`, rather than returning an error
- **Components:** `txn/txn_prepare.c`, `txn/txn_timestamp.c`
- **Notes:** No scenarios; exercises the edge cases where prepare_ts <= stable_ts and prepare_ts <= oldest_ts; without roundup_timestamps the same calls would fail; confirms the rounded-up timestamp is accepted and the transaction can be committed; important for MongoDB's recovery path
