# test_txn27 — WT_ROLLBACK reason codes: write conflict and oldest-pinned-for-eviction

**File:** `test/suite/test_txn27.py`
**Storage mode:** General
**Components under test:** `error_info` API, `WT_WRITE_CONFLICT`, `WT_OLDEST_FOR_EVICTION`, rollback reason codes

## Test Cases

### `test_txn27.test_rollback_reason`
- **What it tests:** (1) Updates key 5 in session1; session2 attempts to update the same key and gets `WT_ROLLBACK` (conflict); checks `error_info` API returns code `WT_WRITE_CONFLICT` with message "Write conflict between concurrent operations"; after rollback checks `error_info` returns success. (2) Begins a new transaction, inserts a 5MB value; sleeps 2 seconds for accounting; inserts another 1KB value which triggers cache pressure rollback; despite the exception message saying "conflict", checks `error_info` returns `WT_OLDEST_FOR_EVICTION` with message "Transaction has the oldest pinned transaction ID".
- **Components:** `txn.c`, `evict.c`, `error_info.c`
- **Notes:** Extends `error_info_util`. Uses 1MB cache to trigger cache pressure. Tests that `error_info` gives the true reason for `WT_ROLLBACK` even when the surface-level exception message is misleading.
