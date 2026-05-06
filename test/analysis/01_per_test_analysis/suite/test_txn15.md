# test_txn15 — Transaction sync modes: log_release_write_lsn and log_sync stats

**File:** `test/suite/test_txn15.py`
**Storage mode:** General (skipped for disagg — logging stats not relevant)
**Components under test:** transaction sync modes (`sync=on/off/true/false`), `log_release_write_lsn`, `log_sync` stats

## Test Cases

### `test_txn15.test_sync_ops`
- **What it tests:** Skips if begin and commit both have explicit sync settings (illegal combination); inserts 100 records in a transaction with the given begin_sync and commit_sync settings; checks `log_release_write_lsn` and `log_sync` stats before and after: if effective sync level is `None` (disabled), stats are unchanged; if `write`, `log_release_write_lsn` increments; if `sync`, both `log_release_write_lsn` and `log_sync` increment.
- **Components:** `log.c`, `txn.c`
- **Notes:** Parameterized over integer-row/column × conn_sync_enabled (on/off) × conn_sync_method (dsync/fsync/none) × begin_sync (true/None/false) × commit_sync (on/None/off). Derives effective sync level from the combination of connection config and per-transaction override. Tests that the correct code paths are taken for each combination.
