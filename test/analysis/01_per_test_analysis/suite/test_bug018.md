# test_bug018 — WT-3590: table sync consistency when a write fails during close

**File:** `test/suite/test_bug018.py`
**Storage mode:** General (logging enabled)
**Components under test:** connection close, log recovery, multi-table consistency

## Test Cases

### `test_bug018.test_bug018`
- **What it tests:** Reproduces WT-3590 where, if writing table data fails during connection close, tables updated in the same transaction could end up out of sync after recovery. Spawns a subprocess that: opens two `file:` tables, inserts `'key'='value'` in each within the same transaction, then forcibly closes the OS file descriptor for the second table's `.wt` file immediately before closing the connection — simulating a write failure on that file. The main process then reopens the database directory (running WAL recovery) and asserts that both tables have the same contents (either both have the record or both are empty/unreadable).
- **Components:** `src/conn/conn_close.c`, `src/log/log_recover.c`
- **Notes:** Linux-specific (`/proc/self/fd` introspection). Skipped for `nonstandalone`, `tiered` hooks, and when running under TSan. Uses `run_subprocess_function` and `copy_wiredtiger_home` for forensics.
