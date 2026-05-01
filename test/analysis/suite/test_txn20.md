# test_txn20 — Granular isolation level testing: dirty reads and non-repeatable reads

**File:** `test/suite/test_txn20.py`
**Storage mode:** General
**Components under test:** isolation levels (`read-uncommitted`, `read-committed`, `snapshot`), dirty read, non-repeatable read

## Test Cases

### `test_txn20.test_isolation_level`
- **What it tests:** Inserts `old_value`; begins a transaction updating to `new_value` without committing; in a second session with the specified isolation level reads the key: `read-uncommitted` sees `new_value` (dirty read), `read-committed` and `snapshot` see `old_value`; then commits the first transaction; reads again: `snapshot` still sees `old_value` (repeatable read), `read-committed` and `read-uncommitted` see `new_value` (non-repeatable read).
- **Components:** `txn.c`, `cursor.c`
- **Notes:** Parameterized over string-row/column × 3 isolation levels (6 scenarios). Directly tests the semantic difference between snapshot (fully repeatable), read-committed (non-repeatable), and read-uncommitted (dirty reads).
