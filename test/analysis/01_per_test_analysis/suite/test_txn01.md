# test_txn01 — Basic transaction visibility: isolation levels and checkpoint

**File:** `test/suite/test_txn01.py`
**Storage mode:** General
**Components under test:** transaction visibility, isolation levels (`read-uncommitted`, `read-committed`, `snapshot`), checkpoint visibility

## Test Cases

### `test_txn01.test_visibility`
- **What it tests:** Inserts 1,000 records in batches with periodic commits; before each commit checks that: own cursor sees all uncommitted writes; `read-uncommitted` cursors in a new session see all writes; `snapshot` and `read-committed` cursors see only committed records; checkpoint sees only committed records. Repeats check after final commit confirming all records visible to all isolation levels.
- **Components:** `txn.c`, `cursor.c`, `checkpoint.c`
- **Notes:** Parameterized over file/table × column/row (4 scenarios). Foundational test of isolation level visibility semantics. Isolation: `read-uncommitted` always sees all, `snapshot` and `read-committed` see only committed.

### `test_read_committed_default.test_read_committed_default`
- **What it tests:** Inserts one record and commits; inserts another without committing; verifies that a `read-committed` cursor in a second session sees only one record; also verifies a session begun with `None` (default isolation) also sees only one record, confirming `read-committed` is the default isolation level.
- **Components:** `txn.c`, `cursor.c`
- **Notes:** Standalone class (not parametrized). Confirms the default isolation level is `read-committed`.
