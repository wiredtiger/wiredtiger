# test_verify2 — session.verify: EBUSY on dirty data, clean search, ENOENT on missing table

**File:** `test/suite/test_verify2.py`
**Storage mode:** General
**Components under test:** `session.verify`, EBUSY dirty-data check, empty-tree search without marking dirty, ENOENT on non-existent URI

## Test Cases

### `test_verify2.test_verify_ckpt`
- **What it tests:** Creates a table with stable_timestamp=10; inserts one record (making btree dirty); calls `session.verify` without checkpointing — expects `EBUSY` because of dirty data; checkpoints to clean the btree; calls `session.verify` again — expects success.
- **Components:** `verify.c`, `btree.c`, `txn.c`
- **Notes:** No parameterization. Tests that verify correctly rejects dirty tables with `EBUSY`.

### `test_verify2.test_verify_search`
- **What it tests:** Creates an empty table with stable_timestamp=10; searches for a non-existent key (`WT_NOTFOUND`); calls `session.verify` without checkpointing — expects success (search on empty tree should not mark btree dirty). Regression for WT-8126 where empty-tree search incorrectly set btree modified flag.
- **Components:** `verify.c`, `btree.c`, `cursor.c`
- **Notes:** No parameterization. Tests that `cursor.search` on an empty table does not trigger false EBUSY.

### `test_verify2.test_verify_empty`
- **What it tests:** Calls `session.verify` on a non-existent URI (`table:test_verify`) without creating it first; asserts `WiredTigerError` with `ENOENT` message (`os.strerror(errno.ENOENT)`).
- **Components:** `verify.c`, `schema.c`
- **Notes:** No parameterization. Tests that verifying a non-existent table returns the correct error.
