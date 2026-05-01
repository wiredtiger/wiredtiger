# test_checkpoint31 — Read-only connection checkpoint cursor with prepared transactions

**File:** `test/suite/test_checkpoint31.py`
**Storage mode:** General
**Components under test:** checkpoint cursor, read-only connection, prepared transactions

## Test Cases

### `test_checkpoint.test_checkpoint`
- **What it tests:** Verifies that a read-only connection can open a checkpoint cursor and correctly read data even when the database was closed with an active prepared transaction. The checkpoint cursor must handle the prepared state correctly under read-only mode.
- **Components:** `src/conn/conn_open.c`, `src/cursor/cur_btree.c`, `src/txn/txn_prepare.c`
- **Notes:** A prepared transaction is committed (or left prepared) and the connection is closed. The database is reopened as read-only. A checkpoint cursor is opened and reads are performed. Tests that the read-only code path correctly handles prepared transaction state stored in the checkpoint.
