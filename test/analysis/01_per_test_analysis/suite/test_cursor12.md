# test_cursor12 — cursor.modify() API: boundary conditions, isolation, persistence, recovery

**File:** `test/suite/test_cursor12.py`
**Storage mode:** General
**Components under test:** cursor modify, MVCC, snapshot isolation, recovery, row-store, VLCS

## Test Cases

### `test_cursor12.test_modify_txn_api`
- **What it tests:** Verifies `cursor.modify()` enforces snapshot isolation; fails with read-committed or read-uncommitted transactions.
- **Components:** `src/cursor/cur_modify.c`, `src/txn/`
- **Notes:** Scenarios: file/table × recno/string × item(u)/string(S). Expects `ENOTSUP` for non-snapshot isolation.

### `test_cursor12.test_modify_smoke`
- **What it tests:** Basic modify operations: rewrite at beginning, rewrite at end, append, shrink, grow, discard. Verifies resulting value.
- **Components:** `src/cursor/cur_modify.c`, `src/btree/`

### `test_cursor12.test_modify_smoke_single`
- **What it tests:** Single-item modify list; verifies minimal modify path.
- **Components:** `src/cursor/cur_modify.c`

### `test_cursor12.test_modify_smoke_reopen`
- **What it tests:** Modify followed by connection reopen; verifies persisted value matches expected.
- **Components:** `src/cursor/cur_modify.c`, `src/btree/bt_read.c`

### `test_cursor12.test_modify_smoke_recover`
- **What it tests:** Modify followed by crash-recovery (close without checkpoint, reopen); verifies recovery is consistent.
- **Components:** `src/cursor/cur_modify.c`, `src/log/`, `src/txn/txn_recover.c`

### `test_cursor12.test_modify_many`
- **What it tests:** Many sequential modifies on a single key; exercises modify chain length. Verifies eventual value.
- **Components:** `src/cursor/cur_modify.c`, `src/btree/`
- **Notes:** Skipped for timestamp hook.

### `test_cursor12.test_modify_delete`
- **What it tests:** Modify on a deleted key; expects `WT_NOTFOUND`.
- **Components:** `src/cursor/cur_modify.c`

### `test_cursor12.test_modify_abort`
- **What it tests:** Modify within an aborted transaction; verifies value reverts to pre-modify state.
- **Components:** `src/cursor/cur_modify.c`, `src/txn/`
