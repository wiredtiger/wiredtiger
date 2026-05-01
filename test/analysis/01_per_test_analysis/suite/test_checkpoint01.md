# test_checkpoint01 — Named checkpoint lifecycle and cursor API correctness

**File:** `test/suite/test_checkpoint01.py`
**Storage mode:** General
**Components under test:** checkpoint subsystem, checkpoint cursor API, named checkpoints, metadata

## Test Cases

### `test_checkpoint.test_checkpoint`
- **What it tests:** Verifies the full named-checkpoint lifecycle: create, verify existence in metadata, open a cursor, verify data, drop, verify gone. Also tests that creating a checkpoint with a duplicate name overwrites the old one.
- **Components:** `src/checkpoint/`, `src/session/session_api.c`, `src/cursor/cur_stat.c`
- **Notes:** Uses `metadata:` cursor to confirm checkpoint presence. Tests multiple named and anonymous checkpoints. Verifies that `session.checkpoint("drop=(all)")` removes all non-WiredTiger checkpoints.

### `test_checkpoint_cursor.test_checkpoint_cursor_dne`
- **What it tests:** Verifies that opening a cursor to a checkpoint that does not exist raises `WT_NOTFOUND`.
- **Components:** `src/session/session_api.c`
- **Notes:** Attempts to open `checkpoint=nonexistent_name` and expects error.

### `test_checkpoint_cursor.test_checkpoint_cursor_multiple`
- **What it tests:** Verifies that multiple cursors can be opened on the same checkpoint simultaneously.
- **Components:** `src/session/session_api.c`, `src/btree/`
- **Notes:** Opens two cursors to the same named checkpoint and reads from both.

### `test_checkpoint_cursor.test_checkpoint_cursor_inuse`
- **What it tests:** Verifies that dropping a checkpoint while a cursor is open on it raises `EBUSY`.
- **Components:** `src/checkpoint/`, `src/session/session_api.c`
- **Notes:** Opens cursor then calls `session.checkpoint("drop=(name)")` and expects EBUSY.

### `test_checkpoint_target.test_checkpoint_target`
- **What it tests:** Verifies `checkpoint(target=[uri])` only checkpoints the specified table, leaving others unmodified in the checkpoint.
- **Components:** `src/checkpoint/`
- **Notes:** Creates two tables, writes to both, runs targeted checkpoint on one, checks only that table is updated.

### `test_checkpoint_cursor_update.test_checkpoint_cursor_update`
- **What it tests:** Verifies that cursors opened on a checkpoint are read-only; insert/update/remove operations raise an error.
- **Components:** `src/cursor/cur_btree.c`
- **Notes:** Opens checkpoint cursor and attempts insert, update, and remove — all must fail with `WT_ROLLBACK` or similar.

### `test_checkpoint_last.test_checkpoint_last`
- **What it tests:** Verifies the `checkpoint=WiredTigerCheckpoint` specifier always refers to the most recent checkpoint.
- **Components:** `src/session/session_api.c`
- **Notes:** Creates multiple checkpoints; verifies the `WiredTigerCheckpoint` cursor reads data from the last one.

### `test_checkpoint_illegal_name.test_checkpoint_illegal_name`
- **What it tests:** Verifies that checkpoint names starting with `WiredTiger` are rejected as reserved names.
- **Components:** `src/checkpoint/`
- **Notes:** Attempts `session.checkpoint("name=WiredTigerFoo")` and expects `EINVAL`.

### `test_checkpoint_empty.test_checkpoint_empty_*` (7 methods)
- **What it tests:** Seven methods covering various scenarios of checkpointing empty files: newly-created empty table, table populated then all rows deleted, table with only schema metadata, etc.
- **Components:** `src/checkpoint/`, `src/btree/`
- **Notes:** Verifies that checkpointing empty/zero-row tables does not error out and that subsequent cursor reads return `WT_NOTFOUND`.
