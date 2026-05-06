# test_checkpoint12 — Checkpoint cursor read fails when prepared transaction is active

**File:** `test/suite/test_checkpoint12.py`
**Storage mode:** General
**Components under test:** checkpoint cursor, prepared transactions, visibility rules

## Test Cases

### `test_checkpoint.test_checkpoint`
- **What it tests:** Verifies that a checkpoint cursor attempting to read a key that has a prepared-but-not-yet-committed transaction returns an error (`WT_PREPARE_CONFLICT`) or behaves as if the prepared value is invisible, depending on the `ignore_prepare` setting.
- **Components:** `src/cursor/cur_btree.c`, `src/txn/txn_prepare.c`, `src/checkpoint/`
- **Notes:** A transaction is prepared (but not committed) before the checkpoint. A checkpoint is then taken, and a cursor opened on it. The cursor's behavior when encountering the prepared key depends on the `ignore_prepare` flag — with `ignore_prepare=false` it raises `WT_PREPARE_CONFLICT`; with `ignore_prepare=true` it reads the pre-prepared value.
