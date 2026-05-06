# test_checkpoint20 — Prepared data in checkpoint with ignore_prepare semantics

**File:** `test/suite/test_checkpoint20.py`
**Storage mode:** General
**Components under test:** checkpoint cursor, prepared transactions, ignore_prepare flag

## Test Cases

### `test_checkpoint.test_checkpoint`
- **What it tests:** Verifies that a checkpoint taken while a prepared (not yet committed) transaction is active captures the pre-prepared value, and that reading from the checkpoint cursor with `ignore_prepare=true` always returns the pre-prepared value (`value_a`) regardless of subsequent commit.
- **Components:** `src/checkpoint/`, `src/txn/txn_prepare.c`, `src/cursor/cur_btree.c`
- **Notes:** Transaction is prepared at ts=20 with value_b, but the checkpoint is taken at stable=10 (before prepare). Checkpoint cursor opened with `ignore_prepare=true` must return `value_a` (ts=10). With `ignore_prepare=false`, encountering the prepared key raises `WT_PREPARE_CONFLICT`. Tests the `ignore_prepare` interaction with checkpoint cursor visibility.
