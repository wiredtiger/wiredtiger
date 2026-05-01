# test_rollback_to_stable34 — RTS interaction with fast-delete (truncate)

**File:** `test/suite/test_rollback_to_stable34.py`
**Storage mode:** General
**Components under test:** rollback_to_stable, fast-delete, truncate, crash recovery, prepared transactions

## Test Cases

### `test_rollback_to_stable34.test_rollback_to_stable`
- **What it tests:** Verifies that RTS correctly undoes a fast-delete (truncate) operation committed at ts=35 (beyond stable=25). Writes valuea@20 (baseline) and valueb@30 to 10,000 rows. Evicts. Sets stable=25. Checkpoints. Truncates the upper half of the table (keys nrows/2+1 to nrows) at ts=35, optionally as a prepared txn. Optionally takes a second checkpoint with the truncation. Then calls RTS (runtime) or simulates crash (recovery). Post-RTS: all rows should show valuea at ts=20 and ts=30 (stable=25 means valueb@30 is past stable). Verifies `rec_page_delete_fast > 0` stat.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/btree/`, `src/truncate/`
- **Notes:** Skipped for tiered. Prepare+runtime is skipped (prepare+RTS runtime would fail). Parametrized on key_format (column/row_integer/string_row), prepare, second_checkpoint, crash/runtime, worker threads. `log=(enabled=false)`. Values have per-row suffix to detect range errors.
