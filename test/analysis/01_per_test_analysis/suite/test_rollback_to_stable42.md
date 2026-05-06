# test_rollback_to_stable42 — RTS on missing file skips with diagnostic message

**File:** `test/suite/test_rollback_to_stable42.py`
**Storage mode:** General
**Components under test:** rollback_to_stable, missing file handling, crash recovery

## Test Cases

### `test_rollback_to_stable42.test_reopen_after_delete`
- **What it tests:** Verifies that when a table file is deleted from disk between checkpoint and recovery, RTS emits "skipped performing rollback to stable" in the log and gracefully continues rather than crashing. Creates a table, writes value@60 (past stable=40), checkpoints, then `os.remove`s the `.wt` file. Uses `simulate_crash_restart` to trigger recovery RTS. A `custom_validator` function checks that only acceptable log messages appear and that the "skipped performing rollback to stable" needle is present.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/log/`
- **Notes:** Skipped for tiered and Windows. Parametrized on key_format (column/row_integer). `verbose=(rts:1)`. Uses `customStdoutPattern(custom_validator)`. The custom validator checks each log line against an allowlist of acceptable message substrings and requires the needle to appear.
