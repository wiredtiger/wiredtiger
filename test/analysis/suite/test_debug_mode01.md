# test_debug_mode01 — Tests debug_mode rollback_error simulated conflict injection

**File:** `test/suite/test_debug_mode01.py`
**Storage mode:** General
**Components under test:** debug mode, transaction rollback, conflict simulation

## Test Cases

### `test_debug_mode01.test_rollback_error`
- **What it tests:** Verifies that with `debug_mode=(rollback_error=5)` a statistically significant number of insert and update operations return `WT_ROLLBACK` (simulated conflict) across 22 entries. Checks that the rollback count meets a minimum threshold (`entries // 5`).
- **Components:** `src/txn/`, `src/conn/conn_debug.c`
- **Notes:** Uses `assertRaisesException` with `True` for optional-raise mode. Tests both `cursor.insert()` and `cursor.update()` paths. Minimum expected rollbacks = 4.

### `test_debug_mode01.test_rollback_error_off`
- **What it tests:** Verifies that reconfiguring `debug_mode=(rollback_error=0)` via `conn.reconfigure()` completely disables the simulated rollback errors, resulting in zero rollbacks across the same workload.
- **Components:** `src/txn/`, `src/conn/conn_debug.c`
- **Notes:** Reconfigure is applied after initial connection is opened with `rollback_error=5`.
