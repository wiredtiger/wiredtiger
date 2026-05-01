# test_bug027 — Snapshot with more than 256 concurrent transactions survives crash

**File:** `test/suite/test_bug027.py`
**Storage mode:** General (logging disabled for table)
**Components under test:** transaction snapshot, crash recovery, rollback to stable

## Test Cases

### `test_bug027.test_bug`
- **What it tests:** Verifies that a checkpoint snapshot correctly handles more than 256 concurrent transactions (historically a limit). Creates 1000 rows (value_a), then opens 500 sessions each with an uncommitted transaction that updates one row to value_b. Commits a single additional transaction (value_c on the last row) and checkpoints. Confirms the reading session sees value_a for all rows except the last (which is value_c) — value_b must be invisible. Then simulates a crash via `simulate_crash_restart` and re-checks: value_b must still be invisible after restart.
- **Components:** `src/txn/txn.c`, `src/txn/txn_ckpt.c`, `src/conn/conn_recover.c`
- **Notes:** Non-parametrized. `session_max=512` to allow 500+ concurrent sessions. Log disabled on the table. Class is named `test_bug` (not `test_bug027`); URI is `table:bug026`.
