# test_prepare20 — Application-level log replay of unstable prepared transactions after crash

**File:** `test/suite/test_prepare20.py`
**Storage mode:** General (log enabled)
**Components under test:** prepared transactions, crash recovery, application-level log, timestamps

## Test Cases

### `test_prepare20.test_prepare20`
- **What it tests:** Simulates a crash and recovery using an application-level log; writes two prepared transactions with full logging (BEGIN/WRITE/PREPARETIME/PREPARE/COMMITTIME/DURABLETIME/COMMIT opcodes to a separate WiredTiger table); simulates a crash via `simulate_crash_restart`; replays the application log to recover unstable prepared updates; verifies all data is correct at multiple timestamps after replay
- **Components:** `txn/txn_prepare.c`, `log/log.c`, `conn/conn_log.c`, `checkpoint/checkpoint.c`
- **Notes:** Scenarios: integer-row/column × 10 checkpoint timing variations (various stable_timestamps for first and second checkpoint, or none) × commit/nocommit before crash; `conn_config = 'log=(enabled),transaction_sync=(enabled=true,method=none)'`; the application log is a second WiredTiger table (key_format=r, value_format=ii{value_fmt}{value_fmt}); log replay correctly handles the case where durable_ts <= stable_ts by bumping durable_ts to stable_ts+1; verifies `log_replays` count matches expected based on which checkpoints were taken (already-durable data does not need replaying)
