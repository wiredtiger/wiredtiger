# test_timestamp06 — Multi-step transactions with timestamps

**File:** `test/suite/test_timestamp06.py`
**Storage mode:** General
**Components under test:** multi-step timestamped transactions, checkpoint, rollback_to_stable, logged vs. non-logged

## Test Cases

### `test_timestamp06.test_timestamp06`
- **What it tests:** In a single transaction, makes three writes for all 100 keys at commit timestamps 1, 101, and 201 (using `timestamp_transaction` mid-transaction); commits at timestamp 301. Verifies reads at stable_timestamp=200 see value=2 for non-logged and value=3 for logged. Takes a checkpoint at stable=200 and verifies via backup that logged table always has value=3 while non-logged table only has value=3 if `use_timestamp=false`. Runs `rollback_to_stable` and re-verifies. Also confirms non-timestamped read sees final value=3 in logged, value=2 in non-logged.
- **Components:** `txn_timestamp.c`, `txn.c`, `checkpoint.c`, `txn_rollback_to_stable.c`, `log.c`
- **Notes:** Parameterized over 2 connection log configs (V1, V2) × 2 table types (row, VLCS) × 3 checkpoint configs. Tests multi-step commit timestamps within a single transaction.
