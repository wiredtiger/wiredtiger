# test_prepare_discover01 — Basic prepared_discover cursor: find and claim a prepared transaction, then rollback

**File:** `test/suite/test_prepare_discover01.py`
**Storage mode:** General (`precise_checkpoint=true,preserve_prepared=true`)
**Components under test:** prepared transactions, prepared_discover cursor, backup, claim_prepared_id

## Test Cases

### `test_prepare_discover01.test_prepare_discover01`
- **What it tests:** Prepares a transaction with `prepared_id=123`, advances stable past prepare_ts, checkpoints, takes a backup; opens the backup DB, iterates the `prepared_discover:` cursor to find the prepared transaction with id=123; claims it with `begin_transaction("claim_prepared_id=123")`; rolls back the claimed transaction; verifies no data visible after rollback
- **Components:** `txn/txn_prepare.c`, `cursor/cur_prepare_discover.c`, `backup/backup.c`, `checkpoint/checkpoint.c`
- **Notes:** `conn_config = 'precise_checkpoint=true,preserve_prepared=true'`; no scenarios; the `prepared_discover:` cursor returns one row per unresolved prepared transaction with its prepared_id; `claim_prepared_id` in `begin_transaction()` takes ownership of the prepared state so it can be committed or rolled back; this test covers the rollback path
