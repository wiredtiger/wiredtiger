# test_prepare_discover02 — prepared_discover cursor: find and claim a prepared transaction, then commit

**File:** `test/suite/test_prepare_discover02.py`
**Storage mode:** General (`precise_checkpoint=true,preserve_prepared=true`)
**Components under test:** prepared transactions, prepared_discover cursor, backup, claim_prepared_id, commit

## Test Cases

### `test_prepare_discover02.test_prepare_discover02`
- **What it tests:** Same pattern as test_prepare_discover01 but commits the claimed prepared transaction; verifies that the prepared keys are visible at the correct timestamp (ts=200) after commit, and not visible at an earlier timestamp (ts=60) that precedes the commit
- **Components:** `txn/txn_prepare.c`, `cursor/cur_prepare_discover.c`, `backup/backup.c`, `checkpoint/checkpoint.c`
- **Notes:** `conn_config = 'precise_checkpoint=true,preserve_prepared=true'`; no scenarios; the commit path of `claim_prepared_id` verifies that the data previously prepared and checkpointed can be successfully committed by a new session after recovery; covers the commit path that test_prepare_discover01 omits
