# test_prepare_discover04 — prepared_discover cursor with prepared delete: claim and commit/rollback

**File:** `test/suite/test_prepare_discover04.py`
**Storage mode:** General (`precise_checkpoint=true,preserve_prepared=true`)
**Components under test:** prepared transactions, prepared_discover cursor, tombstones, claim_prepared_id, commit/rollback

## Test Cases

### `test_prepare_discover04.test_prepare_discover04`
- **What it tests:** Inserts keys and prepares a delete (tombstone) on them with `prepared_id`; advances stable past prepare_ts and checkpoints; opens the backup DB, discovers the prepared transaction via `prepared_discover:` cursor, claims it, then either commits (key not found after commit) or rolls back (original value visible after rollback); verifies no crash occurs
- **Components:** `txn/txn_prepare.c`, `btree/bt_delete.c`, `cursor/cur_prepare_discover.c`, `backup/backup.c`, `checkpoint/checkpoint.c`
- **Notes:** Scenarios: commit/rollback; tests the delete (tombstone) scenario through the discover+claim workflow; the commit path results in the key being deleted; the rollback path restores the prior value; verifies that the checkpoint after claim+commit/rollback completes without error
