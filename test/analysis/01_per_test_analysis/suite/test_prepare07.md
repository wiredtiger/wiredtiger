# test_prepare07 — Prepared transaction non-durability in backup when oldest advances

**File:** `test/suite/test_prepare07.py`
**Storage mode:** General
**Components under test:** prepared transactions, backup, oldest_timestamp, visibility

## Test Cases

### `test_prepare07.test_older_prepare_updates`
- **What it tests:** Leaves a prepared transaction open while advancing `oldest_timestamp` past the prepare_timestamp; takes a checkpoint; takes a backup; commits the prepared transaction; verifies that the prepared update is not visible in the backup (non-durable at checkpoint time), while other committed updates written before the checkpoint are visible in the backup
- **Components:** `txn/txn_prepare.c`, `txn/txn_timestamp.c`, `backup/backup.c`, `block/block_ckpt.c`
- **Notes:** No scenarios; key scenario: oldest_ts advancing past prepare_ts is legal; demonstrates that prepared updates are not considered durable at checkpoint time even if oldest has passed them; backup contains only the checkpoint snapshot, which excludes the prepared data; verifies that committed non-prepared updates at the same timestamp range are visible in backup
