# test_checkpoint21 — Committed-but-not-durable transaction visibility in checkpoint

**File:** `test/suite/test_checkpoint21.py`
**Storage mode:** General
**Components under test:** checkpoint cursor, prepared transactions, durable timestamp, visibility

## Test Cases

### `test_checkpoint.test_checkpoint`
- **What it tests:** Verifies that a transaction committed with `commit_timestamp < durable_timestamp` is not visible in a checkpoint taken at `stable_timestamp` between the two timestamps. Only the durable timestamp determines whether a prepared transaction's commit is stable.
- **Components:** `src/checkpoint/`, `src/txn/txn_prepare.c`, `src/txn/txn_timestamp.c`, `src/cursor/cur_btree.c`
- **Notes:** Transaction prepared at ts=15, committed at ts=20, durable_ts=30. Checkpoint taken at stable=25 (between commit and durable). The checkpoint cursor must NOT see the committed value because durable_ts=30 > stable_ts=25. Tests the commit_ts vs durable_ts distinction in checkpoint visibility rules.
