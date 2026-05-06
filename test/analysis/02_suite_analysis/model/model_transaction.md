# model_transaction — Transaction Isolation, Conflicts, and Prepared Transactions

## Overview

`model_transaction` verifies the full transaction model: snapshot isolation semantics, read
timestamps, write-write conflict detection, per-operation commit timestamps (`set_commit_timestamp`),
snapshot resets, transaction rollback, prepared transactions (including prepare conflicts visible
to concurrent readers), and the special semantics for logged tables (where timestamps are ignored).
All scenarios are tested in the model and then cross-validated against live WiredTiger.

Source: `/data/work/git/wiredtiger4/test/model/test/model_transaction/main.cpp`

---

## Test Cases / Scenarios

### test_transaction_basic
- **What it verifies:**
  - Two concurrent transactions can each see their own writes but not each other's before commit.
  - Read timestamps: a transaction with read_ts=5 does not see data committed at ts=10; one with
    read_ts=10 does.
  - Write-write conflicts: concurrent update to the same key triggers `WT_ROLLBACK` on the second
    writer; the first writer is unaffected.
  - Conflict due to snapshot staleness: if a concurrent transaction commits between txn2's snapshot
    and its write, the write fails with `WT_ROLLBACK`.
  - Per-operation commit timestamps (`set_commit_timestamp`): different keys in the same
    transaction can have different effective commit timestamps; reads at each timestamp return the
    correct value. Reads just before the timestamp return the previous value.
  - Timestamp ordering enforcement within a key: writing a key at ts=52, then at ts=55, then
    trying to write it at ts=53 (going backward for the same key) causes `wiredtiger_abort_exception`
    at the model level (matching WT's behaviour of aborting the transaction at commit or
    reconciliation).
  - Transaction rollback: rolled-back changes are not visible.
  - Snapshot reset (`txn->reset_snapshot()`): after reset, the transaction can see commits that
    happened after its original snapshot, and can then update the same key without a conflict.
- **Model components:** `kv_database::begin_transaction`, `kv_transaction` (commit, rollback,
  set_commit_timestamp, reset_snapshot), `kv_table` (insert, get, get_ext).
- **Notes:** Model-only.

### test_transaction_basic_wt
- **What it verifies:** Same scenarios as `test_transaction_basic` cross-checked against
  WiredTiger. Note: the backward-timestamp abort case (writing a key at a decreasing timestamp)
  is commented out for the WiredTiger side when it would cause a fatal abort that the test
  framework cannot recover from; instead the test verifies the observable result. Debug-log replay
  verified.
- **Model components:** `kv_database`, `kv_transaction`, `debug_log_parser`, model verifier.
- **Notes:** Row-store, `log=(enabled=false)`. Uses two concurrent sessions.

### test_transaction_column_wt
- **What it verifies:** Identical scenarios to `test_transaction_basic_wt` but for a column-store
  table (`key_format=r`). Verifies that snapshot isolation, read timestamps, write-write conflicts,
  per-key commit timestamps, rollback, and snapshot reset all behave identically for record-number
  keys. Debug-log replay verified.
- **Model components:** `kv_database`, `kv_table` (column type), `debug_log_parser`.
- **Notes:** Column-store, `key_format=r,value_format=S`, `log=(enabled=false)`.

### test_transaction_prepared
- **What it verifies:**
  - Two concurrent prepared transactions: each sees its own writes; after prepare and commit with
    different durable timestamps, both keys are visible at the appropriate timestamps.
  - Write-write conflict with a prepared transaction: the second writer sees `WT_ROLLBACK` even
    after the first transaction is prepared (and still uncommitted).
  - Conflict after prepared commit: a transaction that starts before the prepared commit and
    tries to update the same key receives `WT_ROLLBACK`.
  - Prepare conflict: a global read of a key that has a pending prepare returns
    `WT_PREPARE_CONFLICT`; a read with a timestamp before the prepare timestamp succeeds and
    returns the previous value.
  - Prepared-then-rolled-back transaction: reads after rollback return the value before the
    prepare.
  - Overlapping transactions with prepare — three read behaviors tested:
    1. txn2 begins before txn1 is prepared: txn2 cannot see txn1's key (it was not yet committed
       when txn2's snapshot was taken).
    2. txn2 begins after txn1 is prepared: txn2 sees `WT_PREPARE_CONFLICT`.
    3. txn2 begins after txn1 is prepared, txn1 commits before txn2 reads: txn2 can see txn1's
       committed value (prepare conflict resolved).
- **Model components:** `kv_transaction` (prepare, commit with durable_ts, rollback),
  `kv_table::get_ext`.
- **Notes:** Model-only.

### test_transaction_prepared_wt
- **What it verifies:** Same prepared-transaction scenarios as `test_transaction_prepared`
  cross-checked against WiredTiger. All three read behaviours with concurrent prepare are verified
  in WiredTiger. Debug-log replay verified.
- **Model components:** `kv_database`, `kv_transaction` (prepared), `debug_log_parser`.
- **Notes:** Row-store, `log=(enabled=false)`. Uses two concurrent sessions.

### test_transaction_logged
- **What it verifies:** Transaction model for logged tables (`log_enabled=true`):
  - Two concurrent transactions: snapshot isolation is still enforced (each sees only its own
    writes; conflicts still trigger `WT_ROLLBACK`).
  - Read timestamps are *ignored* for logged tables: a transaction with read_ts=5 can see data
    committed at ts=10.
  - Per-operation `set_commit_timestamp` is also ignored: keys committed with
    `set_commit_timestamp(42)` are visible even at timestamp 41 (pre-commit reads return the
    current value for logged tables).
  - Timestamp ordering within the same key is not enforced for logged tables (backward writes do
    not cause abort).
  - Rollback still works correctly.
  - Snapshot reset works correctly.
  - Prepare is not supported for logged tables (`wiredtiger_exception` raised).
- **Model components:** `kv_table` (logged), `kv_transaction`.
- **Notes:** Model-only. Key difference from non-logged: all timestamp-based read filtering
  is bypassed.

### test_transaction_logged_wt
- **What it verifies:** Same logged-table transaction scenarios cross-checked against WiredTiger
  (`log=(enabled=true)`). Debug-log replay verified.
- **Model components:** `kv_database`, `kv_table` (logged), `debug_log_parser`.
- **Notes:** Row-store, `log=(enabled=true)`. Uses two concurrent sessions.
