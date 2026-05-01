# model_checkpoint — Checkpoint Visibility and Persistence

## Overview

`model_checkpoint` verifies that checkpoints correctly capture a consistent snapshot of the
database at the stable timestamp, that named and unnamed checkpoints can be read independently
with the right visibility (committed vs. stable), that prepared transactions interact correctly
with checkpoints, and that checkpoint state survives database restarts. All scenarios are checked
both in the model alone and cross-validated against a live WiredTiger instance, with optional
debug-log replay verification.

Source: `/data/work/git/wiredtiger4/test/model/test/model_checkpoint/main.cpp`

---

## Test Cases / Scenarios

### test_checkpoint
- **What it verifies:**
  - Named checkpoint (`ckpt1`) taken before the stable timestamp is set captures all committed
    data at the time (including data whose commit timestamp > stable, because stable was not yet
    set).
  - Unnamed checkpoint taken after `set_stable_timestamp(15)` captures only data at or before
    ts=15, excluding the commit-ts=20 row.
  - Reading a named checkpoint with a read timestamp (`get(ckpt1, key, 15)`) respects the
    timestamp even within the checkpoint.
  - Checkpoint taken while a transaction (`txn2`) is still open does not include that
    transaction's writes.
  - Prepared transactions: a commit with `durable_ts=60` is included in a checkpoint at
    `stable=60`; a commit with `durable_ts=65` is excluded from that checkpoint.
  - Moving stable backward (`set_stable_timestamp(50)`) fails; the checkpoint after that still
    reflects the previous stable.
  - Illegal prepare/commit sequences (`prepare` at stable timestamp, commit with commit_ts <
    prepare_ts, `set_commit_timestamp` after prepare) raise `wiredtiger_abort_exception`.
  - A prepared-but-uncommitted transaction present when a checkpoint is taken: after a crash
    restart, that key is absent from the checkpoint.
- **Model components:** `kv_database::create_checkpoint`, `kv_checkpoint`, `kv_transaction`
  (prepare/commit), `kv_database::set_stable_timestamp`, `kv_database::restart`,
  `kv_database::crash`.
- **Notes:** Model-only; the crash/prepared scenario notes that it cannot be reproduced with a
  live WiredTiger connection (WT aborts the process if a prepared txn is open at close).

### test_checkpoint_wt
- **What it verifies:** The same checkpoint sequence as `test_checkpoint` cross-checked against
  WiredTiger using `wt_model_ckpt_assert`. After the main sequence, the database is reopened and
  both the binary debug log and a JSON-printed debug log are used to reconstruct the model, which
  is then verified against live WiredTiger for both the current state and for each of the four
  named checkpoints (`ckpt1`–`ckpt4`).
- **Model components:** `kv_database`, `kv_checkpoint`, `debug_log_parser::from_debug_log`,
  `debug_log_parser::from_json`, model verifier.
- **Notes:** Row-store, `log=(enabled=false)`. Uses two concurrent sessions (session1, session2).

### test_checkpoint_restart_wt
- **What it verifies:** Multi-restart checkpoint scenario across three database open/close cycles:
  1. First open: create data at ts=10,20, checkpoint at stable=15 (ckpt1), more data at ts=30,40,
     checkpoint at stable=35 (ckpt2), unnamed checkpoint at stable=40, then close and reopen.
  2. Second open (post-restart): add data at ts=50,60, checkpoint at stable=55 (ckpt3); take
     ckpt4 while a transaction (ts=80) is still open; add data at ts=80, unnamed checkpoint at
     stable=80, then close and reopen.
  3. Third open (post-restart): add data with prepared transactions (prepare/commit at
     ts=90/94/98, 100/104/108), checkpoint at stable=95 (ckpt5), more prepared data
     (ts=110/114/118, 120/124/128), checkpoint at stable=115 (ckpt6), unnamed checkpoint at
     stable=129, then close and reopen.
  4. Final verification: all six named checkpoints are verified against live WiredTiger using the
     debug log.
- **Model components:** `kv_database` with multiple restarts, `kv_checkpoint` (six named),
  `kv_transaction` (prepared), `debug_log_parser`, model verifier.
- **Notes:** This is the most comprehensive checkpoint scenario; tests that checkpoint visibility
  is preserved across restarts and that prepared transactions interact correctly with checkpoints
  at boundaries.

### test_checkpoint_logged
- **What it verifies:** Checkpoint semantics for logged tables (`log_enabled=true`):
  - A named checkpoint captures all committed data at the time regardless of stable timestamp
    (logged tables ignore timestamp ordering).
  - An unnamed checkpoint taken with `stable=15` still captures all committed logged data
    (including the row committed without a timestamp), not just data up to stable.
  - Reading a checkpoint with a read timestamp for a logged table still returns the committed
    value (timestamps are ignored for reads on logged tables).
  - `contains_any` on a checkpoint reflects whether a value was visible at the checkpoint time.
- **Model components:** `kv_table` (logged), `kv_checkpoint`, `kv_database::set_stable_timestamp`.
- **Notes:** Model-only; confirms that logged-table checkpoint semantics differ from
  non-logged (stable timestamp does not filter rows).

### test_checkpoint_logged_wt
- **What it verifies:** Same logged-table checkpoint scenarios cross-checked against WiredTiger
  (`log=(enabled=true)`). Two named checkpoints (`ckpt1`, `ckpt2`) are verified against both the
  live connection and debug-log reconstructed databases (binary and JSON). Commits for logged
  tables do not carry timestamps.
- **Model components:** `kv_database`, `kv_table` (logged), `debug_log_parser` (both binary and
  JSON), model verifier.
- **Notes:** Row-store, `log=(enabled=true)`. Both named checkpoints verified via four paths
  (live, debug-log binary, debug-log JSON, checkpoint-specific verify).
