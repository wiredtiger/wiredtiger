# model_rts — Rollback-to-Stable and Crash/Restart Recovery

## Overview

`model_rts` verifies the rollback-to-stable (RTS) operation and database recovery semantics
(both clean restart and unclean crash). Scenarios are tested first in the model alone, then
cross-checked against a live WiredTiger instance. Crash scenarios use `in_subprocess_abort`
(the child process aborts without a clean shutdown) and verify that the reopened database matches
the model's post-crash state.

The test also validates that RTS correctly handles edge cases: RTS before any stable timestamp is
set, RTS with concurrent open or prepared transactions (which must be rejected), and that after
RTS new writes at timestamps below the new stable are legal.

Source: `/data/work/git/wiredtiger4/test/model/test/model_rts/main.cpp`

---

## Test Cases / Scenarios

### test_rts
- **What it verifies:**
  - RTS before `set_stable_timestamp` is a no-op (data committed at ts=5 is retained).
  - After `set_stable_timestamp(15)` and RTS: data at ts=10 survives, data at ts=20 is removed.
  - After RTS, a new write at ts=18 (below the previous stable) is legal.
  - RTS with a concurrent open transaction is illegal (`model_exception` raised).
  - RTS with a concurrent prepared (but uncommitted) transaction is illegal
    (`model_exception` raised).
- **Model components:** `kv_database::rollback_to_stable`, `kv_database::set_stable_timestamp`,
  `kv_transaction` (prepared).
- **Notes:** Model-only.

### test_rts_wt
- **What it verifies:** Same RTS sequence as `test_rts` cross-checked against WiredTiger.
  `wt_model_rollback_to_stable_both` performs RTS on both the model and live WT and then asserts
  that every key's read result matches. Debug-log replay verified after close.
- **Model components:** `kv_database`, `debug_log_parser`, model verifier.
- **Notes:** Row-store, `log=(enabled=false)`. Two extra sessions opened for concurrent
  transaction scenarios.

### test_rts_crash_wt
- **What it verifies:** RTS followed by a simulated crash:
  - Data is written at ts=10, 20, 30; stable is set to 20; RTS is called; then the process
    crashes (`database.crash()` in model, `in_subprocess_abort` in WT).
  - After crash and reopen: stable timestamp is preserved (20), and only the ts=10, 20 data
    survives (ts=30 rolled back). The model confirms `table->get(key1) == value2`.
  - Post-restart: a new write at ts=25 (between old stable=20 and the rolled-back ts=30) is legal.
  - Debug-log replay verified.
- **Model components:** `kv_database::crash`, `kv_database::rollback_to_stable`,
  `debug_log_parser`.
- **Notes:** Uses `in_subprocess_abort` to simulate an unclean crash; WiredTiger re-opens the
  directory and applies recovery (RTS at open).

### test_restart_wt1 — No explicit checkpoint
- **What it verifies:** Clean restart without an explicit checkpoint:
  - Data at ts=10 and ts=20 written; stable set to 15; clean close/reopen.
  - After restart: stable is preserved at 15, data at ts=10 is present, data at ts=20 is absent
    (WiredTiger applies RTS at open when stable is set but no on-disk checkpoint exists for ts=20).
  - Post-restart write at ts=18 is legal.
  - Debug-log replay verified.
- **Model components:** `kv_database::restart` (no crash), `debug_log_parser`.
- **Notes:** Uses `in_subprocess` (clean close, not abort).

### test_restart_wt2 — With an explicit checkpoint
- **What it verifies:** Clean restart after an explicit unnamed checkpoint at stable=15:
  - Same data sequence as scenario 1; an unnamed checkpoint is taken before close.
  - After restart: same state as scenario 1 (only ts=10 data visible), plus post-restart write
    at ts=18.
  - Debug-log replay verified.
- **Model components:** `kv_database::create_checkpoint`, `kv_database::restart`.
- **Notes:** Confirms that an explicit checkpoint at stable=15 and a subsequent clean restart
  produce the same observable state as restarting without a checkpoint.

### test_restart_wt3 — Exit with active transactions
- **What it verifies:** Clean restart while a regular transaction and a prepared transaction
  are still open at checkpoint time:
  - Active non-prepared transaction (key3, key4) and a prepared transaction (key5 at prepare_ts=14)
    are both present when a checkpoint at stable=15 is taken, then the process exits cleanly.
  - After restart: data at ts=10 survives, data at ts=20, key3, key4, key5 are all absent
    (the active and prepared transactions were not committed; they are cleaned up by RTS at open).
  - Post-restart write at ts=18 is legal.
  - Debug-log replay verified.
- **Model components:** `kv_database::create_checkpoint`, `kv_transaction` (prepared),
  `kv_database::restart`.
- **Notes:** Validates that uncommitted and prepared-but-uncommitted transactions at checkpoint
  time are correctly excluded from the recovered state.

### test_crash_wt1 — No checkpoint
- **What it verifies:** Crash scenario with no checkpoint at all:
  - Data written at ts=10, ts=20; stable set to 15; process crashes immediately.
  - After crash/reopen: stable is absent (`k_timestamp_none`), and no data survives (both rows
    are absent because there was no checkpoint to recover from).
  - Debug-log replay verified.
- **Model components:** `kv_database::crash`, `kv_database::set_stable_timestamp`.
- **Notes:** `in_subprocess_abort` used for the WiredTiger side.

### test_crash_wt2 — Basic RTS on crash
- **What it verifies:** Crash after a checkpoint at stable=15, then stable advanced to 25 before
  crash:
  - A checkpoint captures ts=10 data (stable=15). Stable is then advanced to 25 but no new
    checkpoint is taken. Process crashes.
  - After crash/reopen: stable reverts to the last checkpointed stable (15); ts=10 data is
    present, ts=20 data is absent (RTS rolled it back at open).
  - Post-restart write at ts=18 is legal.
  - Debug-log replay verified.
- **Model components:** `kv_database::crash`, `kv_database::create_checkpoint`,
  `kv_database::set_stable_timestamp`.
- **Notes:** Demonstrates that the stable timestamp at crash reverts to what was in the
  checkpoint, not what was set in memory after the checkpoint.

### test_crash_wt3 — RTS with active and prepared transactions
- **What it verifies:** Crash with both an active regular transaction and a prepared transaction
  open at checkpoint time:
  - Data at ts=10, ts=20; active txn (key3, key4) and prepared txn (key5, prepare_ts=14);
    checkpoint at stable=15; crash.
  - After crash/reopen: only ts=10 data survives; ts=20, key3, key4, key5 are all absent.
  - Post-restart write at ts=18 is legal.
  - Debug-log replay verified.
- **Model components:** `kv_database::crash`, `kv_transaction` (prepared),
  `kv_database::create_checkpoint`.
- **Notes:** Mirrors `test_restart_wt3` but with an unclean crash instead of a clean exit.
