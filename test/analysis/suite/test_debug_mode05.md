# test_debug_mode05 — Regression test for WT-5046: table_logging must not break rollback_to_stable with prepared transactions

**File:** `test/suite/test_debug_mode05.py`
**Storage mode:** General
**Components under test:** debug mode, table logging, prepared transactions, rollback_to_stable

## Test Cases

### `test_debug_mode05.test_table_logging_rollback_to_stable`
- **What it tests:** Regression for WT-5046 where `debug_mode=(table_logging=true)` caused `rollback_to_stable` to fail when the last transaction before the call was an empty prepared transaction (no log records written before commit). Verifies that:
  1. A normal prepared transaction (prepare=150, commit=200, durable=250) commits and rollback_to_stable succeeds.
  2. An empty prepared transaction (no operations, prepare=300, commit=350, durable=400) commits and rollback_to_stable succeeds.
  3. A regular commit-timestamp transaction (commit=450) can follow.
  4. A final rollback_to_stable succeeds.
- **Components:** `src/txn/`, `src/log/`, `src/conn/conn_debug.c`, `src/rollback_to_stable/`
- **Notes:** Table is created with `log=(enabled=false)`. Stable timestamp is set to 100 before the first checkpoint. The bug was caused by a stale transaction ID left in global state from an empty prepared transaction, which made RTS think a transaction was still running. Timestamps used: stable=100, prepare/commit/durable at 150/200/250 and 300/350/400.
