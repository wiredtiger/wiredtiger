# test_durable_ts01 — Durable timestamp: non-durable updates are not visible after restart

**File:** `test/suite/test_durable_ts01.py`
**Storage mode:** General
**Components under test:** durable timestamp, prepared transactions, checkpoint, restart, history store

## Test Cases

### `test_durable_ts01.test_durable_ts01`
- **What it tests:** Verifies that updates whose durable timestamp exceeds the stable timestamp at checkpoint time are not visible after a connection restart (crash-recovery scenario). Full scenario:
  1. Populate 50 rows; checkpoint with `stable_timestamp=100`.
  2. First update (value=111): prepare at ts=150, commit at ts=200, durable at ts=220. First update is durable (durable=220 > stable=100, but stable will be moved to 250 before second update).
  3. Verify reads at ts=150 (sees original), ts=220 (sees 111), and snapshot (sees 111).
  4. Second update (value=222): prepare at ts=230, then set `stable_timestamp=250`, commit at ts=240, durable at ts=300. Durable (300) > stable (250), so second update is NOT durable.
  5. Checkpoint: first update durable, second update only visible.
  6. Verify second update (222) is visible in current session.
  7. Close session and cursor; reopen connection (simulates restart/crash recovery).
  8. Set stable=250, oldest=250; verify all rows show first update (111), confirming non-durable second update was discarded on restart.
- **Components:** `src/txn/txn.c`, `src/checkpoint/`, `src/history/hs.c`, `src/conn/conn_open.c`
- **Notes:** Scenarios: `file`/`table-simple` x `row-string`/`row-int` x `read-committed`/default/`snapshot` isolation (column/recno excluded by `keep` filter). Key timestamp chain: prepare(150) < commit(200) < durable(220) for first update; prepare(230) < commit(240) < stable(250) < durable(300) for second update. Isolation type affects only the transaction snapshot visible during read checks.
