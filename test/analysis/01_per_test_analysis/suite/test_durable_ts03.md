# test_durable_ts03 — Checkpoint honors durable timestamp; non-durable updates not persisted

**File:** `test/suite/test_durable_ts03.py`
**Storage mode:** General (skipped for tiered)
**Components under test:** durable timestamp, prepared transactions, checkpoint, history store, restart

## Test Cases

### `test_durable_ts03.test_durable_ts03`
- **What it tests:** Comprehensive multi-phase test verifying that checkpoints only persist updates whose durable timestamp is <= stable timestamp, and that subsequent restarts reflect only durable data. Scenario:
  1. Load 3000 rows with `valueA` at commit_timestamp=50.
  2. Set stable/oldest=100, checkpoint (baseline). All valueA rows are durable.
  3. Update all rows to `valueB`: prepare at ts=150, commit at ts=200, durable at ts=220. Stable remains at 100 so valueB is NOT durable at this checkpoint.
  4. Open checkpoint cursor and verify it shows only `valueA` (checkpoint does not include non-durable valueB).
  5. Read at ts=150: sees valueA (before commit). Read at ts=210: sees valueB (after commit, before checkpoint). Read at ts=220: sees valueB.
  6. Checkpoint with `use_timestamp=true`. After reopen with stable/oldest=210, all rows show `valueA` (non-durable valueB not persisted).
  7. After reopen, update all rows to `valueC`: prepare at ts=220, commit at ts=230, durable at ts=240. Set stable=250, checkpoint.
  8. After second reopen with stable/oldest=250, all rows show `valueC` (valueC is durable since durable=240 <= stable=250).
- **Components:** `src/txn/txn.c`, `src/checkpoint/`, `src/history/hs.c`, `src/conn/conn_open.c`, `src/cursor/cur_std.c`
- **Notes:** Scenarios: `integer-row` (key_format=i) and `column` (key_format=r). Cache size is 10 MB to allow eviction during large updates. Key timestamp invariants checked: durable(220) > stable(100) means not persisted; durable(240) <= stable(250) means persisted. Uses checkpoint cursor (`checkpoint=WiredTigerCheckpoint`) to validate checkpoint snapshot content directly.
