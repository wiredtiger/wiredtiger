# test_rollback_to_stable32 — RTS with update restore eviction removes on-disk tombstone

**File:** `test/suite/test_rollback_to_stable32.py`
**Storage mode:** General
**Components under test:** rollback_to_stable, eviction, update restore eviction, tombstone, reconciliation

## Test Cases

### `test_rollback_to_stable32.test_rollback_to_stable_with_update_restore_evict`
- **What it tests:** Regression test verifying that update restore eviction correctly removes an on-disk tombstone after RTS (previously triggered an assertion in reconciliation). Writes value_a@20, value_b@30, removes@40. Sets stable=40 (non-prepare) or stable=50 (prepare). Writes value_c@60 and checkpoints. Evicts pages. Calls RTS. Then disables eviction (`debug_mode=(eviction=false)`), attempts a rolled-back update, writes value_c@60 again and evicts. Verifies value_b visible at ts=30 after second eviction.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/evict/`, `src/btree/`, `src/reconcile/`
- **Notes:** Parametrized on key_format (column/row_integer), prepare, worker threads (0/4/8). `cache_size=100MB`, `split_pct=50`. The key scenario: RTS converts a remove (tombstone) back to value_b, then update restore eviction must correctly handle the now-absent tombstone.
