# test_rollback_to_stable40 — RTS with complex HS history and globally-visible update resetting time window

**File:** `test/suite/test_rollback_to_stable40.py`
**Storage mode:** General
**Components under test:** rollback_to_stable, history store, eviction, time window, crash recovery

## Test Cases

### `test_rollback_to_stable40.test_rollback_to_stable`
- **What it tests:** Verifies RTS correctly handles a 3-key table where key 2 has an extensive HS history and its time window is reset by a globally-visible eviction. Keys 1/3 get value_a@20 and value_d@1000. Key 2 gets value_a@20 then 479 updates at ts=21..499 (value_b+str(i)), then checkpointed. Key 2 then gets value_c@500. Stable+oldest set to 500. Key 2 is evicted with `release_evict` under `ignore_prepare=true` (makes it globally visible, resetting time window). Key 2 gets value_d@501 and another checkpoint. Crash-restart. Post-crash: keys 1/3 show value_a; key 2 shows value_c. Stats: `hs_removed >= 3`, `pages_visited > 0`, `keys_removed=0`, `keys_restored=0`, `upd_aborted>=0`.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/history/`, `src/evict/`, `src/checkpoint/`
- **Notes:** Parametrized on key_format (column/row_integer). `cache_size=1MB`, `log=(enabled=true)`. The globally-visible eviction clears the time window, testing that RTS can still correctly determine what to roll back.
