# test_rollback_to_stable28 — RTS with debug_mode update_restore_evict during recovery

**File:** `test/suite/test_rollback_to_stable28.py`
**Storage mode:** General
**Components under test:** rollback_to_stable, eviction, recovery, write generation, debug_mode

## Test Cases

### `test_rollback_to_stable28.test_update_restore_evict_recovery`
- **What it tests:** Verifies that `debug_mode=(update_restore_evict=true)` during recovery correctly sets write generation numbers on pages, preventing stale transaction IDs from being read. Writes value_a/b/c at ts=20/30/40, sets stable=40, writes value_d/a/b at ts=50/60/70 (past stable), checkpoints. Verifies pre-crash: `run_write_gen=1`, `write_gen > 1`. Crash-restart with `update_restore_evict=true`. Post-restart verifies `run_write_gen > checkpoint_write_gen` and `write_gen > run_write_gen`. Checks `cache_write_restore_scrub > 0`. Data check: value_c visible at all ts>=40.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/evict/`, `src/btree/`, `src/checkpoint/`
- **Notes:** Skipped for tiered (`@wttest.skip_for_hook("tiered", ...)`). Row store only (VLCS commented out as it doesn't reliably trigger update_restore eviction). 10,000 rows, values are 500-char strings. Recovery conn config adds `cache_size=1MB,eviction_dirty_trigger=5,eviction_dirty_target=1,eviction_updates_trigger=10`. Parses `write_gen` and `run_write_gen` from metadata using regex.
