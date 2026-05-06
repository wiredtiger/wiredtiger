# test_update_obsolete_short_chain — Regression test for short update chains in __wt_update_serial

**File:** `test/suite/test_update_obsolete_short_chain.py`
**Storage mode:** General
**Components under test:** update chain obsolete cleanup, `cache_obsolete_updates_removed` stat, `__wt_update_serial`

## Test Cases

### `test_update_obsolete_short_chain.test_short_chain_no_prune_then_prune`
- **What it tests:** Inserts value-a at ts=10 and pins oldest=stable=10; inserts value-b at ts=20 and pins oldest=stable=20; verifies `cache_obsolete_updates_removed` has NOT increased (chain length 2 is too short to prune); inserts value-c at ts=30 and pins oldest=stable=30; checkpoints; verifies `cache_obsolete_updates_removed` has increased (chain length >= 3 triggers obsolete cleanup during reconciliation); reads the key and verifies it shows value-c.
- **Components:** `txn.c`, `reconcile.c`, `update.c`
- **Notes:** No parameterization. Integer-row format, logging disabled (to allow timestamped updates). Regression test for a bug in `__wt_update_serial` where short update chains (length < 3) were incorrectly pruned or not pruned at the right time. `statistics=(all)` required to check `cache_obsolete_updates_removed`.
