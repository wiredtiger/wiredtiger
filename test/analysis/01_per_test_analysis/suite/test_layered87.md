# test_layered87 — RTS is skipped at startup but works at runtime in disagg mode

**File:** `test/suite/test_layered87.py`
**Storage mode:** Disagg/Layered
**Components under test:** Rollback-to-stable (RTS) behavior at crash restart and at runtime, disagg startup RTS suppression

## Test Cases

### `test_layered87.test_layered87`
- **What it tests:** Populates 500 keys using `SimpleDataSet`. Commits updates to keys 10, 11, 12 at ts=30. Sets stable_timestamp=20 (lower than the commit ts), checkpoints, then simulates a crash restart. After restart, reads keys 10, 11, 12 and verifies they still have the updated values (ts=30), confirming that startup RTS did NOT roll them back (disagg skips startup RTS). Then calls `conn.rollback_to_stable()` at runtime (explicit call) and verifies keys 10, 11, 12 revert to their original values (from the base dataset at ts≤20), confirming runtime RTS does work.
- **Components:** `src/txn/txn_rollback_to_stable.c` (disagg RTS skip at startup), `src/conn/conn_disagg.c`
- **Notes:** Does not use `@disagg_test_class` decorator — manually loads the `palite` extension and creates `kv_home/` and the follower symlink in `early_setup()`. Uses `simulate_crash_restart` from `helper.py`. Suppresses `WT_VERB_RTS` output. `table:` URI. Skipped for tiered storage hook.
