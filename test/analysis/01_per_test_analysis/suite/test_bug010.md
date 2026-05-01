# test_bug010 — Checkpoint dirty flag consistency with concurrent checkpoint thread

**File:** `test/suite/test_bug010.py`
**Storage mode:** General
**Components under test:** checkpoint, dirty-page tracking, concurrent checkpoint thread

## Test Cases

### `test_bug010.test_checkpoint_dirty`
- **What it tests:** Verifies that a checkpoint does not mark files clean when it could not write all updates. Creates 200 tables (2000 in long-test mode), inserts `'a'=0` into each, and takes an initial checkpoint. Then iterates 10 times: starts a background `checkpoint_thread`, updates `'a'` to the current iteration value across all tables, stops the background thread, takes a second explicit checkpoint, and reads from `checkpoint=WiredTigerCheckpoint` to confirm all tables report the correct `expected_val`. Asserts no table is stale (stuck at an older value).
- **Components:** `src/checkpoint/checkpoint.c`, `src/btree/bt_page.c`
- **Notes:** Uses `checkpoint_sync=false` to speed up checkpoints. Skipped for the `disagg` hook (layered trees do not support opening checkpoint cursors). Non-parametrized; table count scales with `islongtest()`.
