# test_layered90 — Follower picks up multiple sequential checkpoints for the same table

**File:** `test/suite/test_layered90.py`
**Storage mode:** Disagg/Layered
**Components under test:** Follower repeated checkpoint pickup, use-after-free fix (`debug_mode=(cursor_copy=true)`), metadata cursor value lifetime

## Test Cases

### `test_layered90.test_follower_picks_up_updated_checkpoint`
- **What it tests:** Leader creates a layered table, writes 100 keys with prefix "v1-", checkpoints. Follower opens, advances to checkpoint 1, verifies all 100 keys with "v1-" values. Leader writes 100 keys with "v2-", checkpoints. Follower advances to checkpoint 2, verifies "v2-" values. Leader writes "v3-", checkpoints. Follower advances, verifies "v3-" values. Confirms that a follower can correctly pick up the same table across three sequential checkpoints.
- **Components:** `src/conn/conn_disagg.c`, follower checkpoint advance, metadata cursor reuse
- **Notes:** Parametrized by cursor_copy (False/True). When `cursor_copy=True`, `debug_mode=(cursor_copy=true)` is enabled on the follower, which triggers ASAN use-after-free detection if metadata cursor values are accessed after the cursor advances (this was the bug this test was written for). Disagg-only.
