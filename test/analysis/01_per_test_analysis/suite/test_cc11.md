# test_cc11 — Checkpoint cleanup is not run on disaggregated follower nodes

**File:** `test/suite/test_cc11.py`
**Storage mode:** Disagg (disagg_only=True)
**Components under test:** checkpoint cleanup subsystem, disaggregated storage, follower role

## Test Cases

### `test_cc11.test_cc11`
- **What it tests:** Verifies that `checkpoint_cleanup_success` remains 0 after a checkpoint with `debug=(checkpoint_cleanup=true)` is triggered on a node configured as a disaggregated follower. CC must not run on follower nodes.
- **Components:** `src/conn/conn_sweep.c`, `src/disagg/`
- **Notes:** Class decorated with `@disagg_test_class`; extends `DisaggConfigMixin` and `test_cc_base`. Connection configured with `disaggregated=(role="follower")` and `checkpoint_cleanup=[wait=1,file_wait_ms=0]`. Skips tiered hook. Inserts 10 rows, waits 1 second for the CC thread, then issues a forced cleanup checkpoint and asserts `stat.conn.checkpoint_cleanup_success == 0`. Tests the "follower must not run CC" invariant in disaggregated deployments.
