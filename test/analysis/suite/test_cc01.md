# test_cc01 — Shared base class for checkpoint-cleanup tests

**File:** `test/suite/test_cc01.py`
**Storage mode:** General
**Components under test:** checkpoint cleanup subsystem, history store, statistics

## Test Cases

This file defines `test_cc_base`, a shared base class that provides helper methods reused by `test_cc02` through `test_cc11`. It contains no standalone test methods.

### Helper methods (no direct test)
- `get_stat(stat, uri)` — Opens a statistics cursor and returns a single stat value.
- `large_updates(uri, value, ds, nrows, commit_ts)` — Writes `nrows` individual timestamped transactions.
- `large_modifies(uri, value, ds, location, nbytes, nrows, commit_ts)` — Issues in-place modify operations within a single transaction.
- `check(check_value, uri, nrows, read_ts)` — Reads all rows at a given read timestamp and verifies count and value equality.
- `populate(uri, start_key, num_keys, value, ts)` — Inserts keys with per-key or fixed timestamps.
- `wait_for_cc_to_run(ckpt_name)` — Triggers `debug=(checkpoint_cleanup=true)` and polls `checkpoint_cleanup_success` until it increments.
- `check_cc_stats(ckpt_name)` — Calls `wait_for_cc_to_run` then asserts both `checkpoint_cleanup_pages_visited` and `checkpoint_cleanup_pages_removed` are greater than zero.
