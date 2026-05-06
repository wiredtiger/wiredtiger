# test_layered21 — Insert on both leader and follower, with role transition to follower

**File:** `test/suite/test_layered21.py`
**Storage mode:** Disagg/Layered (disagg_only)
**Components under test:** insert on leader and follower roles, role transition at runtime, SimpleDataSet population and verification, ingest btree, conn_layered.c

## Test Cases

### `test_layered21.test_insert_changing_roles`
- **What it tests:** The node starts with either `role="leader"` or `role="follower"` (parametrized). In both cases, uses `SimpleDataSet` to populate 1000 entries and verify them immediately after insertion.

  If the initial role is `"leader"`: checkpoints, then reopens the connection with `role="follower"` (picking up the checkpoint). Inserts another 1000 entries into the follower and verifies both the original and new entries are visible.

- **Components:** ingest btree write on both roles, checkpoint, role transition via `reopen_conn`, cursor insert/scan, `SimpleDataSet` (uses key_format=r, value_format=S by default)
- **Notes:** Parametrized across 2 roles x disagg_storage. The follower-start scenario only tests insert-and-read without role transition. The leader-start scenario tests the full cycle: write, checkpoint, become-follower, write more, verify all data. Would break if: (1) inserts on a follower crash or lose data, (2) checkpoint data is not accessible after reopening as follower, or (3) new inserts on the follower are not visible via cursor scan.
