# test_layered32 — Internal and leaf page delta writes to the page log

**File:** `test/suite/test_layered32.py`
**Storage mode:** Disagg/Layered
**Components under test:** block_disagg, page log, checkpoint, reconciliation, page deltas (leaf and internal), eviction, follower reads

## Test Cases

### `test_layered32.test_internal_page_delta_simple`
- **What it tests:** Verifies that the reconciler writes the correct type of page deltas (leaf-only, internal-only, both, or none) to the page log after small updates to a previously-checkpointed btree. After reopening as leader, confirms that internal page deltas are read back from the page log on cache miss. Then reopens as follower and verifies the same delta-read behaviour.
- **Components:** block_disagg (block manager), reconciliation (`rec_page_delta_leaf`, `rec_page_delta_internal` stats), page log extension (palite), eviction / cache read (`cache_read_internal_delta`), checkpoint
- **Notes:** Parametrized over four delta configurations: `leaf_only`, `internal_only`, `none`, `both`. Uses `file:` URI with `block_manager=disagg`. Small page sizes (512 B allocation, leaf, and internal) and `delta_pct=100` to force delta writes. 1000 rows initial population; two targeted key updates to trigger deltas. Uses `reopen_disagg_conn` to clear cache between phases.

### `test_layered32.test_internal_page_delta_split_internal`
- **What it tests:** Verifies that internal page deltas are written when pages merge back after a split. The workload first inserts small values (forcing a stable tree shape), then updates a subset of keys with large values to force a page split, then reverts those keys back to small values (triggering a merge) and checkpoints after each revert. Checks that the configured delta types are produced and that data is correct after a final reopen.
- **Components:** block_disagg, reconciliation (page split, page merge, delta writing), page log, checkpoint
- **Notes:** Same four-way delta parametrization as the sibling test. Uses 10 specific keys ("241"–"250") for the split-inducing updates. Does not test the follower path (leader-only). The test comment explicitly warns that it depends on reconciliation producing specific page sizes.
