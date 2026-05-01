# test_layered19 — Configurable max consecutive deltas: verify delta limit enforcement

**File:** `test/suite/test_layered19.py`
**Storage mode:** Disagg/Layered (disagg_only)
**Components under test:** `page_delta=(max_consecutive_delta=1)` configuration, page delta limiting, checkpoint, follower reads, block_disagg

## Test Cases

### `test_layered19.test_layered_read_write`
- **What it tests:** Creates a table (either `layered:` URI or `file:` with `block_manager=disagg`), inserts 1000 records, checkpoints (full page written). Updates every 10th record and checkpoints (delta). Updates every 10th record again and checkpoints a second time (would be second delta, but `max_consecutive_delta=1` forces a full page instead). Reopens connection as follower, reads all 1000 records, verifies updated keys have the final value and unmodified keys have the original value. Then checks that `cache_read_leaf_delta` stat is 0 — confirming no deltas were read (because the second checkpoint forced a full page, there are no delta chains to follow on the follower).
- **Components:** `page_delta` configuration (`max_consecutive_delta`), full vs delta page selection during reconciliation, follower page read path, statistics (`cache_read_leaf_delta`)
- **Notes:** Parametrized across 2 URI types (layered, file+disagg) and disagg_storage. The `max_consecutive_delta=1` setting means after one delta checkpoint, the next checkpoint for the same page must write a full image. The zero `cache_read_leaf_delta` stat on the follower is the key assertion: it proves no delta pages were read back, confirming the limit was enforced. Would break if the reconciler ignores the `max_consecutive_delta` knob or if delta-limit logic has an off-by-one error.
