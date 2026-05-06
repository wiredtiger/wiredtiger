# test_layered63 — Internal and leaf page delta correctness across merge scenarios

**File:** `test/suite/test_layered63.py`
**Storage mode:** Disagg/Layered
**Components under test:** Page delta reconciliation (internal and leaf), delta merge, follower read-back

## Test Cases

### `test_layered63.test_internal_page_delta_update`
- **What it tests:** Applies repeated updates to the same fixed set of 100 keys (spanning multiple internal page boundaries) across 1–10 randomly chosen delta checkpoints. Verifies that `rec_page_delta_leaf` and `rec_page_delta_internal` statistics are non-zero (or zero) according to the scenario's delta configuration, then reopens the connection and verifies all updated values, then reopens as follower and verifies again that `cache_read_internal_delta` incremented appropriately.
- **Components:** `src/btree/bt_rec.c`, `src/btree/bt_page.c`, page log extension (`palite`)
- **Notes:** Parametrized by delta config: `write_leaf_only`, `write_internal_only`, `write_none`, `write_both`. `delta_pct=100` is set globally to maximise delta generation. File URI is `file:test_layered63` with small page sizes (512 bytes) to force splits and internal page deltas.

### `test_layered63.test_delta_insert_keys_at_end_of_base_image`
- **What it tests:** Verifies that new keys inserted beyond the base image keyspace (nrows+1 … nrows+10) in the final delta round are captured and correctly merged into the full image. Earlier delta rounds update random mid-range base keys. Checks both leader and follower read-back.
- **Components:** `src/btree/bt_rec.c`, `src/btree/bt_page.c`, page log extension
- **Notes:** Specifically targets the merge code path where a delta has keys that extend past the end of the base image's keyspace.

### `test_layered63.test_base_image_has_more_keys_at_end_of_merge`
- **What it tests:** Base image contains nrows + 50 extra trailing keys never touched by any delta. Verifies that after delta updates to mid-range keys, the unmodified trailing base keys survive intact after reopen and follower verification.
- **Components:** `src/btree/bt_rec.c`, merge logic
- **Notes:** Confirms the merge correctly appends base-image trailing keys after all delta entries are exhausted.

### `test_layered63.test_internal_page_delta_key_updated_multiple_times`
- **What it tests:** A deterministic set of keys at three internal-page boundary positions (nrows/4, nrows/2, 3*nrows/4) is updated in each of 8 fixed delta rounds, along with 20 random noise keys per round. Verifies correct merge ordering and that the final values for the repeatedly-updated keys are preserved.
- **Components:** `src/btree/bt_rec.c`, internal page reconciliation
- **Notes:** Uses `checkpoint("use_timestamp=true,force=true")`. Checks `cache_read_internal_delta > 0` after reopen for `both`/`internal_only` scenarios.
