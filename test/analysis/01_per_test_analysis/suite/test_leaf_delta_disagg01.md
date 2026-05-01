# test_leaf_delta_disagg01 — Leaf page delta construction and merge correctness

**File:** `test/suite/test_leaf_delta_disagg01.py`
**Storage mode:** Disagg (PALite, `disagg_only=True`)
**Components under test:** leaf page delta encoding/decoding, base-image + delta merge, prefix compression in delta pages, disagg block manager (`block_manager=disagg`), reconciliation statistics

## Test Cases

### `test_leaf_delta_disagg01.test_delta_no_duplicate_keys`
- **What it tests:** Verifies that three sequential deltas with completely disjoint key sets correctly overlay the base image. Keys updated in each delta take the delta value; keys only in the base image retain the base value.
- **Components:** `src/btree/`, `src/reconcile/`, `src/block/`, `src/conn/conn_layered_ingest.c`, `src/cursor/cur_layered.c`
- **Notes:** Base has keys 1–10; delta1 updates keys 1–3, delta2 keys 4–6, delta3 keys 7–9. No key appears in more than one delta, so merge priority logic is not exercised; only base-vs-delta overwrite is tested. Runs across 2 prefix-compression scenarios (enabled/disabled).

### `test_leaf_delta_disagg01.test_delta_duplicate_keys`
- **What it tests:** Verifies that when the same key appears in multiple deltas, the value from the chronologically latest delta wins. Exercises the merge-priority logic across overlapping delta sets.
- **Components:** `src/btree/`, `src/reconcile/`, `src/block/`, `src/cursor/cur_layered.c`
- **Notes:** Base has keys 1–10; delta1 updates {1,2,3,4,8}, delta2 updates {3,4,9}, delta3 updates {6,8,10}. Keys 3, 4, and 8 appear in multiple deltas; the test checks that the latest delta's value is visible. Runs across both prefix-compression scenarios.

### `test_leaf_delta_disagg01.test_delta_inserted_keys`
- **What it tests:** Verifies that deltas can introduce keys that were never present in the base image (true inserts via delta). Exercises the merge path for keys that exist only in a delta, not in the base.
- **Components:** `src/btree/`, `src/reconcile/`, `src/block/`, `src/cursor/cur_layered.c`
- **Notes:** Base deliberately omits keys 4, 7, and 8 (base = {3,5,6,9,10,11,12} effectively 10 items from range(3,13)-{4,7,8}); delta1 inserts key 4, delta2 inserts key 7, delta3 inserts keys 8 and 15. Also includes key 15 which is outside the original base range. Runs across both prefix-compression scenarios.

### `test_leaf_delta_disagg01.test_base_empty_values_all`
- **What it tests:** Verifies correct delta merge when the base image contains only empty-string values for all entries. Ensures the delta merge code handles zero-length values in the base without corruption.
- **Components:** `src/btree/`, `src/reconcile/`, `src/block/`
- **Notes:** Base has keys 1–10 all with value `""`. Deltas have non-empty values, so after merge the delta values should dominate for updated keys. Runs across both prefix-compression scenarios.

### `test_leaf_delta_disagg01.test_base_empty_values_mixed`
- **What it tests:** Verifies delta merge correctness when the base image alternates between empty and non-empty values, exercising value-length variation in the base during unpack/merge.
- **Components:** `src/btree/`, `src/reconcile/`, `src/block/`
- **Notes:** Base pattern is `["base", "", "base"] * 3 + [""]` for keys 1–10. Deltas are the same structure as `test_delta_duplicate_keys`. Runs across both prefix-compression scenarios.

### `test_leaf_delta_disagg01.test_comprehensive`
- **What it tests:** A combined stress scenario exercising simultaneously: inserted keys (keys not in base), duplicate keys across deltas, and mixed empty/non-empty values in both base and deltas.
- **Components:** `src/btree/`, `src/reconcile/`, `src/block/`, `src/cursor/cur_layered.c`
- **Notes:** Base omits keys {4,5,6}; deltas contain a mixture of overlapping keys, newly inserted keys, and empty-string values at various positions. Most comprehensive single-method coverage of the leaf delta merge path. Runs across both prefix-compression scenarios.

### `test_leaf_delta_disagg01.test_delete`
- **What it tests:** Verifies that key deletions are correctly encoded into a leaf page delta and that after a subsequent connection reopen the deleted keys are not found. Also checks that the deletion itself produces exactly one additional leaf delta.
- **Components:** `src/btree/`, `src/reconcile/`, `src/block/`, `src/cursor/cur_layered.c`
- **Notes:** Builds on the same three-delta base structure as `test_delta_duplicate_keys`, then deletes keys {1,3,10} and checkpoints. Asserts `rec_page_delta_leaf` stat equals 1 for the deletion checkpoint. Reopens connection and verifies the deleted keys return `WT_NOTFOUND`. Runs across both prefix-compression scenarios.

---

**Shared test infrastructure (`verify_leaf_delta`):**

All test methods delegate to `verify_leaf_delta()`, which:
1. Creates the layered table with small page sizes (512 bytes, `block_manager=disagg`) and a high `delta_pct=100` to maximise delta generation.
2. Populates the base image and checkpoints, asserting zero leaf deltas at that stage.
3. Reopens the disagg connection three times (via `reopen_disagg_conn`), inserting/updating the three delta sets and checkpointing each time, accumulating a delta count.
4. Asserts exactly 3 leaf deltas were produced across the three checkpoints.
5. Checks `rec_prefix_compression_full` / `rec_prefix_compression_delta` statistics against the `prefix_enabled` scenario flag.
6. Reopens once more and verifies that the correct value for every key is readable, respecting delta-overwrite priority order.

**Scenarios:** 1 storage variant × 2 prefix-compression variants = 2 scenario combinations.
