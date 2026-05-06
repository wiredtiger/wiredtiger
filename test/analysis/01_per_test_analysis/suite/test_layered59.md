# test_layered59 — Internal page delta is not built when the first key of a child is modified

**File:** `test/suite/test_layered59.py`
**Storage mode:** Disagg/Layered
**Components under test:** block_disagg, reconciliation (internal page delta first-key guard), page log, checkpoint

## Test Cases

### `test_layered59.test_single_update`
- **What it tests:** Verifies that updating the first key of a child page (key "1") prevents the reconciler from writing an internal page delta. Inserts 999 records, checkpoints, reopens, then updates key "1" (the smallest / first key in the tree) and checkpoints. Asserts `rec_page_delta_internal == 0` — no internal delta should be produced when the first key of a child is modified, because the internal page's separator key would need to change, invalidating the delta format.
- **Components:** reconciliation (first-key guard for internal page delta suppression), block_disagg, page log, checkpoint

### `test_layered59.test_inserts_to_split`
- **What it tests:** Verifies that inserting many keys before the existing key range (causing a page split that changes the first key of an existing child) also prevents internal page delta generation. First inserts keys 1000–1999, checkpoints, reopens, then inserts keys 1–999 (prepending a new range). The first leaf page splits, which modifies the internal tree's separator key. Asserts `rec_page_delta_internal == 0`.
- **Components:** reconciliation (split-induced first-key change guard), block_disagg, page log, checkpoint

### `test_layered59.test_deletes`
- **What it tests:** Verifies that deleting the front portion of a leaf page (keys 1 through nitems/2 - 1) from an otherwise stable tree also prevents internal page delta generation, because the surviving first key of the affected child changes. Inserts keys 1000–1999, checkpoints, reopens, deletes keys 1–499 (which does not exist in the stable tree — actually a no-op in terms of existing keys but modifies the ingest, then makes oldest advance), and checkpoints. Asserts `rec_page_delta_internal == 0`.
- **Components:** reconciliation (delete-induced first-key change guard), block_disagg, page log, checkpoint

- **Notes (all tests):** Uses `DisaggConfigMixin` directly alongside `disagg_test_class`. `delta_pct=100` forces aggressive delta attempts so any suppression is meaningful. Hardcoded `palite` in `conn_config` (no scenario parametrization of storage). 1000 items. Disagg-only.
