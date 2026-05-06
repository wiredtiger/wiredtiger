# test_layered52 — Internal page delta with deleted leaf pages

**File:** `test/suite/test_layered52.py`
**Storage mode:** Disagg/Layered
**Components under test:** block_disagg, reconciliation (internal page delta with deleted children), page log, checkpoint

## Test Cases

### `test_layered52.test_internal_page_delta_delete_leaf`
- **What it tests:** Verifies that internal page deltas containing deleted child references (`rec_page_delta_internal_key_deleted` stat) are written correctly when contiguous ranges of leaf pages are deleted. Populates 5000 records, reopens, then deletes two disjoint ranges of ~200 keys each (keys 200-399 and 3000-3199) with separate checkpoints in between. After both delete+checkpoint cycles, verifies that `rec_page_delta_internal_key_deleted > 0` and `rec_page_delta_internal > 0`. Reopens the connection and verifies data correctness (only expected keys are present).
- **Components:** reconciliation (internal page delta with deleted leaf references), block_disagg, page log, checkpoint
- **Notes:** Class is named `test_layered52` but file comment says "test_layered51.py" (copy-paste). Uses `file:` URI with `block_manager=disagg`. Very small page sizes (512 B allocation/leaf/internal) and `delta_pct=100` to maximize delta production. `internal_page_delta=true, leaf_page_delta=false` config. Uses `reopen_disagg_conn` to clear cache. The comment notes that two separate delete ranges on disjoint subtrees are needed to reliably trigger internal deltas with deleted keys. 5000 rows to ensure a deep enough tree. Disagg-only.
