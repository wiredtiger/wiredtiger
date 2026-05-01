# test_layered35 — Leaf page delta skipping when page is not modified (with encryption and compression)

**File:** `test/suite/test_layered35.py`
**Storage mode:** Disagg/Layered
**Components under test:** block_disagg, page log, reconciliation (leaf page delta), encryption, block compression, checkpoint, follower reads

## Test Cases

### `test_layered35.test_layered_skip_empty_delta`
- **What it tests:** Verifies that when a page contains only an uncommitted update at checkpoint time, the reconciler skips writing a leaf page delta (stat `rec_page_delta_leaf` stays at 0). Inserts 100 records at timestamp 5 and checkpoints. Then makes a single update at timestamp 10 (above stable timestamp 5) — the resulting uncommitted-only dirty page should be skipped during the second checkpoint. Reopens as a follower and verifies both the data correctness and the zero-delta stat.
- **Components:** reconciliation (delta skip logic), block_disagg, encryption extension, block compressor extension, page log, checkpoint (precise_checkpoint=true), stable btree
- **Notes:** Parametrized over: encryption (none, rotn/keyid=13), compression (none, snappy), storage backend (disagg only), and URI type (layered: or file: with block_manager=disagg). Uses `delta_pct=80` and `preserve_prepared=true`. The follower reopen uses `local_files_action=ignore` to avoid deleting the checkpoint meta file. Total scenario combinations: 2×2×1×2 = 8. Disagg-only.
