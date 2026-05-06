# test_ovfl01 — Overflow key handling during bulk insert and checkpoint

**File:** `test/suite/test_ovfl01.py`
**Storage mode:** General
**Components under test:** bulk cursor, overflow keys, reconciliation, checkpoint, verify

## Test Cases

### `test_ovfl01.test_ovfl01`
- **What it tests:** Bulk-inserts 10,000 key/value pairs where keys are 1 KB strings (forcing overflow key storage via `leaf_key_max=10B`); after checkpoint, verifies with `session.verify()` that no orphaned overflow keys remain on disk
- **Components:** `btree/bt_ovfl.c`, `btree/bt_rec.c`, `cursor/cur_bulk.c`, `block/block_ckpt.c`
- **Notes:** Uses `failpoint_rec_split_write` timing stress failpoint to increase test coverage of the reconciliation split path; table config uses `allocation_size=512B,leaf_key_max=10B,leaf_page_max=512B` to force overflow conditions on every key; skipped for tiered storage hook
