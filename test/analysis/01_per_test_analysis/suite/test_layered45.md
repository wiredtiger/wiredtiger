# test_layered45 — Durable entries are excluded from new leaf page deltas

**File:** `test/suite/test_layered45.py`
**Storage mode:** Disagg/Layered
**Components under test:** block_disagg, reconciliation (leaf page delta), page log, checkpoint, prepared transactions

## Test Cases

### `test_layered45.test_normal_update`
- **What it tests:** Verifies that a page with only an uncommitted update is skipped during delta writing (delta count stays at 1 after the uncommitted-only checkpoint). Inserts 10 records, checkpoints, makes a committed update (delta count becomes 1), then opens an uncommitted write on the same page and checkpoints again — the second checkpoint must not produce a new delta.
- **Components:** reconciliation (delta skip for uncommitted-only dirty pages), block_disagg, page log, checkpoint
- **Notes:** Uses `delta_pct=100` and `preserve_prepared=true`. Verifies `rec_page_delta_leaf` per-URI stat.

### `test_layered45.test_delete`
- **What it tests:** Verifies delta skip when a delete is present but an uncommitted update also touches the same page. After a deletion is committed and stable-timestamp-advanced, an uncommitted update should block a further delta. Once the uncommitted update is rolled back and oldest timestamp advances, a new delta (count=2) is produced. A subsequent uncommitted-only checkpoint must again skip delta writing (count stays 2).
- **Components:** reconciliation (delete + uncommitted guard, delta skip), block_disagg, checkpoint

### `test_layered45.test_delete_update_restore`
- **What it tests:** Tests the interaction of a prepared delete and page eviction. After a delete is committed and a delta is written (count=1), an uncommitted update on the deleted key is prepared; the page is then force-evicted. On the next checkpoint the delta count must still be 1 (page skipped). After rolling back the uncommitted update and advancing timestamps, count rises to 2; the following uncommitted-only checkpoint keeps count at 2.
- **Components:** reconciliation (update-restore path, eviction with uncommitted updates, delta skip), block_disagg, checkpoint

### `test_layered45.test_prepare_update`
- **What it tests:** Verifies delta behaviour with prepared transactions. After inserting data and checkpointing, a prepared (but not committed) update causes a delta (count=1). An uncommitted update prevents a subsequent delta. Once the prepared transaction commits and the stable timestamp is advanced, another delta is written (count=2). A checkpoint with no new changes keeps count at 2.
- **Components:** reconciliation (prepared transaction handling in delta logic), block_disagg, checkpoint, `preserve_prepared=true`

### `test_layered45.test_prepare_delete`
- **What it tests:** Same as `test_prepare_update` but the prepared transaction performs a delete (remove) rather than an update. Verifies the same delta count progression (1 → skip → 2 → skip) for the delete path.
- **Components:** reconciliation (prepared delete + delta logic), block_disagg, checkpoint

### `test_layered45.test_prepare_update_delete`
- **What it tests:** Same scenario but the prepared transaction performs both an update and a delete on the same key in sequence. Verifies the delta count progression is the same.
- **Components:** reconciliation (prepared update+delete combined, delta logic), block_disagg, checkpoint
