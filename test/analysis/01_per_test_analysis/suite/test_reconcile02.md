# test_reconcile02 — Removing globally-visible deleted keys counts as reconciliation progress

**File:** `test/suite/test_reconcile02.py`
**Storage mode:** General
**Components under test:** reconciliation, eviction progress, tombstones, oldest_timestamp, statistics

## Test Cases

### `test_reconcile02.test_reconcile02`
- **What it tests:** Inserts two keys at ts=10; deletes one key at ts=20; evicts the page; opens an uncommitted update on the surviving key (in a second session) to block clean eviction; advances `oldest_timestamp` to ts=20 (making the delete globally visible); evicts the page again; verifies that `cache_eviction_blocked_no_progress` statistic is 0, confirming that removing the globally-visible deleted key from the on-disk image is counted as making progress during reconciliation
- **Components:** `btree/bt_rec.c`, `evict/evict_page.c`, `btree/bt_delete.c`, `txn/txn_timestamp.c`
- **Notes:** Scenarios: column/integer-row; the "no progress" stat fires when eviction cannot make any forward progress (no keys can be removed or compacted) — this is a liveness issue; the test guards against a bug where removing a globally-visible tombstone was not counted as progress, causing eviction to give up and potentially leading to cache pressure; the uncommitted update in session2 prevents the page from being fully cleaned but the delete should still be removable
