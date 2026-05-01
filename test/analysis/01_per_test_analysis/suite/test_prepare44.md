# test_prepare44 — Aborted prepared update at tail of chain does not cause split_multi_inmem assertion

**File:** `test/suite/test_prepare44.py`
**Storage mode:** General (`precise_checkpoint=true,preserve_prepared=true`), in-memory table
**Components under test:** prepared transactions, rollback, eviction, split_multi_inmem, has_newer_updates

## Test Cases

### `test_prepare44.test_evict_aborted_prepared_tail`
- **What it tests:** Reproduces an assertion failure in `__split_multi_inmem`: inserts an aborted prepared update at the tail of an update chain; then evicts the page; verifies that `has_newer_updates` is not incorrectly set by the aborted prepare, which would cause the split to assert; verifies the correct value is readable after eviction
- **Components:** `txn/txn_prepare.c`, `txn/txn_rollback.c`, `evict/evict_page.c`, `btree/bt_split.c`
- **Notes:** `conn_config = 'precise_checkpoint=true,preserve_prepared=true'`; uses an in-memory table (`log=(enabled=false)`, no disk pages) so that eviction goes through the `__split_multi_inmem` path; the bug was that a rolled-back prepared update at the tail of the update chain would incorrectly set `has_newer_updates=true`, causing the split code to assert that no newer updates should exist; no scenarios
