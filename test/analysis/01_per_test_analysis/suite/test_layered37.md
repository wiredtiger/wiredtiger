# test_layered37 — Pinned ingest table pages are not evicted prematurely

**File:** `test/suite/test_layered37.py`
**Storage mode:** Disagg/Layered
**Components under test:** ingest btree, cur_layered.c, eviction, checkpoint pick-up, Oplog helper

## Test Cases

### `test_layered37.test_ping_ingest_table`
- **What it tests:** Verifies that ingest table pages that are held by an open cursor are not evicted, even when all data becomes obsolete. Uses the `Oplog` helper to insert 20,000 records into a layered table on both leader and follower, then removes all records and makes them globally visible. A pinned cursor (`session_follow2`) holds a reference to the first ingest record throughout. After triggering aggressive eviction on the ingest table via a debug cursor, the test verifies that the full 20,000 records are still visible to the pinned cursor.
- **Components:** ingest btree (eviction gate, pinned pages), cur_layered.c (cursor lifetime), conn_layered_ingest.c, checkpoint pick-up (`disagg_advance_checkpoint`), eviction (`debug=(release_evict)`), Oplog (helper_disagg.py)
- **Notes:** 20,000 items; small page sizes (allocation_size=512, leaf_page_max=512) to maximise page count. Uses the `Oplog` class from `helper_disagg.py` for timestamped insert/remove/check. Ignores "oldest id pinned in session" stdout warnings. Disagg-only.
