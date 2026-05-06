# test_cc04 — Checkpoint cleanup must not remove non-obsolete pages

**File:** `test/suite/test_cc04.py`
**Storage mode:** General
**Components under test:** checkpoint cleanup subsystem, history store, statistics

## Test Cases

### `test_cc04.test_cc`
- **What it tests:** Verifies that CC does not remove or evict pages that are not yet obsolete. With `oldest_timestamp` pinned at 1 and multiple large update rounds at timestamps 10–70 (all above oldest), CC should visit pages but must not increment `checkpoint_cleanup_pages_evict` or `checkpoint_cleanup_pages_removed`.
- **Components:** `src/btree/`, `src/history/`, `src/conn/conn_sweep.c`
- **Notes:** Uses `SimpleDataSet` with 10 000 rows and 500-byte values. Runs `wait_for_cc_to_run()` five separate times (after each update round), each time asserting `pages_evict == 0` and `pages_removed == 0` while `pages_visited > 0`. The test confirms CC inspects pages correctly but applies no destructive cleanup when the oldest timestamp has not advanced past the data's timestamps. Overrides `get_stat` locally to use the connection-level statistics cursor directly.
