# test_layered33 — Delete operations on the ingest table

**File:** `test/suite/test_layered33.py`
**Storage mode:** Disagg/Layered
**Components under test:** cur_layered.c, ingest btree, cursor remove

## Test Cases

### `test_layered33.test_delete`
- **What it tests:** Inserts 100 records into a layered table (on a follower node), then removes every record using `cursor.remove()`, and verifies that the table is empty afterwards — both via `cursor.next()` (scan) and via individual `cursor.search()` point-lookups returning `WT_NOTFOUND`.
- **Components:** cur_layered.c (delete path through the layered cursor), ingest btree (write path for follower), conn_layered_ingest.c
- **Notes:** Parametrized over two value formats: string (`S`) and integer (`I`). The connection starts as `role="follower"` (with `lose_all_my_data=true`), so all writes go to the ingest table. Tests that tombstones are correctly reflected when scanning and point-reading. Disagg-only scenarios.
