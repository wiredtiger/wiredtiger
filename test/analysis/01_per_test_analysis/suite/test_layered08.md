# test_layered08 — Layered read/write with encryption and compression, plus follower re-read

**File:** `test/suite/test_layered08.py`
**Storage mode:** Disagg/Layered (disagg_only)
**Components under test:** layered table read/write, checkpoint, follower re-read, encryption, block compression, page log (palite)

## Test Cases

### `test_layered08.test_layered_read_write`
- **What it tests:** Inserts 10,000 key/value pairs ("Hello N" -> "World") into a layered table as leader, takes a checkpoint, then reopens the connection as follower (simulating a node that has lost its local in-memory state) and reads back all 10,000 records verifying each value is correct.
- **Components:** ingest btree write path, checkpoint, page log (palite), follower connection re-open, layered cursor point-reads
- **Notes:** Parametrized across 2 encryption options (none, rotn) and 2 compression options (none, snappy): 4 scenarios total. Also runs in the disagg storage scenario. The test uses `reopen_conn` with follower config (not `restart_without_local_files`), so local `.wt` files are retained but the in-memory state is re-initialized from the checkpoint. Block compressor is applied at table creation. Would fail if checkpoint data is not properly flushed to the page log, or if the follower cannot reconstruct the btree from the page log data.
