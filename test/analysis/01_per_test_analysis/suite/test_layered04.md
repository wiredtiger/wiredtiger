# test_layered04 — Large insert triggering stable-table checkpoint, with statistics validation

**File:** `test/suite/test_layered04.py`
**Storage mode:** Disagg/Layered
**Components under test:** layered table insert at scale, checkpoint, btree statistics, cur_layered.c, conn_layered.c

## Test Cases

### `test_layered04.test_layered04`
- **What it tests:** Inserts 50,000 * 3 = 150,000 key/value pairs into a layered table in a single session (no explicit checkpoint between inserts). Then performs a full forward scan to count all records and asserts the count equals 150,000. Finally, opens a statistics cursor on the layered URI and asserts that `btree_entries` equals the same count.
- **Components:** ingest btree (high-volume write path), cursor iteration (`cur_layered.c`), layered statistics aggregation, btree reconciliation, checkpoint (triggered implicitly by volume)
- **Notes:** With 50,000 iterations and 3 keys each, this test is designed to produce enough data to trigger an internal checkpoint on the stable table. Statistics log is enabled (`statistics_log=(wait=1,...)`). The key assertions are: (1) the full-scan item count is exactly 150,000 and (2) `stat.dsrc.btree_entries` reports the same value. Would break if the layered cursor skips records, double-counts, or if statistics gathering is incorrect for layered tables.
