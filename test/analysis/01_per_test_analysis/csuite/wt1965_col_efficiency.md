# wt1965_col_efficiency — Column-store sparse record ID CPU efficiency test

**Path:** `test/csuite/wt1965_col_efficiency/`
**Language:** C
**Storage mode:** General
**Jira ticket:** WT-1965
**Components under test:** Column-store (key_format=r), sparse record IDs, B-tree traversal efficiency, index table

## What This Test Does
This test verifies that column-store tables do not exhibit excessive CPU usage when populated with sparse record IDs (large gaps in the key space). Four threads each insert records using keys of the form `thread_index << 40 | record_number`, creating very large gaps between consecutive keys. The test exercises a concurrent insert workload with both a main column-store table and an auxiliary index table, then verifies all inserted records can be read back. It is primarily a performance/efficiency regression test — the reported bug was high CPU usage, not data corruption.

## Test Scenarios / Cases

### Scenario: Sparse record ID insertion (4 threads)
- **What it tests:** That concurrent inserts with very sparse record IDs (bit-shifted by 40 positions, creating gaps of ~1 trillion between each thread's key range) complete in a reasonable time without consuming excessive CPU. Correctness is validated by a full cursor scan after all threads complete.
- **Components:** Column-store B-tree, sparse recno keys, concurrent transactions (`sync=false`), cursor scan.
- **Notes:** Each thread performs 9 rounds of inserting NR_OBJECTS/NR_THREADS records. Rate-limited to ~5,000 updates/sec per thread to keep timestamps meaningful. At the end, all keys are scanned and printed (if verbose).

## LazyFS Variant
None.
