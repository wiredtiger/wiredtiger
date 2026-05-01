# wt2246_col_append — Column-store append efficiency test

**Path:** `test/csuite/wt2246_col_append/`
**Language:** C
**Storage mode:** General
**Jira ticket:** WT-2246
**Components under test:** Column-store append (`key_format=r`, `append` cursor config), record number allocation efficiency

## What This Test Does
This test demonstrates and measures the efficiency of column-store append operations. Before the fix for WT-2246, the column-store search routine unnecessarily searched the target leaf page even when allocating a new record number via an `append`-configured cursor. The test populates a column-store table with 5,000 records (reopening the connection to force data to disk), then runs 6 concurrent append threads each appending up to 20 million records, measuring processor seconds consumed. It is primarily a performance regression test rather than a correctness test.

## Test Scenarios / Cases

### Scenario: Concurrent column-store append (6 threads)
- **What it tests:** That concurrent appends using the `append` cursor configuration complete without unnecessary leaf-page searches, measured via wall-clock processor seconds for the total number of records inserted.
- **Components:** Column-store B-tree, `cursor->insert` with `append` config, auto-allocated record numbers, `thread_append` utility function.
- **Notes:** Test can be interrupted with SIGINT. Reports millions of records per processor second. The "inefficiency rather than correctness bug" characterization from the ticket means no data content is verified.

## LazyFS Variant
None.
