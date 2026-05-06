# wt4105_large_doc_small_upd — Large document with small repeated modify operations

**Path:** `test/csuite/wt4105_large_doc_small_upd/`
**Language:** C
**Storage mode:** General
**Jira ticket:** WT-4105
**Components under test:** `cursor->modify`, large value storage, modify chain reconstruction, cache pressure with pinned transaction

## What This Test Does
This test stress-tests `cursor->modify` on 1 MB documents by applying 1,024 small (26-byte) modifications to 2 documents in a loop, while a separate long-running snapshot transaction pins the cache to force modify chain growth. A 15-second SIGALRM is set before each `cursor->modify` call to detect hangs — if the modify operation takes more than 15 seconds, the test aborts with a core dump. The pattern simulates append-style sequential modifications cycling through the document offset.

## Test Scenarios / Cases

### Scenario: Small modify operations on 1 MB documents with pinned snapshot
- **What it tests:** That `cursor->modify` does not hang or deadlock when the cache is pressured by a long-running pinned snapshot transaction, causing an ever-growing modify chain on large documents.
- **Components:** `cursor->modify`, 1 MB document (`leaf_key_max=64M`, `leaf_value_max=64M`, `leaf_page_max=32k`, `memory_page_max=1M`), snapshot isolation, SIGALRM timeout detection.
- **Notes:** DATASIZE=1MB, MODIFY_COUNT=1024, NUM_DOCS=2. Alarm is suppressed for MSAN/UBSAN/TSAN builds due to slowdown. Session 1 holds a `begin_transaction(snapshot)` without committing; session 2 performs all the modify operations.

## LazyFS Variant
None.
