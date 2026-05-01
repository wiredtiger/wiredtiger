# wt8057_compact_stress — Compact operation unclean shutdown consistency

**Path:** `test/csuite/wt8057_compact_stress/`
**Language:** C
**Storage mode:** General
**Jira ticket:** WT-8057
**Components under test:** `session->compact`, unclean shutdown (SIGKILL), two-table consistency verification, `WT_EVENT_COMPACT_CHECK` interrupt, row-store, column-store

## What This Test Does
This test verifies that there are no data inconsistencies between two identical tables after a compact operation is interrupted by an unclean shutdown. A child process creates and populates two identical tables (`compact1` and `compact2`), then loops up to 40 times: checkpoint, remove 1/3 of records from a random key range (applied identically to both tables), compact only `compact1`, then repopulate both. The parent kills the child after 40 seconds (once at least one checkpoint has been written). After recovery, the parent verifies that both tables contain identical records. A `WT_EVENT_COMPACT_CHECK` general event handler interrupts compaction every 8th check event to test interrupted compact scenarios.

## Test Scenarios / Cases

### Scenario: Row-store compact interrupted by unclean shutdown — two-table consistency
- **What it tests:** That after SIGKILL during compaction of `compact1` (while `compact2` is never compacted), recovery leaves both tables with identical key/value content.
- **Components:** `session->compact`, fork/SIGKILL, `WT_EVENT_COMPACT_CHECK` interrupt handler, `verify_tables_helper` (cursor scan comparing both tables), `compact_slow` (not used here — timing is free-running).
- **Notes:** NUM_RECORDS=100,000, TIMEOUT=40s. Sentinel file `checkpoint_done` synchronizes parent kill. Compact is interrupted ~every 8th `WT_EVENT_COMPACT_CHECK` callback.

### Scenario: Column-store compact interrupted by unclean shutdown — two-table consistency
- **What it tests:** Same consistency check using column-store tables (`key_format=r`).
- **Components:** Column-store, fork/SIGKILL, compact interrupt, two-table comparison.

## LazyFS Variant
None.
