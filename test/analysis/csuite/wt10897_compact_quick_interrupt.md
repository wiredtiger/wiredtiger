# wt10897_compact_quick_interrupt — Compaction early-interrupt via event handler

**Path:** `test/csuite/wt10897_compact_quick_interrupt/`
**Language:** C
**Storage mode:** General
**Jira ticket:** WT-10897
**Components under test:** `session->compact`, `WT_EVENT_COMPACT_CHECK` event, compaction interrupt, compaction skip detection, statistics (`WT_STAT_DSRC_BTREE_COMPACT_PAGES_REVIEWED`)

## What This Test Does
This test verifies that compaction can be interrupted quickly via the `WT_EVENT_COMPACT_CHECK` general event handler, even before compaction reviews any pages. It exercises three compaction scenarios on the same table: (1) compaction is skipped because there is insufficient space to reclaim; (2) compaction is interrupted immediately by the event handler before it can do any meaningful work (verified by checking the `pages_reviewed` statistic equals 0); and (3) compaction completes normally.

## Test Scenarios / Cases

### Scenario: Compaction skipped (no work to do)
- **What it tests:** That when a table has very few records and no space to reclaim, compaction detects this condition and skips without doing any work, emitting a "skipping compaction" message.
- **Components:** `session->compact()`, compaction skip logic.
- **Notes:** Only 10 records are in the table at this point.

### Scenario: Compaction interrupted immediately via event handler
- **What it tests:** That returning -1 from the `WT_EVENT_COMPACT_CHECK` event handler causes `session->compact()` to return `WT_ERROR` and that the `pages_reviewed` statistic remains 0 (compaction was interrupted before reviewing any pages).
- **Components:** `WT_EVENT_COMPACT_CHECK`, `handle_general` callback, compact interrupt, `WT_STAT_DSRC_BTREE_COMPACT_PAGES_REVIEWED`.
- **Notes:** The table has 100,010 records at this point with roughly half deleted, so there is genuine compaction work to do — the test confirms the interrupt happens before any of it begins.

### Scenario: Compaction completes normally
- **What it tests:** That with the event handler no longer returning -1, compaction runs to completion without being interrupted or skipped.
- **Components:** `session->compact()`, normal compaction path.

## LazyFS Variant
None.
