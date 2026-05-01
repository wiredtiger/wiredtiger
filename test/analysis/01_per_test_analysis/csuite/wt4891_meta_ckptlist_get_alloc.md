# wt4891_meta_ckptlist_get_alloc — Checkpoint list allocation correctness under open checkpoint cursors

**Path:** `test/csuite/wt4891_meta_ckptlist_get_alloc/`
**Language:** C
**Storage mode:** General
**Jira ticket:** WT-4891
**Components under test:** `__wt_meta_ckptlist_get`, checkpoint cursor (`checkpoint=WiredTigerCheckpoint`), `session->verify`, ASAN memory error detection

## What This Test Does
This test reproduces WT-4891, where `__wt_meta_ckptlist_get` had a memory allocation bug detectable by AddressSanitizer when multiple checkpoints were accumulated. The test creates 10 checkpoints, keeping each one alive by opening a checkpoint cursor immediately after taking it (preventing the oldest checkpoint from being discarded). After all checkpoints are created, it calls `session->verify` on the table, which internally calls `__wt_meta_ckptlist_get`. The failure mode is an ASAN error in sanitized builds.

## Test Scenarios / Cases

### Scenario: Accumulate 10 checkpoints with open cursors, then verify
- **What it tests:** That `__wt_meta_ckptlist_get` correctly allocates and manages the checkpoint list when 10 checkpoints are alive simultaneously (each pinned by an open checkpoint cursor), and that `session->verify` succeeds without memory errors.
- **Components:** `session->checkpoint`, `session->open_cursor(checkpoint=WiredTigerCheckpoint)`, `session->verify`, `__wt_meta_ckptlist_get` internal allocation.
- **Notes:** CHECKPOINT_COUNT=10. One write (cursor update on "key1") per checkpoint iteration. Only detectable as a failure in ASAN builds; the test passes silently in non-sanitizer builds even if the bug exists.

## LazyFS Variant
None.
