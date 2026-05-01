# normalized_pos — B-tree normalized position (npos) correctness test

**Path:** `test/csuite/normalized_pos/`
**Language:** C
**Storage mode:** General
**Jira ticket:** N/A
**Components under test:** B-tree page navigation, normalized position (`__wt_page_npos`), `__wt_page_from_npos_for_eviction`, `__wt_page_from_npos_for_read`

## What This Test Does
This is a white-box unit test that verifies the correctness of WiredTiger's normalized position (npos) API, which expresses a page's position within a B-tree as a floating-point value in [0.0, 1.0]. It builds a tree with 100,000 keys arranged one key per page, then checks that: (1) computing npos for every key yields a monotonically non-decreasing sequence; (2) traversal via npos visits every page exactly once in both forward and backward directions; and (3) restoring a page from its own npos returns the same page reference. It runs under both in-memory and on-disk configurations.

## Test Scenarios / Cases

### Scenario: In-memory B-tree, eviction path (`__wt_page_from_npos_for_eviction`)
- **What it tests:** Verifies that the eviction-oriented page-from-npos function traverses all 100,000 pages in strict forward and backward order without revisiting any page.
- **Components:** In-memory WiredTiger, `__wt_page_from_npos_for_eviction`, `__wt_page_npos`.
- **Notes:** In-memory mode guarantees one key per page, enabling exact page-count assertions.

### Scenario: In-memory B-tree, read path (`__wt_page_from_npos_for_read`)
- **What it tests:** Same as above but via `__wt_page_from_npos_for_read`.
- **Components:** `__wt_page_from_npos_for_read`.
- **Notes:** Forward and backward counts must match.

### Scenario: In-memory B-tree, key-by-key npos roundtrip
- **What it tests:** For each of the 100,000 keys, searches for the key, computes npos of the containing page, then calls `page_from_npos` and asserts the returned page reference equals the original.
- **Components:** Cursor search, `__wt_page_npos`, `__wt_page_from_npos_for_eviction`.
- **Notes:** Requires the tree to be stable (no splits) during the check.

### Scenario: On-disk B-tree (same sub-tests)
- **What it tests:** Repeats all three sub-tests with a 1 MB cache that forces eviction to disk; on-disk mode removes the strict one-key-per-page guarantee so page-count assertions are relaxed.
- **Components:** On-disk storage, eviction.
- **Notes:** Forward/backward count equality is still asserted for the read path.

## LazyFS Variant
None.
