# wt10461_skip_list_stress — Skip-list weak-memory-ordering stress test

**Path:** `test/csuite/wt10461_skip_list_stress/`
**Language:** C
**Storage mode:** General (in-memory B-tree; white-box test)
**Jira ticket:** WT-10461
**Components under test:** `__wt_search_insert`, skip-list `next_stack` pointer construction, weak memory model correctness

## What This Test Does
This test reproduces WT-10461, a race condition where platforms with weak memory ordering (e.g., ARM) can corrupt a skip-list's `next_stack` by reading level pointers out of order during concurrent insertion. It sets up an insert list anchored by two keys ("0" and "99999"), then runs all available CPU cores minus one as `__wt_search_insert` threads that repeatedly build a `next_stack` for a probe key "00" (simulating the critical read section of a concurrent insert). Simultaneously, a single thread inserts 10,000 keys in decreasing order (simulating key "C" being inserted while "B"'s pointer is being constructed). An assertion in `__wt_search_insert` fires if an upper level of `next_stack` points to a smaller key than a lower level, catching the out-of-order read. The test loops for 15 minutes.

## Test Scenarios / Cases

### Scenario: Concurrent search-insert stress (all available CPUs)
- **What it tests:** That the `next_stack` constructed by `__wt_search_insert` is always internally consistent — upper levels point to larger keys than lower levels — even under concurrent insertions on weak-memory-order hardware.
- **Components:** `__wt_search_insert`, skip-list insert list, `debug_mode=(stress_skiplist=1)`, multi-threaded insert.
- **Notes:** White-box test; directly calls internal `__wt_search_insert`. The table uses `memory_page_max=1TB` to prevent splits, keeping the entire data set in one insert list.

## LazyFS Variant
None.
