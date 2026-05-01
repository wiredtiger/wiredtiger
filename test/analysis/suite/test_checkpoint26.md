# test_checkpoint26 — Eviction in parallel with checkpoint (timing stress)

**File:** `test/suite/test_checkpoint26.py`
**Storage mode:** General
**Components under test:** checkpoint subsystem, eviction, concurrency, statistics

## Test Cases

### `test_checkpoint26.test_checkpoint26`
- **What it tests:** Verifies that pages can be evicted in parallel with an ongoing checkpoint (`timing_stress_for_test=[checkpoint_evict_page]`) and that the `eviction_pages_in_parallel_with_checkpoint` statistic is incremented, confirming the concurrent eviction code path is exercised.
- **Components:** `src/evict/evict_page.c`, `src/checkpoint/checkpoint.c`
- **Notes:** Uses `timing_stress_for_test=[checkpoint_evict_page]` to artificially trigger evictions during checkpoint. After the checkpoint completes, reads `stat.conn.eviction_pages_in_parallel_with_checkpoint` and asserts it is greater than zero. Tests that the checkpoint-concurrent-eviction path is correctly instrumented.
