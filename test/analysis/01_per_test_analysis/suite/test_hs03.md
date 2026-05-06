# test_hs03 — History store: checkpoint avoids unnecessary HS reads (skewing)

**File:** `test/suite/test_hs03.py`
**Storage mode:** General
**Components under test:** history store, checkpoint, cache eviction, statistics

## Test Cases

### `test_hs03.test_checkpoint_hs_reads`
- **What it tests:** Populates a table with a large initial dataset (100 seed rows + 10,000 extra rows). Sets stable_timestamp=1, then applies 10,000 timestamped updates (bigvalue2) that overflow the 50 MB cache, causing HS writes. Checkpoints. Then in a loop (timestamps 2 and 3), updates only a single record per iteration and checkpoints again. Asserts that `cache_hs_read` increments by at most 200 per checkpoint iteration — verifying that checkpointing only a small change does not cause excessive HS reads (the HS-skewing heuristic is working).
- **Components:** `src/history/`, `src/checkpoint/`, `src/evict/`
- **Notes:** Scenarios: key_format ∈ {`r`, `i`, `S`}; value_format=u; cache_size=50MB, statistics=(fast). The 200 bound is loose to account for concurrent eviction. Uses cumulative stat deltas around each checkpoint call.
