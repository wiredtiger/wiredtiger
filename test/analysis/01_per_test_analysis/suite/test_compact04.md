# test_compact04 — Accuracy of compact work estimation (pages_rewritten vs pages_rewritten_expected)

**File:** `test/suite/test_compact04.py`
**Storage mode:** General (skips tiered)
**Components under test:** compaction subsystem, estimation/prediction, statistics

## Test Cases

### `test_compact04.test_compact04`
- **What it tests:** Verifies that the compaction estimation phase (`btree_compact_pages_rewritten_expected`) is within 15% of the actual pages rewritten (`btree_compact_pages_rewritten`) for a realistic delete+compact workload, running up to 10 iterations until at least one success with no more than 2 failures.
- **Components:** `src/block/block_compact.c`, `src/session/session_compact.c`
- **Notes:** Skip: tiered (gathers stats but returns early). Creates fresh tables per iteration. Populates 100 000 rows, deletes 4 ranges of 10 000 keys each (spread across the file). Enables verbose `compact_progress` and `compact:4`. After `session.compact()`, reads both stats and computes relative error `|actual - expected| / actual`. Test terminates early on first clean success (≤0 failures); tolerates up to 2 failures across 10 runs. Deliberately allows statistical variance in the estimation.
