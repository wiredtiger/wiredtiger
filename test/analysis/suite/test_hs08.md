# test_hs08 — History store: modify insertion logic and squashing of same-timestamp modifies

**File:** `test/suite/test_hs08.py`
**Storage mode:** General
**Components under test:** history store, modify, checkpoint, statistics (cache_write_hs, cache_hs_write_squash)

## Test Cases

### `test_hs08.test_modify_insert_to_hs`
- **What it tests:** Tests the logic for writing modifies to the history store, with multiple checkpoint-driven scenarios:

  **Phase 1:** Inserts a 1,000-byte base value (ts=2), applies 3 modifies in separate transactions (ts=3,4,5), checkpoints, verifies `cache_write_hs >= 1` and `cache_hs_write_squash == 0`. Reads at ts=3, 4, 5 and checks exact values (`value1+'A'`, `value1+'AB'`, `value1+'ABC'`).

  **Phase 2:** Applies 2 more modifies (ts=7, 8), checkpoints again. Verifies `hs_writes >= 2` and `squashed_write == 0`. Reads at ts=7 (`value1+'DBC'`) and ts=8 (`value1+'DEC'`) to confirm correct HS forward-scan reconstruction.

  **Phase 3:** Inserts 3 modifies within one transaction (ts=9). Checkpoints. Asserts `squashed_write == 1` (the first two were squashed).

  **Phase 4:** Two transactions each with 2 modifies at ts=10 and ts=11. Checkpoints. Asserts `squashed_write == 4` (cumulative: the previous squash plus two new ones, plus one more from the earlier squashed set).

  **Phase 5:** Two transactions each with 2 modifies but with per-modify timestamps (ts=12,13 and ts=14,15) so each modify has a distinct timestamp. Checkpoints. Asserts `squashed_write == 5` (no additional squashing in this phase).
- **Components:** `src/history/`, `src/modify/`, `src/checkpoint/`
- **Notes:** Scenarios: key_format ∈ {`r`, `i`}; `cache_size=100MB,statistics=(all)`. Squashing occurs when multiple modifies in the same transaction share a timestamp.
