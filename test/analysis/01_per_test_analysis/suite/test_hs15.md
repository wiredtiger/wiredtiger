# test_hs15 — History store: eviction does not re-clear HS after checkpoint already cleared it (non-ts update interaction)

**File:** `test/suite/test_hs15.py`
**Storage mode:** General
**Components under test:** history store, eviction, checkpoint, non-timestamped updates, modify

## Test Cases

### `test_hs15.test_hs15`
- **What it tests:** Inserts key 1 without a timestamp. Adds 998 other keys to trigger eviction. Applies a modify (ts=1) and a full update (ts=2) on key 1. Advances oldest=1 to make the non-ts update and ts=1 modify obsolete. Checkpoints (checkpoint should clean up the obsolete HS records for key 1). Inserts a final update at ts=3. Adds more keys to trigger eviction again. Reads at ts=1, 2, and 3 and verifies correctness: the HS records set by checkpoint should not be cleared a second time by subsequent eviction. Tests that eviction doesn't corrupt the HS state that checkpoint already managed.
- **Components:** `src/history/`, `src/evict/`, `src/checkpoint/`, `src/modify/`
- **Notes:** Scenarios: key_format ∈ {`r`, `S`}; cache_size=5MB. Tagged `history_store:eviction_checkpoint_interaction`. This is a regression test for a bug where eviction would incorrectly remove HS entries that checkpoint had already processed.
