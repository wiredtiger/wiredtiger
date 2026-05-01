# test_hs30 — History store: non-timestamped tables with active history and long-running readers

**File:** `test/suite/test_hs30.py`
**Storage mode:** General
**Components under test:** history store, non-timestamped updates, eviction, checkpoint, statistics (cache_hs_read)

## Test Cases

### `test_hs30.test_insert_updates_hs`
- **What it tests:** Creates a table (optionally with logging). Writes initial data (value_a). Opens reader session2 in a long-running transaction. Writes two more update rounds (value_b, value_c). Optionally checkpoints ("middle" checkpoint). Opens reader session3 at that point (seeing value_c). Writes two more rounds (value_d, value_e). Optionally evicts all pages. Validates:
  - Session2 still sees value_a throughout.
  - Session3 still sees value_c throughout.
  
  After both readers finish, checks the `cache_hs_read` stat:
  - If eviction was done: at least `nrows * 2` HS reads (for both reader transactions).
  - If no eviction: 0 HS reads (old in-memory updates hang around and HS is not accessed).
- **Components:** `src/history/`, `src/evict/`, `src/checkpoint/`
- **Notes:** Scenarios: key_format ∈ {`r`, `i`} × logging ∈ {T,F} × early_ckpt ∈ {T,F} × middle_ckpt ∈ {T,F} × do_evict ∈ {T,F} = 32 scenarios. Addresses case (2) from the module comment: eviction of a page with a long-running reader holding an old snapshot forces HS usage for non-timestamped data. Uses `isolation=snapshot` session config.
