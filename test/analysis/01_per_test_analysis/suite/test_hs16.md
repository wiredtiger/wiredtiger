# test_hs16 — History store: no panic when inserting non-timestamped update into HS

**File:** `test/suite/test_hs16.py`
**Storage mode:** General
**Components under test:** history store, checkpoint, non-timestamped updates

## Test Cases

### `test_hs16.test_hs16`
- **What it tests:** Creates a simple table, inserts key 1 without a timestamp (valuea), updates at ts=1 (valueb), opens session2 to pin the oldest transaction ID (preventing global visibility of the next step), applies another non-timestamped update to key 1 (valuec), applies an update at ts=2 (valued). Then calls `session.checkpoint()`. The test asserts this sequence does not panic — specifically that writing a non-timestamped update to the history store during checkpoint is handled correctly.
- **Components:** `src/history/`, `src/checkpoint/`, `src/txn/`
- **Notes:** Scenarios: key_format ∈ {`r`, `S`}; cache_size=5MB. The pinning via session2 ensures the OOO (out-of-order) update is not globally visible at checkpoint time, which is the condition that triggers the HS insertion code path in question.
