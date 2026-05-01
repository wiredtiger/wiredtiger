# test_dictionary03 — Dictionary reuse despite time window (timestamp) metadata

**File:** `test/suite/test_dictionary03.py`
**Storage mode:** General
**Components under test:** btree reconciliation, dictionary compression, time windows, timestamps

## Test Cases

### `test_dictionary03.test_dictionary03`
- **What it tests:** Verifies that a value cell that shares a value with an existing dictionary entry is correctly reused even when it carries a non-trivial time window (commit_timestamp=20) due to being written inside a timestamped transaction. Inserts two seed values, then commits one matching value at timestamp 20. After checkpoint, confirms `rec_dictionary` count = 1.
- **Components:** `src/btree/bt_rec.c`, `src/btree/bt_rec_dict.c`, `src/txn/`
- **Notes:** Timestamps are pinned (`oldest_timestamp=1`, `stable_timestamp=1`) to prevent time windows from being eliminated by global visibility, keeping them present in the reconciled page. Scenarios: `row` and `var`. Tags: `compression`.
