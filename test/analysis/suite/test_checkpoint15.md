# test_checkpoint15 — Three checkpoints with per-checkpoint timestamp restrictions

**File:** `test/suite/test_checkpoint15.py`
**Storage mode:** General
**Components under test:** checkpoint cursor, timestamps, oldest/stable/read timestamp semantics

## Test Cases

### `test_checkpoint.test_checkpoint`
- **What it tests:** Verifies that each named checkpoint enforces its own `oldest_timestamp`, `stable_timestamp`, and `read_timestamp` restrictions independently. Reading from an older checkpoint at a `read_timestamp` that was valid when that checkpoint was taken succeeds; reading at a timestamp before that checkpoint's `oldest_timestamp` fails.
- **Components:** `src/checkpoint/`, `src/cursor/cur_btree.c`, `src/txn/txn_timestamp.c`
- **Notes:** Creates three named checkpoints at different stable timestamps. Each checkpoint cursor enforces its own `oldest_timestamp` lower bound. Verifies that timestamp boundaries are per-checkpoint (not global), allowing independent per-checkpoint read ranges. Tests a key semantic property of the named checkpoint API.
