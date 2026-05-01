# test_rollback_to_stable08 — RTS does not abort updates when stable equals latest commit timestamp

**File:** `test/suite/test_rollback_to_stable08.py`
**Storage mode:** General
**Components under test:** rollback_to_stable, transactions, checkpoint

## Test Cases

### `test_rollback_to_stable08.test_rollback_to_stable`
- **What it tests:** Verifies that RTS performs no aborts when the stable timestamp is set at or above the latest committed timestamp. Writes 10,000 rows at ts=20/30/40/50. Sets stable=50 (non-prepare) or stable=60 (prepare). After checkpoint and RTS, all four values remain visible at their respective timestamps. Stats: `calls=1`, `hs_removed=0`, `upd_aborted=0`, `keys_removed=0`, `keys_restored=0`; `pages_visited=0` for on-disk mode (no page scans needed); `pages_visited>0` for in-memory mode.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/checkpoint/`
- **Notes:** Parametrized on key_format (column/row_integer), in_memory, prepare, worker threads (0/4/8). Key test: on-disk RTS can skip all pages when no updates exceed stable, so `pages_visited=0`. In-memory must still walk pages but nothing is aborted. `cache_size=50MB`.
