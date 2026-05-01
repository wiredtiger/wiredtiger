# test_checkpoint30 — Aggregate time window visible to snapshot but individual deletes not all visible

**File:** `test/suite/test_checkpoint30.py`
**Storage mode:** General
**Components under test:** checkpoint cursor, aggregate time window, delete visibility, snapshot

## Test Cases

### `test_checkpoint.test_checkpoint`
- **What it tests:** Verifies the interaction between aggregate time window information on internal pages and individual delete visibility: a checkpoint snapshot may show the aggregate TW as valid (some live keys exist) even when the individual delete timestamps cause some specific keys to be invisible at a given read timestamp.
- **Components:** `src/btree/bt_walk.c`, `src/checkpoint/`, `src/cursor/cur_btree.c`, `src/reconcile/rec_visibility.c`
- **Notes:** Inserts rows, deletes a subset at a specific timestamp, checkpoints. At a `read_timestamp` between insert and delete, all rows are visible. At `read_timestamp` after the delete, deleted rows disappear. Verifies that aggregate TW on internal pages does not cause incorrect skip of pages when some keys are visible and some are not.
