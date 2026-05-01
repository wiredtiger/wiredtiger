# test_layered86 — Follower step-up uses file ID high-water mark from leader

**File:** `test/suite/test_layered86.py`
**Storage mode:** Disagg/Layered
**Components under test:** File ID high-water mark, follower step-up table ID assignment, metadata ID continuity

## Test Cases

### `test_layered86.test_standby_uses_table_id_high_water_mark`
- **What it tests:** Leader creates 100 layered tables, checkpoints, records the maximum file ID seen across all metadata entries. Drops all 100 tables, checkpoints again. Opens a follower and advances it to the latest checkpoint. Kills the leader with `skip_checkpoint=true` (to avoid double-free of same-ID pages on PALite). Follower steps up to leader, creates one new table (`test_layered86_101`), checkpoints. Verifies that the new table's file ID exceeds the previously recorded maximum from the old leader — confirming the file ID counter persists through the step-up and does not restart from 0.
- **Components:** `src/conn/conn_disagg.c`, file ID counter persistence, follower step-up ID tracking
- **Notes:** Without the high-water mark, a follower stepping up might reuse file IDs that were already used (and dropped) by the old leader, causing ID collisions in PALite. `metadata_helper.extract_id()` is used to extract IDs from metadata values.
