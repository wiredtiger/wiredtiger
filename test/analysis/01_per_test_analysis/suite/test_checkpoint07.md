# test_checkpoint07 — Clean checkpoint timer across modified and unmodified tables

**File:** `test/suite/test_checkpoint07.py`
**Storage mode:** General
**Components under test:** checkpoint subsystem, clean-checkpoint optimization, backup cursor, statistics

## Test Cases

### `test_checkpoint07.test_checkpoint07`
- **What it tests:** Verifies that the `btree_clean_checkpoint_timer` is set appropriately for unmodified tables (skipped) vs modified tables (not skipped), and that the timer is reset when a backup cursor is open (preventing the clean-checkpoint optimization from skipping needed tables).
- **Components:** `src/checkpoint/checkpoint.c`, `src/btree/bt_walk.c`, `src/backup/`
- **Notes:** Three sub-scenarios: (1) unmodified table after first checkpoint — clean timer is set and table is skipped; (2) table modified then checkpointed — timer cleared; (3) backup cursor open — timer is reset so backup can capture the table. Checks `stat.conn.txn_checkpoint_skipped` or equivalent clean-checkpoint stats.
