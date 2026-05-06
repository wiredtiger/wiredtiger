# test_backup26 — Selective backup at scale: recovery correctness with varying exclusion percentages

**File:** `test/suite/test_backup26.py`
**Storage mode:** General
**Components under test:** backup cursor (selective), partial restore, metadata cleanup

## Test Cases

### `test_backup26.test_backup26`
- **What it tests:** Creates 500 (or 10 000 in long-test mode) tables, populates each with 100 rows. Takes a selective backup excluding a configurable percentage of tables (0%, 10%, 50%, 90%, 100%). Opens the backup with `backup_restore_target` listing the included tables (optionally in reversed order). Verifies that excluded tables raise errors when opened; included tables can be opened and their data matches the originals.
- **Components:** `src/cursor/cur_backup.c`, `src/conn/conn_open.c`, `src/meta/meta_table.c`
- **Notes:** Parametrized across 5 exclusion percentages × 2 list orders (forward/reversed). Measures and logs elapsed recovery time.
