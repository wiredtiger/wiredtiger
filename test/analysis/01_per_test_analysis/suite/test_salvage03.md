# test_salvage03 — Salvage with corrupted metadata files

**File:** `test/suite/test_salvage03.py`
**Storage mode:** General
**Components under test:** salvage, metadata, recovery, connection open

## Test Cases

### `test_salvage03.test_corrupt_meta`
- **What it tests:** Removes various combinations of metadata files (`WiredTiger`, `WiredTiger.wt`, `WiredTiger.turtle`, `WiredTigerHS.wt`) and tests whether the database is openable (without salvage) or requires salvage. Verifies correct outcomes: some missing-file combinations are recoverable normally, others require `salvage=true`, and some are unrecoverable.
- **Components:** `src/conn/conn_open.c`, `src/meta/`, `src/btree/bt_salvage.c`
- **Notes:** Tests multiple removal scenarios systematically. Distinguishes between files that are required for normal open vs. files whose absence triggers salvage path. Key files tested: `WiredTiger` (connection config), `WiredTiger.wt` (metadata btree), `WiredTiger.turtle` (checkpoint metadata), `WiredTigerHS.wt` (history store).
