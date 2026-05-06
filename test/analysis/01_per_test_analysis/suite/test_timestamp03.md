# test_timestamp03 — Timestamps with logging: checkpoint, backup, and metadata

**File:** `test/suite/test_timestamp03.py`
**Storage mode:** General
**Components under test:** timestamps with logging, checkpoint `use_timestamp`, backup, metadata verification

## Test Cases

### `test_timestamp03.test_timestamp03`
- **What it tests:** Creates four tables (logged+timestamps, not-logged+timestamps, logged+no-timestamps, not-logged+no-timestamps); inserts, updates, and a third round of updates; verifies read visibility with and without timestamps across all four table types at multiple points; takes named checkpoints with `use_timestamp=true/false`; takes backups after each checkpoint and verifies how many instances of each value version appear in each table. Also verifies via metadata cursor that `log=(enabled=true/false)` and the HS file config is correct.
- **Components:** `txn_timestamp.c`, `checkpoint.c`, `log.c`, `backup.c`, `schema.c`
- **Notes:** Skipped for disagg (log tables disabled). Parameterized over file/table URIs × column/row × three checkpoint configs (default, use_timestamp=false, use_timestamp=true) × two log versions (V1 compatibility, V2). Tests `log_flush` before checkpoint to verify backup durability of logged vs non-logged tables.
