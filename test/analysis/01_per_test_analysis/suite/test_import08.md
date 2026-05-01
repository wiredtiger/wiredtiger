# test_import08 — Imported file transaction IDs are ignored regardless of write generation

**File:** `test/suite/test_import08.py`
**Storage mode:** General
**Components under test:** schema/import, btree write generation, transaction visibility, metadata

## Test Cases

### `test_import08.test_import_write_gen`
- **What it tests:** Verifies that after importing a file that was written with high transaction IDs (from a different database instance), all records remain visible in the new database even though the new connection starts transaction IDs from 1. This validates the per-btree write generation mechanism that replaces the obsolete connection-wide base write gen comparison.
- **Components:** `src/schema/schema_create.c`, `src/btree/bt_read.c`, `src/txn/txn.c`, `src/meta/meta_ckpt.c`
- **Notes:** Parameterized by:
  - `file_metadata` — uses `import=(enabled,repair=false,file_metadata=(...))`
  - `repair` — uses `import=(enabled,repair=true)`

  Setup deliberately pins a transaction ID via a second session doing a remove (forcing reconciliation to write IDs to disk). Checkpoints after each insert to accumulate write generation increments. After import to a fresh DB, asserts `write_gen > 1` in the imported table's metadata, and verifies all values are visible.
