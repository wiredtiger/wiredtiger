# test_import12 — Import file using MongoDB server's dry-run pattern (create, checkpoint, alter, drop, reimport)

**File:** `test/suite/test_import12.py`
**Storage mode:** General
**Components under test:** schema/import, schema/alter, schema/drop, checkpoint, metadata

## Test Cases

### `test_import12.test_file_import`
- **What it tests:** Simulates the MongoDB server's import workflow: import to a temporary URI as a "dry run" (with alter and checkpoint cycles), drop without removing files, then re-import to the same temporary URI. Iterates the cycle with varying checkpoint counts (0, 1, 2) to expose a known issue where multiple forced checkpoints can invalidate the root page of the on-disk file, requiring fallback to `repair=true`.
- **Components:** `src/schema/schema_create.c`, `src/schema/schema_alter.c`, `src/schema/schema_drop.c`, `src/meta/meta_ckpt.c`
- **Notes:** `max_ckpt = 2` — runs the loop 3 times (ck=0,1,2). Each iteration:
  1. Creates a fresh destination DB, populates it, sets oldest timestamp.
  2. Copies the source `.wt` file to the destination under a different name (`new_db_file`).
  3. Imports with `import=(enabled,repair=false,file_metadata=(...))` (first import = dry run).
  4. Does `ck` checkpoint iterations (second is a forced checkpoint).
  5. Alters the table with alternating `access_pattern_hint` values.
  6. Checkpoints after alter.
  7. Drops the imported table with `remove_files=false`.
  8. Re-imports with `panic_corrupt=false`; if this fails with `WT_ERROR` (known issue with forced checkpoints — `FIXME-WT-13639`), falls back to `repair=true`.
  9. Verifies object and checks data integrity.

  Ignores specific stderr patterns (`"failed to read .* bytes at offset"`) and stdout patterns (`'extent list'`) that may appear after the failed reimport attempt.
