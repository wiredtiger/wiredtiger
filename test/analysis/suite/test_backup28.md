# test_backup28 — Selective backup restore rejects colgroup/index URIs in target list

**File:** `test/suite/test_backup28.py`
**Storage mode:** General
**Components under test:** backup cursor (selective), partial restore, schema validation

## Test Cases

### `test_backup28.test_backup28`
- **What it tests:** Creates a table with column groups and indexes, takes a full (non-selective) backup, then attempts to open the backup with `backup_restore_target` lists containing: a `file:` URI, a `table:` URI (valid), an `index:` URI + table URI (invalid), or a `colgroup:` URI + table URI (invalid). Asserts that only the `table:` URI list succeeds; all other types raise `WiredTigerError` with `/partial backup restore only supports objects of type .* formats in the target uri list/`.
- **Components:** `src/conn/conn_open.c`, `src/meta/meta_table.c`
- **Notes:** Parametrized across 4 URI-prefix × target combinations (file, table-simple, table-cg with index target, table-index with colgroup target).
