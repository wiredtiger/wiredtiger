# test_tiered13 — Import of tiered tables is unsupported and returns appropriate errors

**File:** `test/suite/test_tiered13.py`
**Storage mode:** Tiered
**Components under test:** `session.create` with `import=(enabled,...)`, error handling for tiered object import via table URI, file URI, and renamed object URI

## Test Cases

### `test_tiered13.test_tiered13`
- **What it tests:** Confirms that all import paths for tiered tables return appropriate errors, not silent data corruption. Sequence: (1) create a tiered table, insert data, force-flush (creating object 1), insert more, plain checkpoint (creating object 2), then close the connection; (2) export metadata for the file-2 object and for the table; (3) set up a fresh import database; (4) copy the `.wtobj` file into the import DB; (5) attempt six different import variations and verify each raises the correct error:
  - `table:uri` with `import=(enabled,repair=true)` → ENOENT (table file not found; tiered not detected at this stage)
  - `table:uri` with `file_metadata=` → "import for tiered storage is incompatible with the 'file_metadata' setting"
  - `file:uri` (object URI) with `repair=true` → "Operation not supported"
  - `file:uri` with `file_metadata=` → "import for tiered storage is incompatible with the 'file_metadata' setting"
  - Renamed object (`other.wt`) via `file:` with `file_metadata=` → "import for tiered storage is incompatible with the 'file_metadata' setting"
  - (Commented out) Renamed object via `file:` with `repair=true` — blocked by FIXME-WT-8644.
- **Components:** `src/schema/schema_create.c` (import path), `src/tiered/tiered_handle.c`, `src/block/block_tiered.c`, import validation code
- **Notes:**
  - Parametrized across all tiered storage backends.
  - Inherits from `test_import_base` (from `test_import01`) for helper methods such as `copy_file`.
  - The FIXME-WT-8644 path (renaming + import without metadata) is commented out due to an error path bug in `wt_bm_read`.
