# test_tiered17 — Readonly connection and readonly cursor do not create new tiered objects

**File:** `test/suite/test_tiered17.py`
**Storage mode:** Tiered and non-tiered (all scenarios)
**Components under test:** readonly connection (`readonly=true`), readonly cursor (`readonly=true`), object file count invariance during read-only access, clean vs unclean shutdown before readonly open

## Test Cases

### `test_tiered17.test_open_readonly_conn`
- **What it tests:** After populating a tiered table and performing a checkpoint+flush, records the current set of `.wtobj` and `.wt` files. Then reopens the connection with `readonly=true` and verifies that no new object files are created. After closing the readonly connection, again verifies the file count has not changed. Covers both clean shutdown (no unflushed data) and unclean shutdown (dirty data in memory that was never checkpointed) before the readonly open.
- **Components:** `src/tiered/conn_tiered.c` (readonly open path), `src/conn/conn_open.c`, tiered manager startup inhibition in readonly mode
- **Notes:**
  - Parametrized across all storage sources (dir_store, s3_store, gcp_store, azure_store, non_tiered) × shutdown mode (clean vs unclean). In the unclean case, extra data (`"c"`, `"d"`) is inserted after the flush but before the connection is closed — no checkpoint is done for those writes.
  - `get_object_files` scans the current directory for `*.wtobj` and `*.wt` files.

### `test_tiered17.test_open_readonly_cursor`
- **What it tests:** After populating and flushing a tiered table, records the file count. Reopens the connection in normal mode (not readonly) and opens a cursor with `readonly=true`. Verifies no new object files are created after opening the cursor, after closing the cursor, or after closing the connection.
- **Components:** `src/tiered/tiered_handle.c` (readonly cursor open path), `src/cursor/cur_file.c`
- **Notes:**
  - Same parametrization as `test_open_readonly_conn`.
  - The unclean scenario inserts data `"c"` and `"d"` without a checkpoint before reopening. This means only the data up to the flush is visible through the checkpoint cursor, but the test focuses on object file count not data content.
  - `verify_checkpoint` (called in `test_open_readonly_conn`) opens a checkpoint cursor (`checkpoint=WiredTigerCheckpoint`) and confirms this also does not create new object files.
