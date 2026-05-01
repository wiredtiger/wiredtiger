# test_tiered16 — session.drop remove_shared: removing shared bucket objects on drop

**File:** `test/suite/test_tiered16.py`
**Storage mode:** Tiered and non-tiered (all scenarios)
**Components under test:** `session.drop` with `remove_shared=true/false`, bucket and cache directory cleanup on drop, error for `remove_files=false,remove_shared=true` combination, drop after connection reopen

## Test Cases

### `test_tiered16.test_remove_shared`
- **What it tests:** Validates the `remove_shared` configuration option for `session.drop` on tiered tables:
  1. **Error case:** Calling `drop` with `remove_files=false,remove_shared=true` (or same with `force=true`) raises an error ("drop for tiered storage object must configure removal of underlying files") when tiered is active.
  2. **dir_store only — shared removal:** Creates two tiered tables (`tiereda`, `tieredb`). Inserts data and does two forced flushes on `tieredb` (producing two objects). Then: `drop(tiereda, remove_files=true,remove_shared=true)` — verifies that the bucket and cache directories are now empty of `tiereda` objects and still contain the two `tieredb` objects. Then `drop(tieredb, ...)` — verifies both bucket and cache are empty.
  3. **All scenarios — drop after reopen:** Creates `tieredc`, inserts, flushes, reopens connection, inserts more, then drops with `dropUntilSuccess` to handle any pending checkpoint work.
- **Components:** `src/tiered/tiered_handle.c` (drop with remove_shared), `src/tiered/conn_tiered.c` (work units for shared object removal), `ext/storage_sources/dir_store`, bucket + cache directory management
- **Notes:**
  - Parametrized across all storage sources (dir_store, s3_store, gcp_store, azure_store, non_tiered).
  - `remove_shared` is only fully tested on dir_store (not yet implemented for S3/GCS/Azure).
  - Extension config overrides `tiered_extension_config` to force `cache=1` (enable cache directory).
  - Cache directory is `"cache-" + self.bucket` (the default when no explicit cache_directory is set).
  - The extension config `cache=1` ensures cache files are created and thus verifiable.
