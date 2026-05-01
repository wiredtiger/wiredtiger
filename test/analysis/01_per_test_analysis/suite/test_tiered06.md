# test_tiered06 — Storage source extension low-level API: flush, read, file-system lifecycle

**File:** `test/suite/test_tiered06.py`
**Storage mode:** Tiered
**Components under test:** `StorageSource` C API (`ss_customize_file_system`, `ss_flush`, `ss_flush_finish`, `ss_terminate`), `FileSystem` API (`fs_exist`, `fs_directory_list`, `fs_open_file`, `fs_rename`, `fs_remove`, `fs_size`), `FileHandle` API (`fh_read`, `fh_size`, `fh_lock`, `fh_close`), cache directory, multiple concurrent file systems

## Test Cases

### `test_tiered06.test_ss_basic`
- **What it tests:** Basic round-trip of the storage source API. Creates a local file, flushes it to the bucket with `ss_flush` + `ss_flush_finish`, then reads it back via `fs_open_file` / `fh_read`. Verifies: (1) `fs_exist` returns false before flush and true after; (2) `fs_directory_list` is empty before flush and contains the file after; (3) `fh_read` content matches what was written; (4) `fh_size` / `fs_size` return correct lengths; (5) `fh_lock` is a no-op; (6) `fs_rename` is unsupported on flushed objects; (7) `fs_remove` is unsupported on cloud backends but succeeds on dir_store; (8) trying to open a non-existent file on cloud backends returns an appropriate error.
- **Components:** `ext/storage_sources/dir_store`, `ext/storage_sources/s3_store`, `ext/storage_sources/gcp_store`, `ext/storage_sources/azure_store`, `src/tiered/tiered_handle.c`
- **Notes:** Parametrized across all tiered backends. Each test method appends its own function name to `bucket_prefix` to avoid namespace collisions between test methods within the same class instance.

### `test_tiered06.test_ss_write_read`
- **What it tests:** Non-sequential write and read of a multi-block file (1 000 blocks × 4 096 B) via the storage source API. Writes blocks filled with `'a'`, `'b'`, or `'c'` bytes in interleaved patterns (reverse, forward-even, backward-every-third), flushes to storage, then reads back non-sequentially and verifies block content. The entire read loop is executed twice, with the cache file deleted between iterations to force a re-download from the bucket.
- **Components:** `ext/storage_sources/*`, cache directory logic (download-on-miss)
- **Notes:** Exercises random-access read (`fh_read` at arbitrary offsets) and the cache re-population path. Cache directory is named `<bucket>_cache`.

### `test_tiered06.test_ss_file_systems`
- **What it tests:** Multi-file-system isolation: creates two independent `FileSystem` objects (`fs1` → bucket, `fs2` → bucket1) with separate cache directories, then flushes different files into each and verifies that directory listings, home-directory contents, and local-object directories remain independent. Also tests: (1) `ss_customize_file_system` with a bad bucket name raises an error; (2) dir_store rejects a plain file path as a bucket; (3) double-flush (overwrite) of the same object is rejected on dir_store (EEXIST); (4) prefix filtering in `fs_directory_list`; (5) `fs1.terminate` while `fs2` is still live; (6) `ss.terminate` without terminating all file systems.
- **Components:** `ext/storage_sources/dir_store`, `src/tiered/` file-system management
- **Notes:** dir_store-specific overwrite guard (EEXIST) is noted as not yet implemented for cloud backends (FIXME-WT-11004). Two cache directories (`./cache1`, `./cache2`) are created manually.
