# wt3120_filesys — Custom file system extension smoke test

**Path:** `test/csuite/wt3120_filesys/`
**Language:** C
**Storage mode:** General
**Jira ticket:** WT-3120
**Components under test:** File system extension API, `fail_fs` shared library, `extensions` connection config, `early_load`

## What This Test Does
This test is a minimal smoke test for the WiredTiger custom file system extension mechanism. It loads the `fail_fs` extension (a test-only file system that can be configured to inject failures) via the `extensions` connection configuration, performs a few inserts, closes and reopens the connection without the extension, and verifies the inserted records are still readable. The primary validation is that loading a custom file system extension and closing the connection does not crash or corrupt data.

## Test Scenarios / Cases

### Scenario: Load fail_fs, insert data, reopen without extension
- **What it tests:** That the custom file system extension can be loaded via `early_load=true`, that basic insert operations work through the custom file system, and that after closing and reopening without the extension the data remains intact and readable.
- **Components:** `fail_fs` `.so` extension, `extensions` connection config, `wiredtiger_open`, `cursor->insert`, `cursor->next`.
- **Notes:** The test does not inject any failures — it only verifies the load/unload/reopen lifecycle. The `fail_fs` path is resolved from the build directory.

## LazyFS Variant
None.
