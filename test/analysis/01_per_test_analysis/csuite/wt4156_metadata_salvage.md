# wt4156_metadata_salvage — Metadata file corruption detection and salvage

**Path:** `test/csuite/wt4156_metadata_salvage/`
**Language:** C
**Storage mode:** General
**Jira ticket:** (not specified — general metadata salvage feature test)
**Components under test:** Metadata salvage (`wiredtiger_open` with `salvage=true`), `WiredTiger.wt` corruption, `WiredTiger.turtle` corruption, metadata cursor, `WT_TRY_SALVAGE`

## What This Test Does
This test verifies WiredTiger's ability to detect and recover from a corrupted metadata file (`WiredTiger.wt`) or turtle file (`WiredTiger.turtle`). It creates 8 tables (mix of `file:` and `table:` URIs, row and column formats) plus a deliberately named corrupt table (`file:zzz-corrupt.SS`), each with large `app_metadata` strings to force each entry onto its own page. It then corrupts the metadata or turtle file by scribbling one byte over all occurrences of the target URI string, attempts to open the database (expecting `WT_TRY_SALVAGE`), opens with `salvage=true` (must succeed), and finally opens normally to verify salvaged tables are intact and the corrupt table is absent from metadata.

## Test Scenarios / Cases

### Scenario: Corrupt WiredTiger.wt (metadata file) — detect and salvage
- **What it tests:** That opening a database with a corrupted `WiredTiger.wt` returns `WT_TRY_SALVAGE`, that opening with `salvage=true` successfully reconstructs metadata from remaining good pages, and that non-corrupt tables are readable afterward while `file:zzz-corrupt.SS` is absent.
- **Components:** `corrupt_file(WT_METAFILE, CORRUPT)`, `wiredtiger_open(salvage=true)`, `metadata:` cursor, `verify_metadata`.
- **Notes:** APP_MD_SIZE=4096, APP_BUF_SIZE=3KB to force one entry per metadata page. Bypass for ASAN/TSAN builds.

### Scenario: Corrupt WiredTiger.turtle (turtle file) — detect and salvage
- **What it tests:** Same salvage verification when the turtle file rather than the metadata B-tree file is corrupted. Not all turtle-file corruption combinations lead to a detectable error (prior checkpoint may be recovered), so `open_with_corruption` accepts both `WT_TRY_SALVAGE` and success.
- **Components:** `corrupt_file(WT_METADATA_TURTLE, WT_METAFILE_URI)`, `wiredtiger_open(salvage=true)`.
- **Notes:** `corruption_abort=false` debug flag is used when opening to detect corruption without triggering a diagnostic abort.

## LazyFS Variant
None.
