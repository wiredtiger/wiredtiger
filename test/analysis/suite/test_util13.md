# test_util13 — wt dump: preservation of non-default table configuration in dump/load roundtrip

**File:** `test/suite/test_util13.py`
**Storage mode:** General
**Components under test:** `wt dump`, `wt load`, dump header format, table configuration preservation

## Test Cases

### `test_util13.test_dump_config`
- **What it tests:** Creates a table with a non-default configuration parameter (e.g., `prefix_compression_min=3`, `split_pct=50`, `allocation_size=512B`); dumps it with `wt dump`; verifies the dump header contains the expected WiredTiger version string, format, URI, and configuration substring; loads the dump into a new database directory; re-dumps from that directory; verifies the non-default config parameter is preserved in the re-dump header.
- **Components:** `util_dump.c`, `util_load.c`, `schema.c`, `meta.c`
- **Notes:** Parameterized over file-simple (`prefix_compression_min=3`), table-simple (`split_pct=50`), and table-complex (`allocation_size=512B` with colgroups). Complex datasets strip `colgroups` and `columns` from config comparison to enable key=value parsing. The `compare_config` method parses actual vs. expected config into dicts and checks that expected keys are a subset of actual keys.
