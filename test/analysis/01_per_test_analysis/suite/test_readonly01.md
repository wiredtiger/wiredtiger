# test_readonly01 — Read-only connection mode: basic read correctness and permission handling

**File:** `test/suite/test_readonly01.py`
**Storage mode:** General
**Components under test:** read-only connection, logging, file permissions

## Test Cases

### `test_readonly01.test_readonly`
- **What it tests:** Creates a table, inserts 10,000 entries, closes the connection; optionally chmods the database directory to 0555 (read-only filesystem); reopens the database with `readonly=true`; reads all 10,000 entries and verifies they are correct; for the dirchmod case, expects a 'Permission' string in stderr (since WiredTiger cannot write lock files to the read-only directory)
- **Components:** `conn/conn_open.c`, `cursor/cur_std.c`, `log/log.c`
- **Notes:** Scenarios: basecfg/no_basecfg × write/readonly directory permissions × logging/no_logging × file-row/file-var/table-row/table-var; the basecfg scenario writes a WiredTiger.basecfg file before reopening; the dirchmod scenario uses `os.chmod('.', 0o555)` to make the directory read-only; the test is careful to chmod back to 0o777 in teardown to avoid breaking test cleanup
