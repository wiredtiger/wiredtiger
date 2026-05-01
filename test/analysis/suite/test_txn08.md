# test_txn08 — Printlog Unicode output and LSN range filtering

**File:** `test/suite/test_txn08.py`
**Storage mode:** General
**Components under test:** `wt printlog`, Unicode value encoding, JSON output, LSN range `-l` option

## Test Cases

### `test_txn08.test_printlog_unicode`
- **What it tests:** Inserts 5 keys with Unicode control characters (`abcd`) into a logged table; runs `wt printlog -u` and verifies the output contains the escaped Unicode characters and is valid JSON; runs `wt printlog -u -x` and checks the hex-encoded version is also present; tests LSN range filtering with `-l start,end` in multiple cases: valid range, invalid LSN (expects `WT_NOTFOUND`), start > end (prints only start LSN), missing argument (expects usage error), offset=0 normalization, start==end.
- **Components:** `log.c`, `printlog.c`, `os_fs.c`
- **Notes:** Parameterized over column (key_format='r') and row (key_format='i'). Tests both Unicode encoding in log output and all edge cases of the `-l` LSN range option.
