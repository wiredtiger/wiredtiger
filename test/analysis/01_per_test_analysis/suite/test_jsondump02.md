# test_jsondump02 — JSON cursor direct read/write and dump/load round-trip

**File:** `test/suite/test_jsondump02.py`
**Storage mode:** General (skipped for tiered storage)
**Components under test:** JSON cursor (`dump=json`), utilities (`wt dump -j`, `wt load -j`), schema, index, colgroup

## Test Cases

### `test_jsondump02.test_json_cursor`
- **What it tests:** Directly uses JSON cursors (`dump=json` option) to read and write data to tables with various formats. Verifies JSON serialization of strings (including Unicode/UTF-8), binary data, multi-value columns, complex tables with column groups and indexes. Tests error handling for malformed JSON input. Also tests the dump/load utility round-trip for each individual table and for all tables combined in one file.
- **Components:** `src/cursor/cur_json.c`, `src/utilities/util_dump.c`, `src/utilities/util_load.c`, `src/schema/schema_colgroup.c`, `src/schema/schema_index.c`
- **Notes:** **Currently skipped** — `self.skipTest('Known failure in JSON cursor')` (FIXME-WT-9986). Skipped for tiered storage.

  Tables covered:
  - `table_uri1` — `key_format=S,value_format=S` with Unicode/escape characters (`π`, `઼`)
  - `table_uri2` — `key_format=S,value_format=iS` (two-value columns)
  - `table_uri3` — `key_format=r,value_format=u` (recno key, binary value)
  - `table_uri4` — complex `key_format=iS,value_format=SiSi` with two column groups (`c1`, `c2`) and three indexes (`by-Skey`, `by-S3`, `by-i2i4`)
  - `table_uri5/6` — used in `test_json_all_bytes`

  Error cases tested on `table_uri2`:
  - Unknown token `<>abc?`
  - Invalid Unicode `\u` escape variants (7 forms)
  - Unterminated string
  - Bad syntax (missing colon, wrong key name)
  - Type mismatch (string where int expected, int where string expected)
  - Extra trailing content
  - Fields out of order (not currently supported)
  - Extraneous/missing whitespace (allowed)

### `test_jsondump02.test_json_all_bytes`
- **What it tests:** Generates all 256 possible byte values as keys and values in both binary (`u`) and string (`S`) formats, serializes via JSON cursor, and verifies exact Unicode escape sequence output. Round-trips through JSON cursor load and `wt dump -j`/`wt load -j` utility.
- **Components:** `src/cursor/cur_json.c`, `src/utilities/util_dump.c`, `src/utilities/util_load.c`
- **Notes:** **Currently skipped** — same FIXME-WT-9986. String format (`S`) replaces null bytes and bytes >= 0x80 with `'X'`. Verifies printable characters appear as-is and control characters use `\f`, `\n`, `\r`, `\t` escapes. Tests only up to 0x80 due to Python3 Unicode-awareness of strings.
