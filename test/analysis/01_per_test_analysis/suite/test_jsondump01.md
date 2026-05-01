# test_jsondump01 — JSON dump utility output and JSON load round-trip

**File:** `test/suite/test_jsondump01.py`
**Storage mode:** General
**Components under test:** utilities (`wt dump -j`, `wt load -j`), JSON cursor, schema

## Test Cases

### `test_jsondump01.test_jsondump_util`
- **What it tests:** Dumps a populated table using the `wt dump -j` utility, parses the output with Python's `json` module, spot-checks specific row values, and verifies all data using a `FakeCursor` adapter against the dataset's `check_cursor` method.
- **Components:** `src/utilities/util_dump.c`, `src/cursor/cur_json.c`
- **Notes:** **Currently skipped** — `self.skipTest('Known failure in JSON cursor')` (FIXME-WT-9986). Parameterized by:
  - `types`: file, table-simple, table-index, table-complex (using `SimpleDataSet`, `SimpleIndexDataSet`, `ComplexDataSet`)
  - `keyfmt`: integer (`i`), recno (`r`), string (`S`)
  2500 entries per table. `FakeCursor` iterates JSON `data` array fields in fixed column order.

### `test_jsondump01.test_jsonload_util`
- **What it tests:** Dumps a table with `wt dump -j`, loads it into a second table name with `wt load -j`, verifies the second table matches the original dataset, then reloads into the original URI, re-dumps, and compares both dump files byte-for-byte.
- **Components:** `src/utilities/util_dump.c`, `src/utilities/util_load.c`, `src/cursor/cur_json.c`
- **Notes:** **Currently skipped** — same FIXME-WT-9986. For `recno` key format, the load command appends `-a` (append mode). Uses `compare_files` helper to confirm idempotent round-trip.
