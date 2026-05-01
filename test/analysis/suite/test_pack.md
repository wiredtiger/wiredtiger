# test_pack — Value packing format correctness via cursor insert and lookup

**File:** `test/suite/test_pack.py`
**Storage mode:** General
**Components under test:** packing/unpacking, cursor API, index cursors

## Test Cases

### `test_pack.test_packing`
- **What it tests:** Verifies that structured value formats are correctly packed on insert and unpacked on retrieval; covers integer packs (`iii`, `3i`), mixed integer+string (`iS`), string-only formats (`S`, `9S`, `9SS`, `42S`, `10SS`), raw byte (`u`, `uu`, `3u`), and fixed-length string (`s`, `1s`, `2s`)
- **Components:** `packing/pack_api.c`, `packing/pack_impl.c`, `cursor/cur_std.c`
- **Notes:** Each format is tested by inserting a record, searching by key, and verifying the retrieved value matches; also tests secondary index cursors where the index key includes packed value columns; exercises boundary cases for fixed-length formats (single char, two chars) and repeated-format shorthand (e.g., `3i` vs explicit `iii`)
