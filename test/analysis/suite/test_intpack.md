# test_intpack — Integer packing correctness for all integer format codes

**File:** `test/suite/test_intpack.py`
**Storage mode:** General
**Components under test:** packing/unpacking (`src/packing/`), index cursor, btree key ordering

## Test Cases

### `test_intpack.test_packing`
- **What it tests:** For a given integer format code, inserts values as both keys and values into forward and backward tables and their associated inverse indexes, and verifies round-trip equality. Covers boundary regions around 0, ±2^32, and (for 64-bit) ±2^64 and power-of-two boundaries from 2^3 to 2^59.
- **Components:** `src/packing/pack_impl.c`, `src/packing/pack_stream.c`, `src/cursor/cur_index.c`
- **Notes:** Parameterized by 10 format codes:
  - `b` / `B` — int8_t / uint8_t (8-bit)
  - `h` / `H` — int16_t / uint16_t (16-bit)
  - `i` / `I` — int32_t / uint32_t (32-bit)
  - `l` / `L` — int32_t / uint32_t (32-bit, `long` aliases)
  - `q` / `Q` — int64_t / uint64_t (64-bit)

  Uses `PackTester` helper class which creates four tables per format: `forw` (int key, format value), `forw_idx` (inverse index), `back` (format key, int value), `back_idx` (inverse index). Checks all four cursors for each value.

  `base_range` is 5000 in normal mode, 66000 in long-test mode. For 32-bit types, also checks the 1000-value ranges around ±2^32. For 64-bit types, additionally checks ±2^64 bounds and power-of-two sweep from 8 to 2^60.
