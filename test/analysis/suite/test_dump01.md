# test_dump01 — wt dump utility: pretty-hex format validation (keys=pretty, values=hex)

**File:** `test/suite/test_dump01.py`
**Storage mode:** General
**Components under test:** wt utility (dump), pretty-hex dump format

## Test Cases

### `test_pretty_hex_dump.test_dump`
- **What it tests:** Creates a table with binary byte-array values (integer keys, raw byte values). Generates three dump files: hex (`-x`), pretty (`-p`), and pretty-hex (`-px`). Then validates the pretty-hex format by comparing it line-by-line with the hex and pretty dumps:
  - Header lines in pretty-hex must match the pretty dump, except the `Format=` line which must say `Format=print hex`.
  - In the data section, key lines must match the pretty dump (human-readable keys).
  - Value lines must match the hex dump (hex-encoded values).
  - Both dumps must have the same total number of lines.
- **Components:** `src/utilities/util_dump.c`
- **Notes:** Values are pseudo-random bytes generated via a deterministic function using modular arithmetic. 13 records with lengths from 4 to 196 bytes. The `-px` flag combines pretty keys with hex values. Tags: `wt_util`.
