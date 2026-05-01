# test_encrypt06 — Encryption effectiveness: no cleartext leakage on disk

**File:** `test/suite/test_encrypt06.py`
**Storage mode:** General (skipped for tiered storage)
**Components under test:** encryptors (rotn, sodium), btree, block manager, column groups, indices

## Test Cases

### `test_encrypt06.test_encrypt`
- **What it tests:** Creates two tables with various combinations of column groups and indices, inserts 1,000 records with known distinguishable plaintext strings, closes the connection to flush everything to disk, then scans all files in the run directory looking for the plaintext strings as raw bytes. Asserts that data, key-column names, and value-column names are only visible on disk when the applicable encryption is `none`, and are not visible when encryption is active.
- **Components:** `src/block/`, `src/btree/`, `src/schema/`, `ext/encryptors/rotn`, `ext/encryptors/sodium`
- **Notes:** Scenarios: 6 encrypt configurations (none, rotn-implied, rotn-all, rotn-sys, rotn-table0, sodium-implied) × 7 storage types (table, table+index, table+cg, table+cg+index, and three "unmatch" variants where CG/index encryption is not explicitly set). The `match=False` case tests a misuse scenario. `visible_name()` accounts for the special case where key names are stored in system metadata (which may be encrypted separately). Skipped under tiered storage hook.
