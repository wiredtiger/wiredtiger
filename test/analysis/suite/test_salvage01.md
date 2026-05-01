# test_salvage01 — Salvage utility and API tests

**File:** `test/suite/test_salvage01.py`
**Storage mode:** General
**Components under test:** salvage, wt utility, cursor, btree, eviction

## Test Cases

### `test_salvage01.test_salvage_api`
- **What it tests:** Verifies that `session.salvage()` returns 0 (success) on a clean table after it has been created and populated, and that data is still accessible via cursor after salvage.
- **Components:** `src/session/session_api.c`, `src/btree/bt_salvage.c`
- **Notes:** Basic API call test. No corruption involved.

### `test_salvage01.test_salvage_api_rand`
- **What it tests:** Invokes `session.salvage()` on random positions within a table (via cursor iteration) to stress the salvage code path during concurrent reads.
- **Components:** `src/session/session_api.c`, `src/btree/bt_salvage.c`
- **Notes:** Uses randomized cursor positions.

### `test_salvage01.test_salvage_damage_inner`
- **What it tests:** Corrupts an inner (non-leaf) page of the btree by overwriting part of the binary file, then calls `session.salvage()` to recover. Verifies that some data is recovered and the table is still openable.
- **Components:** `src/btree/bt_salvage.c`, `src/block/`
- **Notes:** Binary corruption via direct file write. Recovery may not restore all records.

### `test_salvage01.test_salvage_damage_leaf`
- **What it tests:** Corrupts a leaf page of the btree and invokes salvage. Verifies recovery.
- **Components:** `src/btree/bt_salvage.c`, `src/block/`
- **Notes:** Similar to `test_salvage_damage_inner` but targets leaf-level corruption.

### `test_salvage01.test_salvage_eviction_split`
- **What it tests:** Uses a failpoint to simulate a crash during an eviction split. After triggering the split-then-abort scenario, verifies salvage can recover the table correctly.
- **Components:** `src/btree/bt_salvage.c`, `src/evict/`, `src/btree/bt_split.c`
- **Notes:** Requires failpoint support. Tests interaction between eviction/split and salvage recovery.
