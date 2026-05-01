# test_modify02 — modify fails without a base value (WT_NOTFOUND)

**File:** `test/suite/test_modify02.py`
**Storage mode:** General
**Components under test:** `cursor.modify`, partial update semantics, error handling

## Test Cases

### `test_modify02.test_modify02`
- **What it tests:** Verifies that calling `cursor.modify` on a key that has no existing base value returns `WT_NOTFOUND`. This confirms that modify is not equivalent to insert — it requires an existing record to apply partial updates to.
- **Components:** `src/cursor/cur_modify.c`, `src/btree/bt_cursor.c`
- **Notes:** Parameterized by value format:
  - `item` — `value_format='u'` (byte array)
  - `string` — `value_format='S'` (string)

  Uses `random.Random(43)`. For 1000 iterations:
  - Generates random modify descriptors via `create_mods` (same parameters as test_modify01).
  - Does NOT insert the base value (`c[k]` is never set to `oldv`).
  - Calls `cursor.modify(mods)` inside a transaction.
  - Asserts return code is `wiredtiger.WT_NOTFOUND`.
  - Commits the transaction (no-op since the modify failed, but verifies commit doesn't error).

  Edge case: partial update semantics strictly require an existing value; there is no upsert behavior.
