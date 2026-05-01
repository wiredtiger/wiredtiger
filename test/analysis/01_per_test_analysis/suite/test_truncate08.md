# test_truncate08 — No WT_PREPARE_CONFLICT when cursor walks after committed fast truncate with prepare

**File:** `test/suite/test_truncate08.py`
**Storage mode:** General
**Components under test:** fast delete, prepared transactions, cursor iteration, `WT_PREPARE_CONFLICT` regression (WT-6325)

## Test Cases

### `test_truncate08.test_truncate08`
- **What it tests:** Inserts 80,000 rows, forces to disk, then in one transaction: truncates keys 10,000-70,000; modifies key 40,000 (on a fast-truncated page); prepares at ts=10, commits at ts=20 with durable ts=20. Then iterates the entire table with a cursor and verifies no `WT_PREPARE_CONFLICT` is returned (regression test for WT-6325 where walking after a committed prepare+fast-truncate wrongly returned a prepare conflict).
- **Components:** `btree.c`, `txn.c`, `cursor.c`
- **Notes:** Parameterized over column and integer-row formats. Skipped on disagg if fast truncate support is not built.
