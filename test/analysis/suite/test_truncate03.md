# test_truncate03 — Address-deleted cells: page freeing and empty page instantiation

**File:** `test/suite/test_truncate03.py`
**Storage mode:** General
**Components under test:** address-deleted cells, btree page lifecycle, fast delete, empty page instantiation

## Test Cases

### `test_truncate_address_deleted.test_truncate_address_deleted_free`
- **What it tests:** Inserts a dataset, truncates all records, checkpoints, then does a second checkpoint; verifies that address-deleted pages are freed (not simply marked deleted) after the second checkpoint confirms no reader can see them.
- **Components:** `btree.c`, `checkpoint.c`, `block.c`
- **Notes:** Exercises the two-checkpoint protocol for freeing address-deleted disk blocks.

### `test_truncate_address_deleted.test_truncate_address_deleted_empty_page`
- **What it tests:** Inserts a dataset, truncates all records, then immediately reads back (forcing page instantiation from the address-deleted state); verifies all keys return `WT_NOTFOUND`.
- **Components:** `btree.c`, `page.c`
- **Notes:** Confirms that instantiating an address-deleted page as an empty in-memory page works correctly and does not expose stale data.
