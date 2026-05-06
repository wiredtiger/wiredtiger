# test_metadata_cursor01 — Basic metadata cursor operations (forward/backward iteration and search)

**File:** `test/suite/test_metadata_cursor01.py`
**Storage mode:** General
**Components under test:** metadata cursor (`metadata:`, `metadata:create`), cursor iteration, cursor search

## Test Cases

### `test_metadata_cursor01.test_forward_iter`
- **What it tests:** Opens a metadata cursor and iterates forward through all metadata entries, verifying that each entry has a non-null key and value. Confirms cursor returns `WT_NOTFOUND` at end and that `reset()` clears the current position.
- **Components:** `src/cursor/cur_metadata.c`, `src/meta/`
- **Notes:** Parameterized by `metauri`: `metadata:` (plain) and `metadata:create` (create-mode). Creates one table before iteration.

### `test_metadata_cursor01.test_backward_iter`
- **What it tests:** Opens a metadata cursor and iterates backward through all metadata entries using `cursor.prev()`, verifying non-null key/value at each step and `WT_NOTFOUND` at exhaustion.
- **Components:** `src/cursor/cur_metadata.c`
- **Notes:** Both `metadata:` and `metadata:create` variants.

### `test_metadata_cursor01.test_search`
- **What it tests:** Verifies that searching the metadata cursor by key finds both the special `metadata:` entry and a user-created table entry. Both must have a value containing the string `"key_format"`.
- **Components:** `src/cursor/cur_metadata.c`, `src/meta/`
- **Notes:** Both `metadata:` and `metadata:create` variants. Uses Python dict-style access (`cursor['metadata:']`, `cursor['table:...']`).
