# test_layered78 — cursor.remove() returns WT_NOTFOUND for non-existent key on layered table

**File:** `test/suite/test_layered78.py`
**Storage mode:** Disagg/Layered
**Components under test:** Layered cursor remove operation, `WT_NOTFOUND` return for missing keys

## Test Cases

### `test_layered78.test_delete_non_existent_key`
- **What it tests:** On a freshly created layered table (no data inserted), begins a transaction, sets cursor key to 1, calls `cursor.remove()`, and verifies it returns `WT_NOTFOUND`. Rolls back the transaction.
- **Components:** `src/cursor/cur_layered.c`, ingest btree remove path
- **Notes:** Minimal regression test for correct `WT_NOTFOUND` handling on empty layered tables. Connection starts as follower (`role="follower"`). `layered:` URI with `precise_checkpoint=true`.
