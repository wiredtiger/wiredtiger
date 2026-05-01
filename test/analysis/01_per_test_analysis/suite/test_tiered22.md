# test_tiered22 — Compaction is unsupported on tiered table object files

**File:** `test/suite/test_tiered22.py`
**Storage mode:** Tiered
**Components under test:** `session.compact` rejection on tiered object files, object file existence after table creation

## Test Cases

### `test_tiered22.test_tiered22`
- **What it tests:** Creates a tiered table and verifies that the initial local object file (`tiered-0000000001.wtobj`) exists on disk. Then attempts to call `session.compact` on that object file URI and asserts that this raises a `WiredTigerError` with message "Operation not supported". This confirms that compaction is explicitly blocked for tiered object files.
- **Components:** `src/session/session_api.c` (`WT_SESSION::compact`), `src/compact/compact.c` (tiered object rejection), `src/tiered/tiered_handle.c`
- **Notes:**
  - Parametrized across all tiered storage backends (tiered_only=True).
  - The test accesses the object file via its raw filename string (e.g., `'tiered-0000000001.wtobj'`) rather than a `table:` or `tiered:` URI, reflecting a low-level attempt to compact the underlying object directly.
