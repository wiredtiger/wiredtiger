# test_bug007 — Forced salvage recovers a file with a corrupt/random header

**File:** `test/suite/test_bug007.py`
**Storage mode:** General
**Components under test:** salvage, file header validation

## Test Cases

### `test_bug007.test_bug007`
- **What it tests:** Creates an empty file, then overwrites it with `'random data' * 100` (an invalid file header). Asserts that `session.salvage()` without `force` raises `WiredTigerError` with message `/WT_SESSION.salvage/`. Confirms that `session.salvage(uri, "force")` succeeds, recovering the file despite the corrupt header.
- **Components:** `src/session/session_api.c`, `src/salvage/salvage.c`, `src/block/block_mgr.c`
- **Notes:** Non-parametrized. File-only test (`file:test_bug007`).
