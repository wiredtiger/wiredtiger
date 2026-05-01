# test_bug024 — WT-6526: readonly connection open succeeds when turtle.set exists

**File:** `test/suite/test_bug024.py`
**Storage mode:** General
**Components under test:** turtle file, readonly connection open

## Test Cases

### `test_bug024.test_bug024`
- **What it tests:** Verifies that a readonly `wiredtiger_open` succeeds even when `WiredTiger.turtle.set` exists alongside `WiredTiger.turtle` (simulating a crash between turtle file writes). Populates 10 rows, closes the connection, copies `WiredTiger.turtle` to `WiredTiger.turtle.set`, then opens the same home directory with `readonly` mode. Asserts no error is raised.
- **Components:** `src/conn/conn_open.c`, `src/meta/meta_turtle.c`
- **Notes:** Non-parametrized. Tagged `connection_api:turtle_file`. Skipped for `tiered` and `disagg` hooks.
