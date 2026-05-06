# test_live_restore01 — Live restore compatibility with various connection options

**File:** `test/suite/test_live_restore01.py`
**Storage mode:** General (Unix only; Windows path returns early)
**Components under test:** live restore, connection configuration validation, file system

## Test Cases

### `test_live_restore01.test_live_restore01`
- **What it tests:** Exhaustively verifies that `live_restore=(enabled=true,...)` succeeds with valid configurations and fails with appropriate errors for incompatible connection options. Also tests multi-round start/stop behavior.
- **Components:** `src/live_restore/`, `src/conn/conn_open.c`, `src/conn/conn_reconfig.c`
- **Notes:** All sub-cases in one test method. Successes:
  - Valid path: `live_restore=(enabled=true,path=SOURCE)` → OK
  - `in_memory=true` with `enabled=false` → OK
  - `threads_max=12` (max allowed) → OK
  - `threads_max=0` (minimum) → OK

  Failures (each checks for specific error message):
  - Windows → `"Live restore is not supported on Windows"`
  - `in_memory=true` + enabled → `"Live restore is not compatible with an in-memory connection"`
  - Empty path → `"No such file or directory"`
  - Nonexistent path → error contains path string
  - `threads_max=13` (exceeds limit) → `"Value too large for key"`
  - `readonly=true` → `"live restore is incompatible with readonly mode"`
  - `salvage=true` → `"Live restore is not compatible with salvage"`
  - `statistics=(none)` → `"Statistics must be enabled when live restore is active."`
  - `disaggregated=(page_log=palite)` → `"Live restore is not compatible with disaggregated storage mode"`
  - Multi-round: start with `threads_max=0`, then reopen with `enabled=false` while in-progress → `"Cannot start in non-live restore mode while a live restore is in progress!"`
  - `conn.reconfigure("statistics=(none)")` while live restore active → same statistics error
