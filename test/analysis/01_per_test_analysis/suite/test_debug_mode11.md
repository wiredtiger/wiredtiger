# test_debug_mode11 — Tests close config debug.skip_checkpoint suppresses shutdown checkpoint

**File:** `test/suite/test_debug_mode11.py`
**Storage mode:** General
**Components under test:** debug mode, checkpoint, shutdown checkpoint, connection close

## Test Cases

### `test_debug_mode11.test_skip_shutdown_checkpoint_restart_visibility`
- **What it tests:** Verifies the behavioral difference between closing with and without the shutdown checkpoint. Two scenarios (parameterized via `make_scenarios`):
  1. **with_shutdown_checkpoint** (`close_cfg=""`): After writing a key after the last explicit checkpoint and then closing normally, the key is visible on reopen because the shutdown checkpoint persisted it.
  2. **without_shutdown_checkpoint** (`close_cfg="debug=(skip_checkpoint=true)"`): The same key is NOT visible on reopen because the shutdown checkpoint was skipped.
  In both scenarios the key written before the explicit checkpoint (`ckpt_1st`) is always visible.
- **Components:** `src/checkpoint/`, `src/conn/conn_debug.c`, `src/schema/`
- **Notes:** Skipped for tiered storage hook. Uses `WiredTigerCursor` context manager from `helper`. Scenarios: `with_shutdown_checkpoint` and `without_shutdown_checkpoint`. The `skip_checkpoint` config is passed to `close_conn()`, not to `wiredtiger_open`.
