# test_layered66 — Verify fails for unmaterialized pages, succeeds after LSN update

**File:** `test/suite/test_layered66.py`
**Storage mode:** Disagg/Layered
**Components under test:** Page materialization, `last_materialized_lsn`, `session.verify()`, page log LSN tracking

## Test Cases

### `test_layered66.test_layered66`
- **What it tests:** Three-phase test of `session.verify()` behavior relative to the last materialized LSN. Phase 1: inserts key=1, checkpoints at stable=1, then advances `last_materialized_lsn` to match the checkpoint LSN via `pl_set_last_materialized_lsn()` and `conn.set_context_uint(WT_CONTEXT_TYPE_LAST_MATERIALIZED_LSN, ...)`. Phase 2: inserts key=2, checkpoints at stable=2 (LSN now lags the checkpoint), then asserts that `session.verify()` raises `WiredTigerError` (unmaterialized pages detected). Phase 3: advances `last_materialized_lsn` to the new checkpoint LSN and verifies that `session.verify()` now succeeds.
- **Components:** `src/conn/conn_disagg.c`, page log extension (`pl_get_last_lsn`, `pl_set_last_materialized_lsn`), `src/session/session_api.c` (verify)
- **Notes:** Tests the invariant that verify must fail when pages in the checkpoint have not yet been materialized (written to durable storage), and succeed once the LSN catches up. Uses `wiredtiger.WT_CONTEXT_TYPE_LAST_MATERIALIZED_LSN` context key. Disagg-only; `layered:` URI.
