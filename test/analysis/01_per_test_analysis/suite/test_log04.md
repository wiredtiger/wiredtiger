# test_log04 — Logging and timestamp interaction: logged vs. non-logged tables

**File:** `test/suite/test_log04.py`
**Storage mode:** General (logging enabled: `log=(enabled)`)
**Components under test:** logging, timestamps, rollback_to_stable, table log configuration

## Test Cases

### `test_log04.test_logts`
- **What it tests:** Verifies that commit timestamps only apply to non-logged tables (`log=(enabled=false)`), not to logged tables. Also verifies that rollback_to_stable (RTS) honors timestamps for non-logged tables but not for logged tables, and that non-timestamped non-logged tables behave like logged tables (ignoring timestamps).
- **Components:** `src/log/log.c`, `src/txn/txn_timestamp.c`, `src/txn/txn_rollback_to_stable.c`, `src/btree/`
- **Notes:** Decorated with `@wttest.prevent(["timestamp"])` to avoid timestamp hook interference. Parameterized by:
  - `col` — `key_format='r'`
  - `row` — `key_format='S'`
  - `ckpt` / `no-ckpt` — whether to checkpoint before RTS

  Three tables:
  - `uri_log` — logged table (default, no `log=(enabled=false)`)
  - `uri_ts` — non-logged table with timestamps
  - `uri_nots` — non-logged table without timestamps

  Key observations verified:
  - Logged table always sees the latest write regardless of read timestamp (timestamps ignored)
  - Non-logged+timestamp table sees historical versions at correct read timestamps
  - Non-logged+no-timestamp table behaves like logged (latest always visible)
  - After RTS to stable=25: `uri_ts` at timestamp 30 rolls back to the timestamp-25 value; logged and no-timestamp tables retain the value written at ts=30
