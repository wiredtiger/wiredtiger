# test_log06 — Recovery from partial log record (pre-allocated log block corruption)

**File:** `test/suite/test_log06.py`
**Storage mode:** General with logging and no fsync (`log=(enabled),transaction_sync=(enabled=true,method=none)`)
**Components under test:** log recovery, partial write detection (`__log_has_hole`, `__log_record_verify`), log salvage

## Test Cases

### `test_log06.test_recovery_from_partial_log_record`
- **What it tests:** Simulates a crash mid-write leaving a partially-flushed 128-byte log block in the WAL. Verifies that recovery detects the corruption, emits the expected NOTICE message, truncates the log, and replays committed transactions that preceded the corruption.
- **Components:** `src/log/log.c`, `src/log/log_verify.c`, `src/log/log_salvage.c`
- **Notes:** Parameterized by which bytes in the 128-byte aligned block are non-zero (2 scenarios):
  - `record_len` — bytes 4-7 are `\xde\xad\xbe\xef`, bytes 0-3 are `\x00` (zero length); triggers `"record len corruption 0x0"`.
  - `flag` — bytes 8-9 are `\x04\x00` (unknown bit 2 in flags field), bytes 0-3 are `\x00`; triggers `"flag corruption 0x4"`.

  Test phases:
  1. Insert 100 rows as `value_a`, checkpoint (makes `value_a` durable).
  2. Insert 100 rows as `value_b` (in WAL but not checkpointed).
  3. Copy the live database to `RESTART/` while connection is open (no clean-shutdown marker), then append the corruption block to every log file.
  4. Close the original connection. Open a new connection on `RESTART/` — expects the scenario-specific NOTICE in stdout.
  5. Verify all 100 rows carry `value_b` (phase-2 WAL replay succeeded despite the partial block).
