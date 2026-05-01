# test_key_provider_disagg02 — Key provider metadata integrity after crash during checkpoint

**File:** `test/suite/test_key_provider_disagg02.py`
**Storage mode:** Disagg (PALite only; non-PALite scenarios are skipped via `skipTest`)
**Components under test:** key provider extension, checkpoint crash injection, layered/disagg block manager, PALite SQLite shard storage

## Test Cases

### `test_key_provider_disagg02.test_key_provider_disagg02`
- **What it tests:** Verifies that a crash injected at three distinct points during a checkpoint does not corrupt the key provider's KEK metadata stored in the PALite turtle shard. After recovery the metadata (page_id, LSN, and version) must be byte-for-byte identical to the pre-crash snapshot.
- **Components:** `ext/page_log/palite/`, `src/checkpoint/`, `src/conn/conn_layered_ingest.c`, key provider test extension (`test/` extension library `key_provider`), `suite_subprocess` test infrastructure
- **Notes:**
  - **Scenarios:** 1 storage variant (PALite, `disagg_only=True`) × 3 crash-point variants = up to 3 scenario combinations per configured page_log; only PALite is exercised at runtime.
  - **Crash injection points** (passed via `debug=(checkpoint_crash_trigger_point=...)` to `session.checkpoint()`):
    - `before_key_rotation` — crash before the KEK rotation step begins
    - `during_key_rotation` — crash in the middle of KEK rotation
    - `after_key_rotation` — crash after KEK rotation but before checkpoint completion
  - Uses `suite_subprocess` / `run_subprocess_function` to run the workload in a subprocess, ensuring the crash (`SIGKILL` / process exit) truly kills the database without running teardown.
  - Pre-crash KEK metadata is written to a file (`key_provider.results`) by `sqlite_fetch_shared_meta(write=True)`. After subprocess exit, the parent process re-reads the PALite shard and compares `page_id`, `lsn`, and `version` fields using a regex; all three must match.
  - Significant because it proves the key provider write is atomic with respect to the checkpoint: a crash at any checkpoint phase must leave the persisted KEK unchanged.
