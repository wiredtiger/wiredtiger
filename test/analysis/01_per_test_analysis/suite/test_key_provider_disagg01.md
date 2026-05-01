# test_key_provider_disagg01 — Basic key provider lifecycle and persistence under crash/restart

**File:** `test/suite/test_key_provider_disagg01.py`
**Storage mode:** Disagg (PALite only; non-PALite scenarios are skipped via `skipTest`)
**Components under test:** key provider extension, checkpoint, layered/disagg block manager, PALite SQLite shard storage

## Test Cases

### `test_key_provider_disagg01.test_key_provider_disagg01`
- **What it tests:** Verifies the full key provider lifecycle: population of a layered table, checkpointing to trigger key provider KEK (Key Encryption Key) rotation, validation of KEK metadata in the PALite SQLite store, key persistence across both clean reopens and simulated crash-restarts, and correct handling of key expiry settings.
- **Components:** `ext/page_log/palite/`, `src/block/`, `src/checkpoint/`, `src/conn/conn_layered_ingest.c`, key provider test extension (`test/` extension library `key_provider`)
- **Notes:**
  - **Scenarios:** 2 storage variants (from `gen_disagg_storages`) × 2 crash variants (`reopen` / `crash`) = up to 4 scenario combinations per configured page_log; only the PALite storage is exercised (others are skipped at runtime).
  - The test directly queries PALite's SQLite shards via the bundled `sqlite3` binary, verifying that the KEK page ID and version recorded in the turtle shard (`pages_NN.db`) match expected constants (`MAIN_KEK_PAGE_ID=1`, `EXPECTED_KEK_VERSION=1`).
  - Validates that the count of pages in the key provider shard equals the count in the turtle shard when `key_expire=0` (no expiry), and is `>=` when expiry is set (12 hours, `key_expire=43200`), checking that expiry metadata is written without dropping existing entries.
  - The crash path uses `simulate_crash_restart` (helper), re-validating metadata from the `RESTART` directory; the non-crash path uses `reopen_conn`.
  - Shard routing uses the `get_shard_id` helper (modulo `NUM_SHARDS=17` matching PALite's C++ constant).
  - Significant because it is the primary test that exercises the key provider's interaction with the PALite page store across crash-recovery boundaries.
