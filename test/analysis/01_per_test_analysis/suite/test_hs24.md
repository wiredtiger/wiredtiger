# test_hs24 — History store: missing-timestamp deletes/commits racing with checkpoint produce consistent checkpoint

**File:** `test/suite/test_hs24.py`
**Storage mode:** General
**Components under test:** history store, checkpoint, non-timestamped operations, concurrent threads, crash recovery

## Test Cases

### `test_hs24.test_missing_ts`
- **What it tests:** Inserts 2,000 rows at ts=4 and ts=5. Sets stable=5. Spawns a thread that deletes all rows without timestamps (`no_timestamp=true`). After a 3-second delay, calls checkpoint. After the thread joins, simulates a crash-restart. Verifies that the recovered database is consistent: if a row exists at ts=5, it must also have the ts=4 version in HS; if a row does not exist at ts=5, it must not exist at ts=4 either. Tests that missing-timestamp deletes racing with HS checkpoint don't create inconsistencies.
- **Components:** `src/history/`, `src/checkpoint/`, `src/txn/`

### `test_hs24.test_missing_commit`
- **What it tests:** Same initial setup (2,000 rows at ts=4 and ts=5). Sets stable=4. Spawns a thread that overwrites all rows without timestamps. Calls checkpoint concurrently. After crash-restart, reads at ts=4 and verifies: each row either shows value1 (the original ts=4 committed value) or value3 (the non-ts commit from the thread that completed before checkpoint started). The two values should not intermingle row-by-row.
- **Components:** `src/history/`, `src/checkpoint/`, `src/txn/`
- **Notes:** Scenarios: key_format ∈ {`r`, `i`} × checkpoint_stress ∈ {`checkpoint_slow`, `history_store_checkpoint_delay`} = 4 scenarios. Uses `simulate_crash_restart` helper. Thread functions `missing_ts_deletes` and `missing_ts_commits` run concurrently with checkpoint. The `timing_stress_for_test` delays checkpoint to widen the race window.
