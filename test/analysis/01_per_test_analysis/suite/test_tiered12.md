# test_tiered12 — flush_tier does not wait for the background flush_finish thread

**File:** `test/suite/test_tiered12.py`
**Storage mode:** Tiered
**Components under test:** flush_tier return-before-flush_finish behaviour, `timing_stress_for_test=(tiered_flush_finish)`, background tiered-manager thread, local object presence after flush

## Test Cases

### `test_tiered12.test_tiered`
- **What it tests:** Verifies that `checkpoint('flush_tier=(enabled,force=true)')` returns to the caller as soon as the object is copied to shared storage, without waiting for the internal background thread to call `flush_finish` (which moves the object from the local staging area to the cache directory). The test uses the `tiered_flush_finish` timing stress, which delays the internal thread's `flush_finish` call by 1 second. After the flush call returns, the test confirms the bucket object already exists on dir_store (`bucket/<prefix><base>1.wtobj`), then sleeps 2 seconds to allow the background thread to run and complete `flush_finish`. This validates that flush_tier's completion contract is "data is in shared storage" not "background housekeeping is done".
- **Components:** `src/tiered/conn_tiered.c` (flush_tier work units vs flush_finish work units), tiered manager background thread, `src/support/timing_stress.c` (`tiered_flush_finish` stress point), storage_source
- **Notes:**
  - Parametrized across all tiered storage backends.
  - `local_retention=1` used so removed objects do not linger.
  - `force=true` required because no prior checkpoint exists yet (first flush would otherwise be a no-op).
  - The sleep after the flush gives the background thread time to process, avoiding a race in the teardown.
