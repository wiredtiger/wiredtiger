# test_hs33 — History store: recovery after crash before metadata sync; eviction during metadata recovery

**File:** `test/suite/test_hs33.py`
**Storage mode:** General
**Components under test:** history store, recovery, checkpoint, eviction, metadata

## Test Cases

### `test_hs33.test_hs_recovery`
- **What it tests:** Simulates WT-14376/WT-14391: the HS file was opened by eviction before metadata recovery completed, causing an incorrect checkpoint to be loaded.

  Sequence:
  1. Creates 99 tables (all non-logged).
  2. Inserts bigvalue into each.
  3. Opens a long-running session2 transaction while session1 applies bigvalue2 updates (moving data to HS).
  4. Configures `timing_stress_for_test=[checkpoint_stop]` to pause checkpoint before metadata sync.
  5. Starts a background checkpoint thread.
  6. Polls until the checkpoint reaches the stress point (`checkpoint_stop_stress_active`).
  7. Copies the database directory (simulating a crash mid-checkpoint).
  8. Completes checkpoint, rolls back session2.
  9. Reopens the copy with `cache_size=1MB,eviction_dirty_trigger=2,eviction_dirty_target=1` to maximize eviction pressure during metadata log replay.
  10. The fact that the connection opens successfully (without panic) is the pass condition.
- **Components:** `src/history/`, `src/checkpoint/`, `src/conn/`, `src/evict/`, `src/log/`
- **Notes:** No scenarios. Uses `checkpoint_thread` from `wtthread`. Uses `copy_wiredtiger_home` for crash simulation. The eviction pressure configuration is critical to reproduce the original bug. This tests that recovery correctly prevents the HS from being opened until after metadata recovery completes.
