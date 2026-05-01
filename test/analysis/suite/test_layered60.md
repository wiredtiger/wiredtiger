# test_layered60 — Creating empty tables while a checkpoint is running

**File:** `test/suite/test_layered60.py`
**Storage mode:** Disagg/Layered
**Components under test:** checkpoint, schema (concurrent table creation), conn_layered_ingest.c, stable btree, follower checkpoint pick-up, restart without local files

## Test Cases

### `test_layered60.test_layered60`
- **What it tests:** Verifies that creating an empty layered table concurrently with an in-progress checkpoint does not corrupt state. Uses `timing_stress_for_test=[checkpoint_slow]` to make the checkpoint take at least 10 seconds, starts the checkpoint in a background thread, waits for it to begin, then creates a second (empty) table in the main thread while the checkpoint is still running. After the checkpoint completes, takes another checkpoint and then: (a) opens a follower, advances the checkpoint, and verifies the empty table exists and has zero records; (b) calls `restart_without_local_files(step_up=True)` and verifies the empty table still exists with zero records.
- **Components:** checkpoint (concurrent schema operation safety), schema / table creation (conn_layered_ingest.c), stable btree, follower checkpoint pick-up (`disagg_advance_checkpoint`), `restart_without_local_files`
- **Notes:** Uses `table:` URI with `type=layered` (not `layered:` prefix). Uses `precise_checkpoint=true` and requires a stable timestamp before checkpointing. The concurrent-creation thread joins before the final checkpoint, so the race window is only during the slow first checkpoint. Two-part verification: follower path and cold-restart path. Disagg-only.
