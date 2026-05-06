# test_verify_disagg — SESSION::verify() correctness for leader and follower in disagg storage

**File:** `test/suite/test_verify_disagg.py`
**Storage mode:** Disagg (`disagg_only=True`)
**Components under test:** `session.verify()`, layered table metadata, leader/follower role management, history store (HS), checkpoint propagation via `disagg_advance_checkpoint`

## Test Cases

### `test_verify_disagg.test_verify_disagg`
- **What it tests:** The primary end-to-end verify scenario covering: verify on an empty leader table, verify after follower creation (before any checkpoint), verify that the follower returns `ENOENT` before its first checkpoint is loaded, verify after an empty checkpoint, verify that dirty data on the leader returns `EBUSY`, verify after checkpointing the leader, and verify that the follower correctly sees data after `disagg_advance_checkpoint`. Also closes and re-verifies the leader alone after the follower is torn down.
- **Components:** `src/session/session_api.c` (verify), `src/conn/conn_layered_ingest.c`, `src/cursor/cur_layered.c`, history store (`src/history/`)
- **Notes:**
  - **Scenarios:** 2 HS variants (`empty` / `populated`) × N storage variants = up to 2N combinations. The `populated` scenario uses commit timestamps on every write to drive history store population, exercising verify when the HS is non-empty.
  - Verifies 10,000 key/value rows with up to 3 updates per key to ensure HS entries are generated for the `fill_hs=True` scenario.
  - The sequence of expected errors is load-bearing: `ENOENT` before the follower has a checkpoint, `EBUSY` while the leader has uncommitted dirty pages, success at all other points.
  - Uses `disagg_advance_checkpoint` (from `DisaggConfigMixin`) to push the leader's checkpoint LSN to the follower via `conn.reconfigure(disaggregated=(checkpoint_meta=...))`.

### `test_verify_disagg.test_verify_leader_no_table`
- **What it tests:** Confirms that calling `session.verify()` on a layered URI that has never been created returns `ENOENT` on the leader.
- **Components:** `src/session/session_api.c` (verify), `src/cursor/cur_layered.c`
- **Notes:** Minimal negative test; no table creation, no data, no checkpoint. Runs across all HS and storage scenarios. Checks the error-path branch in verify when the table metadata is entirely absent.

### `test_verify_disagg.test_verify_follower_no_metadata`
- **What it tests:** Verifies the follower's verify behavior in the specific transient state where the layered table exists on the leader but the follower has not yet received a checkpoint. Before the checkpoint is propagated the follower must return `ENOENT`; after propagation it must succeed.
- **Components:** `src/session/session_api.c` (verify), `src/conn/conn_layered_ingest.c`, `src/cursor/cur_layered.c`
- **Notes:** Creates the table and verifies on the leader (success), opens a follower, verifies on the follower (expects `ENOENT`), checkpoints the leader, calls `disagg_advance_checkpoint`, then re-verifies the follower (expects success). Tests the metadata propagation boundary without any data rows.

### `test_verify_disagg.test_verify_follower_no_checkpoint`
- **What it tests:** Verifies the follower's behavior when the layered table URI is created on the follower itself (via `session_follow.create`) before the follower has received any leader checkpoint. In this state the stable constituent does not exist, so verify should silently succeed (catches `ENOENT` on the stable table and returns 0). After checkpoint propagation full success is expected.
- **Components:** `src/session/session_api.c` (verify), `src/conn/conn_layered_ingest.c`, `src/cursor/cur_layered.c`
- **Notes:** This test covers the documented follower behavior that followers can only create their ingest constituents and only see stable data through checkpoint or step-up. The subtle distinction from `test_verify_follower_no_metadata` is that here the follower has created the layered URI (so the URI is recognized) but lacks the stable file — the code is expected to tolerate a missing stable file gracefully rather than returning `ENOENT`.
