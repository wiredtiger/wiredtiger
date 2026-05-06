# test_verify_disagg02 — Detection of duplicate btree IDs among follower stable files

**File:** `test/suite/test_verify_disagg02.py`
**Storage mode:** Disagg (`disagg_only=True`)
**Components under test:** `session.verify()` btree-ID uniqueness check, follower local metadata (`WiredTiger.wt`), layered stable file management

## Test Cases

### `test_verify_disagg02.test_verify_duplicate_btree_ids`
- **What it tests:** Verifies that `session.verify()` on a follower detects and rejects a situation where two stable-file metadata entries share the same btree ID. The test injects a synthetic duplicate by copying the real stable file's metadata config string (which includes `,id=N`) under a fake URI, then asserts that verify raises `WT_ERROR` with a `metadata corruption` / `stable table verification failed` indication.
- **Components:** `src/session/session_api.c` (verify), `src/meta/` (metadata read), `src/conn/conn_layered_ingest.c`, `src/cursor/cur_layered.c`
- **Notes:**
  - **Scenarios:** N storage variants from `gen_disagg_storages(..., disagg_only=True)` — one scenario per configured page log.
  - **Injection mechanism:** Opens `file:WiredTiger.wt` directly as a raw cursor on the follower, reads the real stable file config from `metadata:` (containing the `id=N` field), and inserts an additional entry under the key `file:fake_duplicate.wt_stable` with the identical config. This simulates metadata corruption without modifying on-disk pages.
  - After the verify error is confirmed, the fake entry is removed so that the test framework's own teardown verification (which calls verify internally) does not fail.
  - `ignoreStderrPatternIfExists` suppresses expected error-log output for `metadata corruption` and `stable table verification failed`.
  - Significant because it provides targeted coverage for the btree-ID uniqueness invariant enforced inside `verify()` for disaggregated layered tables — a guard against metadata corruption that could cause two stable constituents to share a B-tree ID and produce silent data corruption or crashes.
