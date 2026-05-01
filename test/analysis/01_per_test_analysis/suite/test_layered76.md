# test_layered76 — Checkpoint size verification across various dataset sizes

**File:** `test/suite/test_layered76.py`
**Storage mode:** Disagg/Layered
**Components under test:** Checkpoint size tracking, `session.verify()` (`verifyUntilSuccess()`)

## Test Cases

### `test_layered76.test_ckpt_size_verify_simple`
- **What it tests:** Inserts 1 key (no timestamp), checkpoints, then calls `verifyUntilSuccess()`. Validates that verify passes for the minimal case.
- **Components:** `src/conn/conn_ckpt.c`, checkpoint size metadata, `src/session/session_verify.c`
- **Notes:** `layered:` URI with integer keys and string values. Disagg-only.

### `test_layered76.test_ckpt_size_verify_multi_insert`
- **What it tests:** Inserts 10 keys (each 100 bytes), checkpoints, then calls `verifyUntilSuccess()`.
- **Components:** Checkpoint size metadata
- **Notes:** Small multi-key insert to verify the size tracking is correct for a handful of entries.

### `test_layered76.test_ckpt_size_verify_large_dataset`
- **What it tests:** Inserts 100,000 keys (each 100 bytes), checkpoints, then calls `verifyUntilSuccess()`. Tests checkpoint size tracking under a large dataset that spans many pages.
- **Components:** Checkpoint size metadata, multi-page checkpoint writes
- **Notes:** The largest single-checkpoint scenario in this file.

### `test_layered76.test_ckpt_size_verify_many_ckpt`
- **What it tests:** Inserts 10,000 string-key entries, checkpoints. Updates every even-indexed key, checkpoints. Updates every 100th key, checkpoints. Then calls `verifyUntilSuccess()`. Tests that size metadata remains consistent across three checkpoint rounds with partial updates.
- **Components:** Checkpoint size metadata, multi-checkpoint delta tracking
- **Notes:** Uses string keys to exercise a different key format. Tests cumulative size correctness across multiple checkpoint rounds.
