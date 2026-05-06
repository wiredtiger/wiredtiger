# test_tiered03 — Block-log-structured tree configuration and secondary-database sharing (skipped)

**File:** `test/suite/test_tiered03.py`
**Storage mode:** Tiered
**Components under test:** tiered storage connection config (cache_directory), secondary/read-only database sharing via metadata copy, checkpoint metadata, `session.alter`

## Test Cases

### `test_tiered03.test_sharing`
- **What it tests:** (Permanently skipped — `self.skipTest(...)` at entry) Was intended to verify that a secondary WiredTiger database can share tiered objects produced by a primary by copying the `file:` URI metadata and opening the table as `readonly=1`. Also tested that a secondary can follow checkpoint bumps via `session.alter`. The skip message: "Sharing the checkpoint file containing transaction ids is not supported."
- **Components:** `src/tiered/` metadata, `conn_tiered.c`, checkpoint metadata, `session.alter`, secondary connection open
- **Notes:**
  - Parametrized across all tiered storage backends (dir_store, s3_store, gcp_store, azure_store) and two record-count scenarios: 10 records (90% probability) and 10 000 records (10% probability) to vary flush workload. Scenarios are pruned to at most 100 (500 in long mode).
  - Connection config sets an absolute bucket path (to allow sharing across two connection home directories) and a relative `cache_directory`.
  - No active assertions run at test time due to the skip; the test body is retained as a specification of intended behaviour.
