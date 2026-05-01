# test_hs01 — History store: HS inserts from updates, modifies, and timestamp-based updates; durability

**File:** `test/suite/test_hs01.py`
**Storage mode:** General
**Components under test:** history store, checkpoint, recovery, btree, modify

## Test Cases

### `test_hs01.test_hs`
- **What it tests:** Three scenarios exercising the history store across checkpoint and recovery:

  **Scenario 1 — Old reader pins old version while new updates move to HS:**
  Inserts 10,000 rows. Opens a long-running reader transaction (session2). Applies bulk updates (bigvalue2) on session1 and checkpoints. Asserts `cache_hs_insert == nrows-1`, `cache_hs_key_processed == nrows-1`, `cache_hs_update_processed == nrows-1`. Simulates crash/recovery via file copy and verifies the recovered value is `bigvalue2`. Rolls back session2.

  **Scenario 2 — Modify operations moved to HS:**
  Opens another long-running reader. Applies two modify operations (replacing first/second byte with 'A') on all rows. Checkpoints. Asserts total `cache_hs_insert == (nrows-1)*3` (cumulative). Verifies recovered value equals `bigvalue3` (original with first two bytes replaced by 'AA'). 

  **Scenario 3 — Timestamp-based updates, stable_timestamp controls durable version:**
  Sets `stable_timestamp=1`. Applies timestamped updates (bigvalue4) at `commit_timestamp=i+1`. Checkpoints. Asserts `hs_writes == (nrows-1)*4`. Verifies recovery shows `bigvalue3` (old stable value). Advances stable_timestamp and re-checkpoints; verifies recovery now shows `bigvalue4`.

- **Components:** `src/history/`, `src/checkpoint/`, `src/txn/`, `src/btree/`, `src/modify/`
- **Notes:** Scenarios: `key_format` ∈ {`r`, `i`, `S`}; `value_format=u`. Uses `copy_wiredtiger_home` for crash simulation. Ignores stdout pattern `"oldest id .* pinned in session"`.
