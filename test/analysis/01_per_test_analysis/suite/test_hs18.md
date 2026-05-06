# test_hs18 — History store: multiple older-reader scenarios with non-timestamped updates and modifies

**File:** `test/suite/test_hs18.py`
**Storage mode:** General
**Components under test:** history store, eviction, non-timestamped updates, older readers (snapshot isolation), modify

## Test Cases

### `test_hs18.test_base_scenario`
- **What it tests:** A long-running reader (session2) reads key 1 at ts=3. Main session updates at ts=5 and ts=10, evicts the page, then commits a non-ts update (value4) and a ts=15 update (value5). Evicts again. Verifies session2 still sees value0 (ts=3) correctly from the history store. Confirms that non-ts updates placed between timestamped HS records do not corrupt older readers.
- **Components:** `src/history/`, `src/evict/`, `src/txn/`

### `test_hs18.test_read_timestamp_weirdness`
- **What it tests:** Two readers: session2 starts at read_ts=5, session3 starts at read_ts=5 after ts=10 update exists. After eviction and a non-ts update, session2 (started before the ts=10 update) correctly sees value1. Session3 (started after ts=10 update) can now see value2 (the ts=10 update) even though its read_ts is 5 — because the non-ts update removed the HS stop timestamp. Documents this documented "weirdness" in the comment: a timestamp reader can see a newer value after a non-ts update.
- **Components:** `src/history/`, `src/evict/`, `src/txn/`

### `test_hs18.test_ignore_tombstone`
- **What it tests:** Session2 holds an open transaction seeing an initial non-ts update. Main session adds ts=5 and ts=10 updates, evicts, then commits another non-ts update. Evicts again. Verifies session2 still sees the original non-ts value. Tests that a tombstone in the HS is not used to remove the initial non-ts update that session2 is reading.
- **Components:** `src/history/`, `src/evict/`

### `test_hs18.test_multiple_older_readers`
- **What it tests:** Five sessions read key 1 at progressively newer values (0–4). The key is updated at ts=3, 5, 10, then with a non-ts update (value3), then at ts=15. After each update, a new session begins a transaction that pins that version. Eviction is triggered twice. Validates all 4 surviving reader sessions (sessions 0–3) see their expected pinned values from HS.
- **Components:** `src/history/`, `src/evict/`

### `test_hs18.test_multiple_older_readers_with_multiple_missing_ts`
- **What it tests:** Nine sessions, two non-ts updates interspersed with timestamped updates. After each eviction, all surviving readers are validated. Exercises the most complex update-chain scenario with multiple OOO updates mixed with timestamped ones.
- **Components:** `src/history/`, `src/evict/`

### `test_hs18.test_modifies`
- **What it tests:** Five sessions each reading a different version of key 1, where versions are created by a mix of a full insert and prepend modifies. A non-ts full update (value3) is inserted. A final prepend modify is applied. Checkpoint is performed (to update the last_running value used by eviction). Evicts twice. All five readers are validated plus a timestamp-based reader at ts=3 who should see value1 + the first modify.
- **Components:** `src/history/`, `src/evict/`, `src/modify/`, `src/checkpoint/`
- **Notes:** Scenarios: key_format ∈ {`r`, `S`}; cache_size=5MB, eviction=(threads_max=1).
