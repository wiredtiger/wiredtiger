# test_timestamp17 — Non-timestamped tombstone covers all timestamped history

**File:** `test/suite/test_timestamp17.py`
**Storage mode:** General
**Components under test:** non-timestamped deletes, history store visibility, oldest_timestamp interaction

## Test Cases

### `test_timestamp17.test_inconsistent_timestamping`
- **What it tests:** Writes key=1 at timestamps 25, 50, 200; reads before ts=25 (WT_NOTFOUND); adds a non-timestamped tombstone; reads at ts=25, 50, 100, 200, 300 all show WT_NOTFOUND (tombstone covers everything). Advances oldest from 49 to 99 to 100 to 200, verifying WT_NOTFOUND at each read. Confirms that history store correctly handles non-timestamped deletes over timestamped updates at all read timestamps.
- **Components:** `txn.c`, `txn_timestamp.c`, `history_store.c`
- **Notes:** Parameterized over integer-row and column formats. Key insight: a no-timestamp tombstone wins over all prior timestamped writes, even when oldest is advanced past those timestamps.
