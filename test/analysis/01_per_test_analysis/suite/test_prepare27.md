# test_prepare27 — Aborted prepared update not selected as base value after ignore_prepare eviction

**File:** `test/suite/test_prepare27.py`
**Storage mode:** General
**Components under test:** prepared transactions, rollback, eviction, ignore_prepare, rollback_to_stable, history store

## Test Cases

### `test_prepare27.test_prepare27`
- **What it tests:** Commits 5 successive updates on a key (at timestamps 1–5), then prepares a 6th update; evicts the page with `ignore_prepare=true` (so the prepared update is written to disk); rolls back the prepare; runs rollback_to_stable; reads at timestamp 1 and verifies that the aborted prepared update is not visible and the correct committed value is returned
- **Components:** `txn/txn_prepare.c`, `txn/txn_rollback.c`, `evict/evict_page.c`, `rts/rts.c`, `history/hs_cursor.c`
- **Notes:** Scenarios: column/integer-row/string-row; the bug this guards against: when evicting with ignore_prepare, the aborted prepared update could be incorrectly persisted as the "base" value on the page, causing it to be selected as the visible value after RTS; verifies that after rollback+RTS, the value at ts=1 is the first committed value (not the aborted prepared value)
