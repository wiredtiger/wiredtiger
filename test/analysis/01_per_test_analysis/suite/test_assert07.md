# test_assert07 — Reserved updates at various positions in update chain do not trigger false assertions

**File:** `test/suite/test_assert07.py`
**Storage mode:** General
**Components under test:** cursor reserve, update chain reconciliation, timestamps

## Test Cases

### `test_assert07.test_timestamp_alter`
- **What it tests:** Verifies that the "resolved update" assertion is not incorrectly fired when `cursor.reserve()` calls appear at various positions within the same transaction's update chain. Tests all combinations: reserve at chain start with one update; reserve at chain end with one update; reserve at start with multiple updates; reserve at end with multiple updates; reserve sandwiched between updates; reserve with multiple extra updates before and after; multiple reserves in one transaction.
- **Components:** `src/cursor/cur_std.c`, `src/btree/bt_walk.c`, `src/txn/txn.c`
- **Notes:** Parametrized across `column` (r key) and `string-row` (S key) formats. All transactions use `prepare_transaction` + explicit `commit_timestamp` + `durable_timestamp`. The test is purely a regression guard — it should complete without any assertion failures.
