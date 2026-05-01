# test_layered96 — Stale alternate cursor regression: re-search under new transaction context

**File:** `test/suite/test_layered96.py`
**Storage mode:** Disagg/Layered (disagg_only)
**Components under test:** Layered cursor alternate-cursor caching, read_timestamp change between `next()`/`prev()` calls, snapshot generation when concurrent ingest writes land between two cursor calls

## Test Cases

### `test_layered96.test_talbe_scan_with_different_read_ts_stable_next`
- **What it tests:** Regression for stale alternate-cursor values when `read_timestamp` decreases between `next()` calls. Setup: leader writes key=2 at ts=1 and updates it at ts=2; follower picks up checkpoint; follower ingest adds key=1 at ts=3 (sorts before key=2). A single open cursor runs: T1 (`read_timestamp=3`) calls `next()` and must return key=1/value=3 (from ingest, now current); T2 (`read_timestamp=1`) calls `next()` and must return key=2/value=1 (from stable at ts=1). The bug without the fix was that the stable cursor was left parked at key=2/value=2 (ts=2) from the previous call and its cached value was reused directly under T2's snapshot, silently returning value=2 instead of value=1.
- **Components:** `src/cursor/cur_layered.c` (alternate cursor re-search on transaction context change)
- **Notes:** The test name has a typo: `test_talbe_scan_...` (should be `test_table_scan_...`). The key that is the "alternate" (non-current) cursor during T1 is the stable cursor; the bug occurs when that alternate cursor is not re-positioned under T2's snapshot. Disagg-only.

### `test_layered96.test_talbe_scan_with_different_read_ts_stable_prev`
- **What it tests:** Same regression as above but using `prev()`. Setup: leader writes key=1 at ts=1 and updates at ts=2; follower ingest adds key=2 at ts=3 (sorts after key=1). T1 (`read_timestamp=3`) calls `prev()` → key=2/value=3; T2 (`read_timestamp=1`) calls `prev()` → key=1/value=1 (bug: would return value=2).
- **Components:** `src/cursor/cur_layered.c`, alternate cursor re-search on `prev()` direction
- **Notes:** Mirror of the `_next` case in reverse direction.

### `test_layered96.test_talbe_scan_with_different_read_ts_ingest_next`
- **What it tests:** Regression for stale alternate-cursor values when the ingest cursor is the alternate and `read_timestamp` increases between `next()` calls. Setup: leader writes key=1 at ts=1; follower ingest adds key=2 at ts=2 then updates it to ts=3 (key=2 sorts after key=1). T1 (`read_timestamp=2`) calls `next()` → key=1/value=1 (stable, ingest key=2 is alternate); T2 (`read_timestamp=3`) calls `next()` → key=2/value=3 (bug: the ingest cursor was cached at key=2/value=2 from T1's context and would return value=2).
- **Components:** `src/cursor/cur_layered.c`, alternate cursor re-search when ingest is the non-current cursor
- **Notes:** Demonstrates the same bug but with the ingest btree as the alternate and a rising read_timestamp.

### `test_layered96.test_talbe_scan_with_different_read_ts_ingest_prev`
- **What it tests:** Same as above using `prev()`. Setup: leader writes key=2 at ts=1; follower ingest adds key=1 at ts=2 then updates to ts=3. T1 (`read_timestamp=2`) calls `prev()` → key=2/value=1; T2 (`read_timestamp=3`) calls `prev()` → key=1/value=3 (bug: returns value=2).
- **Components:** `src/cursor/cur_layered.c`, ingest alternate cursor re-search on `prev()`

### `test_layered96.test_snapshot_gen_ingest_next_txn_txn`
- **What it tests:** Calls `snapshot_gen_ingest_next(first_explicit_txn=True, second_explicit_txn=True)`. Both cursor calls are wrapped in explicit transactions. Between T1 (`next()` → key=1/value=1) and T2, the ingest write for key=2 is updated from value=2 to value=22. Because an isolation boundary was crossed (explicit txn ended), T2's snapshot is new and must re-search the alternate ingest cursor. `next()` → key=2/value=22.
- **Components:** `src/cursor/cur_layered.c`, snapshot generation boundary detection

### `test_layered96.test_snapshot_gen_ingest_next_txn_auto`
- **What it tests:** Calls `snapshot_gen_ingest_next(first_explicit_txn=True, second_explicit_txn=False)`. T1 uses an explicit transaction; T2 runs under autocommit. Between the two calls key=2 is updated to value=22. Since T1 ended (explicit → autocommit boundary), T2 uses a new snapshot and sees value=22.
- **Components:** Snapshot isolation boundary between explicit transaction and autocommit on layered cursor

### `test_layered96.test_snapshot_gen_ingest_next_auto_txn`
- **What it tests:** Calls `snapshot_gen_ingest_next(first_explicit_txn=False, second_explicit_txn=True)`. T1 is autocommit; T2 is explicit. Between them, key=2 is updated to value=22. An isolation boundary is crossed (new explicit transaction), so T2 re-searches and returns value=22.
- **Components:** Snapshot isolation boundary between autocommit and explicit transaction on layered cursor

### `test_layered96.test_snapshot_gen_ingest_next_auto_auto`
- **What it tests:** Calls `snapshot_gen_ingest_next(first_explicit_txn=False, second_explicit_txn=False)`. Both calls run under autocommit with no explicit transaction. Between them key=2 is updated to value=22. Since both calls share no explicit isolation boundary (autocommit-to-autocommit within the same open cursor is treated as a consistent scan), T2 returns value=2 (the value visible at the time of the first autocommit). This verifies that the fix only re-searches the alternate when an isolation boundary is detected.
- **Components:** Autocommit-to-autocommit consistency on layered cursor (no stale-value re-search)

### `test_layered96.test_snapshot_gen_ingest_prev_txn_txn`
- **What it tests:** Calls `snapshot_gen_ingest_prev(first_explicit_txn=True, second_explicit_txn=True)`. Mirror of `_next_txn_txn` using `prev()`. Leader writes key=2; follower ingest has key=1 (value=1 → value=11). T1 `prev()` → key=2/value=2; key=1 is updated between calls; T2 `prev()` → key=1/value=11.
- **Components:** Alternate cursor re-search on `prev()` with explicit/explicit isolation boundary

### `test_layered96.test_snapshot_gen_ingest_prev_txn_auto`
- **What it tests:** Calls `snapshot_gen_ingest_prev(first_explicit_txn=True, second_explicit_txn=False)`. T1 explicit, T2 autocommit. Between calls key=1 updated to value=11. T2 sees value=11.
- **Components:** Snapshot boundary (explicit → autocommit) on `prev()` alternate cursor

### `test_layered96.test_snapshot_gen_ingest_prev_auto_txn`
- **What it tests:** Calls `snapshot_gen_ingest_prev(first_explicit_txn=False, second_explicit_txn=True)`. T1 autocommit, T2 explicit. Between calls key=1 updated to value=11. T2 sees value=11.
- **Components:** Snapshot boundary (autocommit → explicit) on `prev()` alternate cursor

### `test_layered96.test_snapshot_gen_ingest_prev_auto_auto`
- **What it tests:** Calls `snapshot_gen_ingest_prev(first_explicit_txn=False, second_explicit_txn=False)`. Both autocommit. Between calls key=1 updated to value=11. T2 sees value=1 (consistent scan; no isolation boundary crossed, no re-search).
- **Components:** Autocommit-to-autocommit consistent scan on `prev()` — verifies no spurious re-search
