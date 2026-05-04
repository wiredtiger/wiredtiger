# Disaggregated Storage: Missing Test Scenarios

> Analysis date: May 2026  
> Framing: test-scenario gaps, not implementation paths  
> Method: audit of all test_layered*.py + test_disagg*.py + CONFIG.disagg + hook_disagg.py  

This document identifies classes of scenarios that are not covered, or only partially covered, in the disagg/layered test suite. The goal is concrete additions to existing tests (or new focused tests), organized by API or scenario class.

---

## 1. `cursor.reserve()`

### Current coverage (test_layered92, test_layered93)

test_layered92 covers reserve() on a **leader** connection (key exists before/after checkpoint; key missing) and on a **follower** connection (key in stable-only; key in ingest-only; key in both; key missing). That is a good matrix for the basic key-location dimension.

test_layered93 adds reserve() for a follower reading a key that the leader wrote to stable.

### Missing scenarios

**A. reserve() on a key with a tombstone in ingest**  
Current tests only set up keys that exist (or are absent entirely). What if a key exists in stable (from a leader checkpoint) but the follower then deletes it — creating a tombstone in the ingest btree — and then `reserve()` is called? The `overwrite=false` default on reserve means it should return `WT_NOTFOUND` for a deleted key. This scenario is not tested.

```
leader: insert key=1 @ts=1, checkpoint
follower: delete key=1 @ts=2   ← tombstone in ingest
follower: reserve(key=1)        ← should return WT_NOTFOUND; is it tested? No.
```

**B. reserve() end-to-end: reserve then write**  
test_layered92's `do_reserve` always rolls back the transaction. No test verifies that after a successful `reserve()`, an `update()` in the same transaction actually lands in the correct btree and is readable after commit. The write-lock semantics of reserve are confirmed by "reserve succeeds" but not by "the subsequent write went where it should."

**C. reserve() conflict between two concurrent sessions**  
No test has two sessions try to reserve the same key simultaneously. reserve() is a write-lock; if session A reserves key=1 and session B tries to reserve key=1, session B should block or return WT_ROLLBACK. This is the core purpose of reserve() and it's never tested on layered tables.

**D. reserve() with a non-default read timestamp**  
```python
session.begin_transaction('read_timestamp=' + ts_str(old_ts))
cursor.set_key(key)
cursor.reserve()   # key exists at ts=5, read_timestamp=3 — key not visible
```
Does reserve() with a historical read timestamp fail (WT_NOTFOUND) even when the key exists at a newer timestamp? Not tested.

**E. reserve() behavior across role transition**  
A session has an open transaction with a reserve() in flight when the connection changes role. Not tested.

**Where to add:** Extend test_layered92.py with new test methods for A, B, C, D. Keep the same structure.

---

## 2. `cursor.modify()`

### Current coverage

test_layered16.py (tiny, modify-only), test_layered_modify01.py, test_layered22.py (modify + search_near), test_layered93.py (follower modify for key in stable-only). Disabled in CONFIG.disagg via `ops.pct.modify=0` (FIXME-WT-16479).

### Missing scenarios

**A. modify() on a key whose base value is in ingest, not stable**  
All current tests set up the key on the leader (in stable) and then modify on the follower. What if a follower inserts a key and then modifies it — both operations are in the ingest btree, never flushed to stable? Is the modify delta correctly applied on top of the ingest version?

**B. modify() on a key with a version split across stable and ingest**  
Key written by leader → in stable. Follower updates it (version 2 in ingest). Follower then calls modify() (should modify version 2 from ingest, not version 1 from stable). Does `cur_layered.c` correctly identify the most recent version as the base for modify?

**C. modify() on a key that has a tombstone in ingest**  
Key in stable, tombstone in ingest → logical key is deleted. modify() should return WT_NOTFOUND. Not tested.

**D. modify() re-enable in CONFIG.disagg**  
FIXME-WT-16479 says "Extend testing for cursor->modify." The format test would exercise many random workloads with modify once the flag is removed. Should be unblocked and confirmed working. The modify-specific layered tests (A-C above) are the prerequisite.

**Where to add:** test_layered_modify02.py covering scenarios A-C; unblock CONFIG.disagg once A-C pass.

---

## 3. `cursor.search_near()`

### Current coverage

Well covered in isolation: test_layered05 (764 lines), test_layered82, test_layered83 (1000 keys), test_layered89 (with prepare). These test the basic iteration and boundary recovery.

### Missing scenarios

**A. search_near() on a table with keys only in stable (no ingest data)**  
Most search_near tests have a mix of stable + ingest content. What about a pure-stable table where the follower has never written anything? Does search_near correctly operate on stable-only content?

**B. search_near() where the nearest key has a tombstone in ingest**  
```
stable: keys 1, 5, 10
ingest: tombstone on key 5
search_near(key=5) → should return key 1 or key 10 (the logical nearest non-deleted key)
```
Does the layered search_near correctly skip ingest tombstones when scanning for the nearest match? The boundary case where the exact match is deleted in ingest is not covered.

**C. search_near() straddling the stable/ingest boundary**  
The last key in stable is K1, the first key in ingest is K2, and we search for a key between them. This exercises the exact handoff between the two cursor constituents. Already partially covered by test_layered83, but not with explicit tombstone-at-boundary scenarios.

**D. search_near() on an empty layered table**  
Should return WT_NOTFOUND immediately. Trivial but confirms no crash on empty constituent.

**Where to add:** Extend test_layered05 or test_layered83 with scenarios B and C.

---

## 4. `cursor.bound()`

### Current coverage

test_layered05, test_layered81, test_layered82, test_layered85 use bounds directly via the `disagg_test_class` decorator. However, the disagg hook (hook_disagg.py:382) skips ALL `test_cursor_bound` generic tests with reason "Can't use cursor bounds with a disagg table." This creates an inconsistency: bounds work when explicitly programmed in layered tests, but the generic bound test suite never runs under the disagg hook.

### Missing scenarios

**A. cursor.bound() + role transition**  
A cursor with bounds set is open when `conn.reconfigure(role="leader")` is called. Does `__clayered_adjust_state` correctly reset or re-apply bounds when it switches the stable constituent? Not tested.

**B. cursor.bound() on a table where one constituent is empty**  
Bounds search on a table with all data in stable and nothing in ingest (or vice versa). Confirms the bound logic correctly handles a single-constituent case.

**C. cursor.bound() with tombstones at the boundary**  
First key in the bounded range is a tombstone in ingest. Does `next()` correctly advance past it?

**D. Re-evaluate the hook skip**  
The hook skips all `test_cursor_bound` tests, but bounds clearly work for layered tables. The skip comment should be revisited — either bounds are genuinely broken in some test scenario (which should be documented as a bug), or the skip is overly broad and should be narrowed to specific broken cases.

**Where to add:** New test methods in test_layered05.py or test_layered82.py for A-C. Separately, reassess the hook_disagg skip.

---

## 5. Key-location matrix: is it fully covered per operation?

The layered architecture has three key locations (stable-only, ingest-only, both) and a "missing" state, plus the role (leader vs follower). test_layered93 tests {reserve, search, search_near, update, remove, modify} × {follower + key in stable-only}. test_layered92 tests reserve × {leader, follower} × {all 4 states}.

For the full matrix, none of the other operations have the 4-state coverage that test_layered92 gives for reserve. Specifically:

| Operation | stable-only | ingest-only | both | missing | leader vs follower |
|-----------|-------------|-------------|------|---------|--------------------|
| search    | ✓ (many)    | ✓ (some)    | ✓    | ✓       | partially          |
| search_near | ✓         | ✓           | ✓    | ✓       | partially          |
| insert    | ✓ (upsert)  | ✓           | ✓    | ✓       | partially          |
| update    | ✓ (test_layered93) | ? | ?    | ?       | follower-only      |
| remove    | ✓           | ?           | ?    | ✓       | partially          |
| modify    | ✓ (test_layered93) | **no** | **no** | ? | follower-only |
| reserve   | ✓ (test_layered92) | ✓   | ✓    | ✓       | ✓ both roles       |
| next/prev | ✓           | ✓           | ✓    | ✓       | partially          |
| truncate  | ✓           | ?           | ?    | ✓       | leader-only        |

The `?` cells are the missing combinations. The most valuable addition is a parametrized test (similar to test_layered93's structure) that runs {update, remove, modify} × {all 4 key-location states} × {leader, follower}.

---

## 6. Transactions: scenarios not covered

### A. `overwrite=false` cursor on layered table

Most cursor opens use default (`overwrite=true`) or explicitly set `overwrite=true`. An `overwrite=false` cursor means insert() fails if the key already exists. This cursor mode is used in conflict-detection scenarios.

```python
c = session.open_cursor(uri, config='overwrite=false')
c[existing_key] = 'v'  # should return WT_DUPLICATE_KEY
```

Is `overwrite=false` even supported on layered cursors? Does it check both stable and ingest for key existence? Not tested.

### B. Read-your-own-writes within the same transaction

Write key=1 in a transaction; then read key=1 in the same transaction before commit. Does the layered cursor correctly find the uncommitted value from the transaction's own update chain?

### C. Transaction isolation: snapshot read vs older stable data

A follower has a long-running read transaction (snapshot at ts=5). Meanwhile the leader checkpoints ts=10. The follower advances its checkpoint. The reader should still see data as of ts=5. Does the layered cursor correctly apply snapshot visibility to the stable constituent?

### D. Cross-table transactions on layered tables

A single transaction writes to table A and table B (both layered). Does the transaction correctly span two layered tables? Does commit atomically update both? Not explicitly tested — most tests use a single table.

### E. Nested transactions / subtransactions

WiredTiger doesn't support nested transactions, but some drivers use savepoints. Does the layered cursor handling of `WT_SESSION_IGNORE_CACHE_SIZE` or other session flags interact correctly?

---

## 7. Timestamp scenarios

### A. Operations at oldest_timestamp boundary

When `oldest_timestamp` is advanced past data in ingest, does GC correctly evict the old ingest versions? A test that:
1. Inserts keys at ts=1..10 in ingest
2. Advances oldest_timestamp to ts=5
3. Verifies keys at ts=1..4 are no longer readable (correctly expired)
4. Verifies keys at ts=5..10 still readable

### B. Durable timestamp vs commit timestamp

Prepared transactions have a prepare timestamp and a durable timestamp. For drain filtering (line 404 of conn_layered_ingest.c), the code uses `durable_start_ts > last_checkpoint_timestamp`. Is the distinction between commit_timestamp and durable_timestamp tested for drain filtering? test_layered45 covers some of this, but not the boundary where they differ.

### C. Operations with `all_durable` timestamp

Does `conn.query_timestamp('get=all_durable')` return correct values for disagg connections? Are there tests that query this and verify the result against actual data?

### D. `stable_timestamp` not yet set

Some tests create tables without setting any timestamps at all. Others set stable_timestamp before every checkpoint. Is there a test that verifies behavior when stable_timestamp is never set — i.e., all data is always checkpointed without timestamp filtering?

---

## 8. Named checkpoints (completely untested)

hook_disagg.py skips all named checkpoints (`name=` in config). The layered tests never test named checkpoints either. Specifically untested:

- `session.checkpoint('name=myckpt')` on a connection with layered tables
- Opening a checkpoint cursor on a layered table: `session.open_cursor(uri, config='checkpoint=myckpt')` (hook skips these too)
- Crash recovery restoring to a named checkpoint

If named checkpoints genuinely don't work, this should be a documented limitation with a FIXME. If they do work, the skip should be removed and tests added.

---

## 9. Statistics cursors

No test opens a `statistics:` cursor on a layered table URI and verifies disagg-specific statistics. For example:

- `statistics:layered:foo` — per-table statistics
- `statistics:` — connection-level statistics with `disagg_*` fields (disagg_role_leader, disagg_num_ingest_tables, etc.)

The stats exist (conn_stat.h has disagg_* fields), but there's no test that:
1. Performs operations
2. Opens a statistics cursor
3. Asserts that the statistics reflect what happened (e.g., `disagg_num_drains` incremented after drain)

This is a broad class of missing verification.

---

## 10. Cursor duplication (`open_cursor` with `dupcursor`)

```python
c1 = session.open_cursor(uri)
c1.set_key(5)
c1.search()
c2 = session.open_cursor(uri, c1)  # duplicate cursor at same position
```

Cursor duplication is used internally in WiredTiger and by some callers. Is it supported on layered cursors? Is it tested? Not found in any `test_layered*.py`.

---

## 11. `session.truncate()` with cursor-delimited ranges

The existing fast-truncate tests (`test_layered_fast_truncate01-03`) test table-level truncation. But `session.truncate()` can also take start/stop cursors to truncate a range. This cursor-range form is not tested on layered tables.

```python
start = session.open_cursor(uri)
stop = session.open_cursor(uri)
start.set_key(10); start.search()
stop.set_key(50); stop.search()
session.truncate(None, start, stop, None)  # truncate range [10, 50]
```

---

## 12. Multi-session concurrent read/write

Nearly all tests use a single session. The few that use multiple sessions (leader + follower) treat them as strictly sequential. Missing:

**A. Concurrent reads and writes on the same follower**  
Session A writes to ingest; session B reads the same key concurrently (in its own snapshot transaction). Do the two see correctly isolated data?

**B. Concurrent writes on the same leader — serialization**  
Two leader sessions try to insert the same key at the same timestamp. Should trigger a write conflict. Is write conflict detection on layered tables tested?

---

## 13. Reverse collator

CONFIG.disagg has `btree.reverse=0` (FIXME-WT-14738). When this is enabled:
- `next()/prev()` order is reversed
- `search_near()` direction semantics change

Once FIXME-WT-14738 is resolved, the entire search_near and bound test matrix should be re-run with reverse=1. Currently there is zero test coverage for reverse collation on layered tables.

---

## 14. Large values and value types

Most tests use short strings (`key_format=i,value_format=S` with values like `'v'`). Missing:

**A. Values larger than a page (overflow pages)**  
Overflow values in the stable btree interact with the page log in ways that compact values do not. An overflow value crossing a checkpoint boundary may reference a different page log entry than the containing page. Not tested.

**B. Multi-field values (`value_format='iSi'`)**  
Multi-field value formats affect how modify() computes deltas. Only single-field values are tested.

**C. Fixed-length byte array (`value_format='20s'`)**  
Fixed-format values have different alignment and sizing from variable-length strings. Not tested on layered tables.

---

## 15. `verify` — what it checks vs when it's called

`session.verify()` is called in test_layered32, test_layered52, test_layered54, test_layered63, test_layered66, test_layered74. In all cases it is called after a successful operation as a sanity check.

Missing:

**A. verify() after a role transition**  
Verify the stable btree after step_down (tables are READONLY). Does verify report them correctly? After step_up?

**B. verify() on a table that has only ingest data (no stable checkpoint yet)**  
What does verify return for a table that was just created as leader but never checkpointed?

**C. verify() with `dump_address` or other config options**  
`session.verify(uri, 'dump_address')` is a more thorough verify. Never used in layered tests.

---

## 16. Column store / RECNO keys

hook_disagg.py session_create_replace skips tables with `key_format=r` (RECNO). This means all column-store tests are excluded from disagg coverage. Whether column store is intentionally unsupported for layered tables is not documented. If it's unsupported, a clear FIXME should exist; if it's supported, the skip is wrong.

---

## 17. `import` and `drop` completeness

Import is disabled (hook + CONFIG). For `drop`:

- `session.drop('layered:foo')` — is it tested while data is still in both stable and ingest?
- Drop while drain is in flight (concurrent drain + drop)?
- Drop a table that has only ingest data (never flushed to stable)?

These are not obvious from the existing test set.

---

## Summary: highest-value additions

| Priority | Scenario class | Where to add |
|----------|----------------|--------------|
| HIGH | `modify()` on key-in-ingest-only and split-version (§2A-B) | test_layered_modify02.py |
| HIGH | Full key-location matrix for `update`, `remove` (§5) | parametrized test like test_layered93 |
| HIGH | `reserve()` conflict between two concurrent sessions (§1C) | extend test_layered92 |
| HIGH | `reserve()` on key with tombstone in ingest (§1A) | extend test_layered92 |
| HIGH | `reserve()` end-to-end write after reserve (§1B) | extend test_layered92 |
| MEDIUM | Statistics cursor verification of disagg_* stats (§9) | new test_disagg_stats.py |
| MEDIUM | search_near() on exact-match tombstone (§3B) | extend test_layered05 |
| MEDIUM | `overwrite=false` cursor behavior on layered tables (§6A) | new test case in test_layered_cursor01 |
| MEDIUM | Cross-table transaction atomicity (§6D) | extend any multi-table test |
| MEDIUM | Named checkpoint support/limitation documentation (§8) | clarify + test or document |
| MEDIUM | cursor.bound() + role transition (§4A) | extend test_layered82 |
| LOW | Cursor range truncate (§11) | extend test_layered_fast_truncate |
| LOW | verify() after role transition (§15A) | extend test_layered66 |
| LOW | Cursor duplication support check (§10) | quick new test |
| LOW | Large / overflow values (§14A) | extend test_layered27 with large values |
