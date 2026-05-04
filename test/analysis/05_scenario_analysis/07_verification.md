# Implementation Verification of Gap Analysis Claims

## Summary Table

| # | Claim | Verdict | Notes |
|---|-------|---------|-------|
| 1 | session.alter() zero coverage on layered tables | CONFIRMED | No test_layered or test_disagg file calls .alter(); hook_disagg.py wraps it but only rewrites the URI |
| 2 | session.rename() zero coverage | CONFIRMED (with caveat) | No test_layered*.py calls rename; schema_rename.c does not exist; WT_SESSION has no rename method in this codebase |
| 3 | No test combines read_timestamp with search/iteration on layered tables | REFUTED | Multiple files found with both; test_layered73.py explicitly calls cursor.search(), cursor.next(), cursor.prev() inside read_timestamp transactions |
| 4 | No single committed transaction writes to two layered tables | REFUTED | test_layered94.py has a multi_table=True scenario that opens cursors on both uri and uri_b and commits them in a single transaction |
| 5 | hook_disagg.py skips ALL rollback_to_stable tests | PARTIAL | hook_disagg.py skips any test whose name contains "rollback_to_stable"; but test_layered87.py calls conn.rollback_to_stable() directly and is not skipped |
| 6 | ops.pct.modify=0 in CONFIG.disagg | CONFIRMED | Line present: `ops.pct.modify=0` with comment FIXME-WT-16479 |
| 7 | Drain code has pinned_dhandle refcount issue | PARTIAL | pinned_dhandle is correctly decremented on the success path (line 571); but __layered_drain_clear_work_queue (lines 598–609) frees work items without calling decr_use, leaving refcount un-decremented on the cleanup/error path |
| 8 | Named checkpoints completely skipped by hook_disagg.py | CONFIRMED | hook_disagg.py lines 249–250: if 'name=' in config: skip_test('named checkpoints do not work in disagg storage') |
| 9 | Only 3 of 16 disagg stats are ever asserted | PARTIAL | 17 WT_STAT_CONN_DISAGG_* stats are defined; 4 unique stat names are asserted in tests (disagg_block_page_discard, disagg_block_put_cold, disagg_block_get_cold, layered_table_manager_checkpoints_disagg_pick_up_follower) — so 4 of 17, not 3 of 16 |
| 10 | reserve() tests all rollback, none commit | CONFIRMED | do_reserve() in test_layered92.py always calls session.rollback_transaction() in a finally block; no commit path exists |

---

## Detailed Findings

### Claim 1: session.alter() coverage
**Verdict:** CONFIRMED

**Evidence:**
- `grep -r "session.alter\|\.alter(" test/suite/test_layered*.py` — no output (zero matches).
- `grep -r "session.alter\|\.alter(" test/suite/test_disagg*.py` — no output (zero matches).
- `grep -n "alter" test/suite/hook_disagg.py` returned lines 78–79 (comment), 236–239 (session_alter_replace function), and 405–407 (hook registration).
- The `session_alter_replace` function at line 237 merely translates the URI via `replace_uri()` and forwards to the original, but no test in test_layered*.py or test_disagg*.py actually invokes session.alter().

**Impact:** Gap is real; no test exercises alter on a layered table. Priority unchanged.

---

### Claim 2: session.rename() coverage
**Verdict:** CONFIRMED (with caveat)

**Evidence:**
- `grep -r "session.rename\|\.rename(" test/suite/test_layered*.py` — no output.
- `src/schema/schema_rename.c` does not exist in this codebase (the schema directory contains: schema_alter.c, schema_create.c, schema_drop.c, schema_list.c, schema_open.c, schema_plan.c, schema_project.c, schema_stat.c, schema_truncate.c, schema_util.c, schema_worker.c).
- Searching `wiredtiger.h.in` for `int __F(rename` on the `WT_SESSION` struct returned no result — `session.rename()` is not a public API method in this version of WiredTiger.
- The `rename` symbol in `wiredtiger.h.in` only appears as a `WT_DATA_SOURCE` callback and a `WT_FILE_SYSTEM` method.

**Caveat:** The claim references `session.rename()` as if it is a WT_SESSION API, but this API does not exist in the codebase. The gap therefore describes a missing capability, not just missing test coverage.

**Impact:** Gap holds — rename of layered tables is both untested and apparently unimplemented at the session API level.

---

### Claim 3: No test combines read_timestamp with search/iteration on layered tables
**Verdict:** REFUTED

**Evidence:**
- `grep -rl "read_timestamp" test/suite/test_layered*.py` returned 19 files.
- Checking representative files:
  - **test_layered73.py** (lines 94–145): opens a cursor, calls `session.begin_transaction('read_timestamp=...')`, then explicitly calls `cursor.search_near()`, `cursor.search()`, `cursor.next()`, and `cursor.prev()` within that transaction.
  - **test_layered09.py** (lines 83–215): opens cursors within read_timestamp transactions and reads values.
  - **test_layered25.py** (lines 93–157): multiple read_timestamp transactions with cursor reads using `cursor[str(i)]`.

**Impact:** This gap claim is incorrect; read_timestamp combined with cursor iteration is covered by at least test_layered73.py.

---

### Claim 4: No single committed transaction writes to two layered tables
**Verdict:** REFUTED

**Evidence:**
- `grep -n "multi_table\|uri_b" test/suite/test_layered94.py` shows:
  - Line 42: `multi_table: True — the prepared transaction also covers a second layered table` (comment in file header).
  - Line 64: `('multi_table', dict(multi_table=True))` — an explicit scenario.
  - Line 83: `return [self.uri] + ([self.uri_b] if self.multi_table else [])` — uris list contains both layered tables when multi_table=True.
  - Lines 142–150: `leader_cursors = [self.session.open_cursor(uri) for uri in uris]`, followed by `self.session.begin_transaction()`, writes to all cursors (both tables), then `self.session.commit_transaction(...)`.
- When `multi_table=True`, a single committed transaction writes to both `layered:test_layered94` and `layered:test_layered94_b`.

**Impact:** This gap claim is incorrect; test_layered94.py's multi_table scenario covers cross-table commits.

---

### Claim 5: hook_disagg.py skips ALL rollback_to_stable tests
**Verdict:** PARTIAL

**Evidence:**
- `grep -n "rollback_to_stable\|rollback" test/suite/hook_disagg.py` returned only line 377:
  ```
  ("rollback_to_stable",   "Rollback to stable is not needed at startup"),
  ```
  This is in `skip_categories` — any test whose string representation contains `"rollback_to_stable"` is skipped.
- However, `grep -rn "rollback_to_stable" test/suite/test_layered*.py` found **test_layered87.py line 82**: `self.conn.rollback_to_stable()` — this file is named `test_layered87`, which does NOT contain the string `"rollback_to_stable"` and therefore is NOT skipped by the hook.

**Impact:** The claim overstates the skip. Class-named rollback_to_stable test suites are skipped, but individual layered tests that call conn.rollback_to_stable() internally (like test_layered87.py) still run.

---

### Claim 6: ops.pct.modify=0 in CONFIG.disagg
**Verdict:** CONFIRMED

**Evidence:**
- Contents of `test/format/CONFIG.disagg` include:
  ```
  # FIXME-WT-16479 Extend testing for cursor->modify.
  ops.pct.modify=0
  ```
- The FIXME comment confirms this is a deliberate exclusion pending future work.

**Impact:** Gap is real; cursor modify operations are entirely excluded from format test runs on disaggregated storage.

---

### Claim 7: Drain code has pinned_dhandle refcount issue
**Verdict:** PARTIAL

**Evidence:**
- `grep -n "pinned_dhandle\|refcount\|ref_cnt\|__wt_dhandle_incr_use\|__wt_dhandle_decr_use" src/conn/conn_layered_ingest.c` returned:
  - Line 568: `WT_ASSERT(session, work_item->entry->pinned_dhandle != NULL);`
  - Line 569–571: `WT_WITH_DHANDLE(session, work_item->entry->pinned_dhandle, { work_item->entry->pinned_dhandle = NULL; __wt_cursor_dhandle_decr_use(session); });` — correct cleanup on success path.
  - Line 669: `WT_ERR(__wt_cursor_uri_incr_use(session, entry->layered_uri, &entry->pinned_dhandle));` — increments refcount when queueing work.
- The `__layered_drain_clear_work_queue` function (lines 594–610) frees work items with `__wt_free(session, work_item)` but does NOT call `__wt_cursor_dhandle_decr_use`. This means if the work queue is cleared (e.g., on shutdown or error) before items are processed, `pinned_dhandle` refcounts are leaked.

**Impact:** The concern is real on the cleanup/error path, but the success path is correctly handled. This is a genuine edge-case gap.

---

### Claim 8: Named checkpoints completely skipped by hook_disagg.py
**Verdict:** CONFIRMED

**Evidence:**
- `grep -n "name=\|named.*checkpoint\|checkpoint.*name" test/suite/hook_disagg.py` returned lines 249–250:
  ```python
  if 'name=' in config:
      skip_test('named checkpoints do not work in disagg storage')
  ```
- This is inside `session_checkpoint_replace`, which intercepts every `session.checkpoint()` call. Any checkpoint call with `name=` in its config string causes the test to be skipped entirely via `skip_test`.

**Impact:** Gap is real; named checkpoints cannot be tested with the disagg hook active.

---

### Claim 9: Only 3 of 16 disagg stats are ever asserted
**Verdict:** PARTIAL

**Evidence:**
- `grep -c "DISAGG" src/include/wiredtiger.h.in` returned 58 (total DISAGG mentions).
- `grep "WT_STAT_CONN_DISAGG" src/include/wiredtiger.h.in | grep -v PERF_HIST` shows **17** `WT_STAT_CONN_DISAGG_*` stats defined:
  - DISAGG_BLOCK_HS_BYTE_READ, DISAGG_BLOCK_HS_BYTE_WRITE, DISAGG_BLOCK_GET, DISAGG_BLOCK_GET_COLD, DISAGG_BLOCK_HS_GET, DISAGG_BLOCK_PAGE_DISCARD, DISAGG_BLOCK_PUT, DISAGG_BLOCK_PUT_COLD, DISAGG_BLOCK_HS_PUT, DISAGG_BLOCK_READ_AHEAD_FRONTIER, DISAGG_ABANDON_CHECKPOINT_FAILED, DISAGG_ABANDON_CHECKPOINT_SUCCEED, DISAGG_CONN_RECONFIG, DISAGG_DATABASE_SIZE, DISAGG_ROLE_LEADER, DISAGG_STEP_DOWN_TIME, DISAGG_STEP_UP_TIME.
- `grep -rn "stat.conn.disagg" test/suite/test_layered*.py test/suite/test_disagg*.py | grep -o "stat\.conn\.[a-z_]*" | sort -u` yields **4 unique stat names**:
  - `stat.conn.disagg_block_get_cold` (test_disagg04.py)
  - `stat.conn.disagg_block_page_discard` (test_layered44.py)
  - `stat.conn.disagg_block_put_cold` (test_disagg04.py)
  - `stat.conn.layered_table_manager_checkpoints_disagg_pick_up_follower` (test_layered23.py)
- Correction to claim: it is **4 of 17** stats asserted, not "3 of 16". The spirit of the claim is correct (coverage is thin), but the numbers are off.

**Impact:** The gap is real — 13 of 17 disagg-prefixed connection stats are never asserted in any test.

---

### Claim 10: reserve() tests all rollback, none commit
**Verdict:** CONFIRMED

**Evidence:**
- `grep -n "rollback_transaction\|commit_transaction\|reserve" test/suite/test_layered92.py` shows:
  - Line 63–71: `do_reserve()` method unconditionally calls `session.rollback_transaction()` in a `finally` block after `c.reserve()`.
  - Line 51: `session.commit_transaction(...)` only appears in the `write()` helper (setup), not in any reserve test path.
  - No `commit_transaction` call appears after a `reserve()` call anywhere in the file.
- Every test method (`test_leader_key_exists`, `test_leader_key_absent`, `test_follower_*`) calls `do_reserve()`, which always rolls back.

**Impact:** Gap is real; the behavior of committing a transaction that contains a reserved key on a layered table is not tested.

---

## Summary: Claims Confirmed vs Refuted

| Status | Claims |
|--------|--------|
| CONFIRMED (gap is real) | 1 (alter zero coverage), 2 (rename zero coverage / no API), 6 (ops.pct.modify=0), 8 (named checkpoints skipped), 10 (reserve always rolls back) |
| REFUTED (gap does not exist) | 3 (read_timestamp + iteration IS tested), 4 (two-table committed transaction IS tested) |
| PARTIAL (gap exists but details differ) | 5 (RTS skipped by name-match only, not all paths), 7 (refcount leak on error path only), 9 (4 of 17 stats, not 3 of 16) |

**Confirmed:** 5 claims fully confirmed  
**Refuted:** 2 claims refuted  
**Partial:** 3 claims partially confirmed with corrections to scope or numbers
