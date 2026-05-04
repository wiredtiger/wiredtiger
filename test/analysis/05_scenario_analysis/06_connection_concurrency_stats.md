# Connection Operations, Concurrency, and Statistics: Scenario Gap Analysis

## Coverage Summary

| Area | Coverage Level | Dedicated Tests |
|------|---------------|-----------------|
| Multi-table transactions (layered+layered) | PARTIAL | test_layered94 (prepared txn only), test_layered31 (no atomic txn test) |
| Statistics (WT_STAT_CONN_DISAGG_*) | MINIMAL | test_layered44 (disagg_block_page_discard only), test_disagg04 (cold put/get only) |
| Read-only connections | MINIMAL | test_layered88 (rejection test only) |
| Cursor duplication (cursor.dup) | NONE | No layered test uses cursor.dup |
| Multi-session concurrency | PARTIAL | test_layered45 (uncommitted blocker), test_layered_fast_truncate01 (truncate conflicts) |
| conn.reconfigure() with disagg options | MODERATE | test_layered39, test_layered34, test_layered62, test_layered68 |
| Connection close during active operations | MINIMAL | test_layered94 (skip_checkpoint only) |
| Multiple connections to same database | PARTIAL | test_layered07 (serial switch), test_layered44 (leader+follower sequential) |
| Error handling and recovery | MINIMAL | test_layered64 (corrupt checksum), test_layered31 (bad checkpoint meta) |

---

## Gap Analysis

### Multi-Table Transactions

**Covered:**

- `test_layered94` tests a prepared transaction that spans two layered tables (via the `multi_table=True` scenario dimension). A single `begin_transaction` / `prepare_transaction` writes to `cursors[uri_a]` and `cursors[uri_b]` atomically, then the follower steps up and resolves the prepared transaction.
- `test_layered31` opens two layered URIs (`layered:test_layered31a`, `layered:test_layered31b`) and writes to both per-iteration, but each individual key write uses autocommit (no explicit `begin_transaction` spanning both tables simultaneously).
- `test_layered47` and `test_layered_cursor01` also use two layered tables sequentially without cross-table atomicity.

**Gap 1 [CRITICAL]: No test for an atomic committed (non-prepared) transaction spanning two layered tables**

- Scenario: `session.begin_transaction()` → write key K1 to `layered:table_a` → write key K2 to `layered:table_b` → `session.commit_transaction()`. Verify that either both writes appear in a subsequent checkpoint or neither does. Then restart as a follower and confirm consistent cross-table state.
- Risk: The two physical btrees behind each layered URI (`*.wt_ingest`) are separate objects. If the layered reconciliation path handles cross-btree atomicity incorrectly, a crash between the two ingest writes could produce a checkpoint where one table shows K1 but the other does not. This is a durability atomicity gap that `test_layered94` does not exercise because it only tests prepared transactions, not regular committed ones.
- Suggested test: Write a `test_layered_multi_txn01.py` that: (1) opens two layered tables, (2) writes key K to both inside a single committed transaction, (3) checkpoints, (4) reopens as follower, (5) reads both tables and asserts K is visible in both or in neither.

**Gap 2 [HIGH]: No test for rollback atomicity across two layered tables**

- Scenario: Two layered tables open; `begin_transaction()` writes to both; `rollback_transaction()`. Verify neither table shows the keys.
- Risk: If the rollback tears down ingest entries in a partially ordered way and an eviction or checkpoint races in between, one table's ingest might still show partial data from the aborted transaction.
- Suggested test: Extend the multi-table test above with a rollback variant. Interleave a checkpoint concurrently with the rollback to stress the race window.

**Gap 3 [MEDIUM]: No test for write conflict detection across two layered tables in the same transaction**

- Scenario: Session1 writes to `layered:table_a` and `layered:table_b` inside one transaction. Session2 concurrently writes to the same key on `layered:table_a`. Verify that exactly one session gets `WT_ROLLBACK`.
- Risk: Write conflict detection is per-btree. If cross-table transaction logic uses different MVCC timestamps for the two ingest btrees, conflict detection might be asymmetric.
- Suggested test: Two sessions, each wrapping a cross-table write; assert `WT_ROLLBACK` on the loser.

---

### Statistics (WT_STAT_CONN_DISAGG_*)

**Covered:**

The following `WT_STAT_CONN_DISAGG_*` counters are asserted in tests:

| Counter | Test |
|---------|------|
| `disagg_block_page_discard` | `test_layered44` |
| `disagg_block_put_cold` | `test_disagg04` |
| `disagg_block_get_cold` | `test_disagg04` |

**Gap 4 [HIGH]: `disagg_block_get` and `disagg_block_put` are never asserted**

- Scenario: Write data to a layered leader, checkpoint, open a follower, read all keys. Assert `disagg_block_put > 0` on the leader and `disagg_block_get > 0` on the follower.
- Risk: These are the core hot-path I/O counters for disaggregated storage. If they are silently zero-incremented (e.g., a stat update is missing in a code path), the problem goes undetected. Because no test asserts these values, a regression removing these stat increments would not be caught.
- Suggested test: Add to an existing leader/follower test (e.g., `test_layered02` or `test_layered04`) assertions that both `disagg_block_put` and `disagg_block_get` are positive after write/read workloads.

**Gap 5 [HIGH]: `disagg_role_leader` is never asserted**

- Scenario: After `conn.reconfigure('disaggregated=(role="leader")')`, assert `stat.conn.disagg_role_leader == 1`. After step-down, assert it returns to 0.
- Risk: If the role-tracking stat is decoupled from the actual role stored in the connection, the wrong role could be reported to monitoring tools. This is undetected.
- Suggested test: Simple reconfigure round-trip test reading this stat before and after each role change.

**Gap 6 [HIGH]: `disagg_conn_reconfig` is never asserted**

- Scenario: Call `conn.reconfigure('disaggregated=(role="leader")')` N times and assert `stat.conn.disagg_conn_reconfig == N`.
- Risk: Counter may never be incremented (dead code path) and the miss goes undetected.
- Suggested test: Loop reconfigures and assert the counter value.

**Gap 7 [MEDIUM]: `disagg_step_up_time` is never asserted**

- Scenario: Perform a step-up, then assert `stat.conn.disagg_step_up_time > 0`.
- Risk: If the step-up timing instrumentation is accidentally removed, the SLO monitoring metric silently disappears.
- Suggested test: Add stat assertion alongside existing step-up reconfigure tests (e.g., `test_layered25`, `test_layered30`).
- Note: `disagg_step_down_time` is not asserted here as it requires elegant step-down (see SD-4 in `08_unsupported_features.md`).

**Gap 8 [MEDIUM]: `disagg_abandon_checkpoint_failed` and `disagg_abandon_checkpoint_succeed` are never asserted**

- Scenario: Force an abandon-checkpoint scenario (e.g., step down during checkpoint) and assert the corresponding counter increments.
- Risk: The abandon-checkpoint path is a complex code path that is already tested in `test_layered62`, but no test confirms the stat counters are correctly updated, making the path invisible to monitoring.
- Suggested test: Extend `test_layered62` to assert one of these counters.

**Gap 9 [MEDIUM]: `disagg_database_size` is never asserted**

- Scenario: After writing and checkpointing data, assert `stat.conn.disagg_database_size > 0`. After a table drop and checkpoint, assert the size decreases.
- Risk: If the database size tracking is broken, capacity planning and monitoring dashboards receive stale data. The bug is undetected.
- Suggested test: Assert the stat after writes and again after a drop + checkpoint.

**Gap 10 [LOW]: `disagg_block_hs_byte_read`, `disagg_block_hs_byte_write`, `disagg_block_hs_get`, `disagg_block_hs_put`, and `disagg_block_read_ahead_frontier` are never asserted**

- Scenario: Trigger history store activity on a layered table (e.g., with MVCC reads requiring HS lookups) and assert the HS byte counters are positive.
- Risk: History store I/O on disaggregated tables is not metered in tests. Silent regressions (e.g., extra HS reads caused by a bug) are invisible.
- Suggested test: Add HS stat assertions to tests that already exercise timestamp-based reads requiring HS (e.g., `test_layered84`, `test_layered85`).

---

### Multi-Session Concurrency

**Covered:**

- `test_layered45` opens two or three sessions on the same connection concurrently and tests that an uncommitted write in `session2` prevents a checkpoint from producing a new delta (but does not test interleaved reads and writes).
- `test_layered_fast_truncate01` uses two sessions to test truncate write conflicts.
- `test_layered62` uses a checkpoint thread concurrently with `conn.reconfigure()` role changes.

**Gap 11 [HIGH]: No test for concurrent reader + active writer on the same layered table**

- Scenario: Session1 begins a write transaction inserting 1000 keys into `layered:table`. Session2 begins a read-only transaction and scans from start to finish. While Session2 is mid-scan, Session1 commits. Verify Session2 sees a consistent snapshot (the keys absent from its read transaction timestamp).
- Risk: The layered scan logic merges ingest and stable btrees. If the merge cursor is not snapshot-isolated from concurrent ingest writes, a reader could see partially-committed data or skip keys.
- Suggested test: Thread-based test with `threading.Thread` similar to `test_layered62`. Writer inserts in a loop; reader asserts it never sees uncommitted data.

**Gap 12 [HIGH]: No test for concurrent writers on the same key in a layered table detecting WT_ROLLBACK**

- Scenario: Session1 and Session2 both write to the same key K in `layered:table` within overlapping transactions. Assert that exactly one gets `WT_ROLLBACK` on commit.
- Risk: Write conflict detection is already tested for truncate (`test_layered_fast_truncate01`) but not for plain insert/update on the same key. The layered ingest btree's MVCC chain could handle this differently from regular btrees.
- Suggested test: Two sessions doing `session.begin_transaction()`, both writing the same key, one commits and the other calls `commit_transaction()` and asserts `WT_ROLLBACK`.

**Gap 13 [MEDIUM]: No test exercising 10+ simultaneous sessions on a single layered table**

- Scenario: Open 10 sessions, each concurrently inserting to distinct key ranges in the same layered table, then checkpoint and verify all keys are present.
- Risk: Latch contention on the ingest btree root or layered metadata lock is untested at any meaningful scale.
- Suggested test: Extend an existing parallel-write test to use `layered:` URIs.

---

### Cursor Duplication

**Covered:**

- `test_dupc.py` tests `session.open_cursor(None, cursor, None)` for `file:` and `table:` URIs but does not include `layered:` URIs.
- No `test_layered*.py` file calls `session.open_cursor(None, cursor, None)` on a layered cursor.
- `hook_disagg.py` intercepts `open_cursor` calls but makes no special provision for cursor duplication on layered tables.

**Gap 14 [HIGH]: cursor.dup on a positioned layered cursor is completely untested**

- Scenario: Open a cursor on `layered:table`, call `cursor.search()` to position it on a key that is in the ingest btree (recently written). Call `session.open_cursor(None, cursor, None)` to duplicate it. Verify the duplicate is positioned at the same key and can iterate forward and backward correctly.
- Risk: Layered cursor reads merge two physical cursors (ingest and stable). The `dup` path may not correctly propagate the merged position to the duplicate, leaving it positioned at the wrong key or causing a use-after-free on the internal state.
- Suggested test: `test_layered_cursor_dup01.py` covering dup from a position in ingest, a position in stable, and a cross-layer position (key in both).

**Gap 15 [MEDIUM]: cursor.dup on a cursor positioned across the ingest/stable boundary is untested**

- Scenario: Write key K1 to stable (via checkpoint) and key K2 to ingest (uncommitted to checkpoint). Position a cursor between K1 and K2. Duplicate the cursor. Iterate from the duplicate and verify correct cross-layer ordering.
- Risk: The merged cursor's position encoding may not survive round-tripping through the dup API if it relies on state that is not duplicated along with the cursor handle.
- Suggested test: Extend `test_layered_cursor_dup01.py` with a cross-layer positioning scenario.

---

### Connection Configuration and Lifecycle

**Covered:**

- `test_layered39` and `test_layered34` call `conn.reconfigure('disaggregated=(last_materialized_lsn=X)')` and verify eviction behavior and that the LSN cannot go backwards.
- `test_layered62` calls `conn.reconfigure('disaggregated=(role="follower")')` concurrently with a running checkpoint.
- `test_layered64` tests corrupt `checkpoint_meta` values on reconfigure.
- `test_layered68` tests `checkpoint_meta` with an incompatible address-cookie version.
- `test_layered88` verifies that `wiredtiger_open(..., 'readonly=true,disaggregated=...')` returns a clear error message.
- `test_layered27`, `test_layered94`, `test_layered86` call `conn.close('debug=(skip_checkpoint=true)')` to simulate a crash at connection close.

*(Moved to 08_unsupported_features.md as SD-4 — requires elegant step-down)*

*(Moved to 08_unsupported_features.md as SD-5 — requires elegant step-down)*

**Gap 16 [MEDIUM]: Connection close with open cursors on layered tables is untested**

- Scenario: Open a cursor on `layered:table`, do not close it, then call `conn.close()`.
- Risk: WiredTiger normally sweeps open cursors on connection close, but the layered dhandle cleanup may have a different path than regular file cleanup. An unreleased reference on the ingest btree's dhandle could cause a hang or assert during close.
- Suggested test: Intentionally leave a cursor open and call `conn.close()`. Verify clean termination (no hang, no assert).

**Gap 17 [MEDIUM]: Connection close with an in-flight drain is untested**

- Scenario: Start a large workload to trigger background drain activity, then call `conn.close()` without waiting for drain to complete.
- Risk: A connection close that arrives while the drain thread holds locks on both the ingest and stable btrees could deadlock or leave the page log in a corrupted state.
- Suggested test: Use `timing_stress_for_test` to lengthen drains; force a `conn.close()` concurrently and verify clean shutdown.

**Gap 18 [LOW]: Bad disaggregated config values at wiredtiger_open are sparsely tested**

- Scenario: Open a connection with invalid config such as an unrecognized `role` value, a negative `last_materialized_lsn`, or conflicting `lose_all_my_data` with `readonly=true`.
- Risk: Config parsing errors in the disagg path may not surface through the standard `EINVAL` path, producing silent misbehavior or an unclear error message.
- Suggested test: Parametric test with a table of invalid configs, each asserting the expected error code and message.

---

### Read-Only Connections

**Covered:**

- `test_layered88.test_readonly` verifies that `wiredtiger_open(home, 'readonly=true,...disaggregated=...')` raises `WiredTigerError` with the message `'disaggregated storage is not supported with read-only connections'`. This is the rejection path for FIXME-WT-17177.

**Gap 19 [LOW]: The positive case for read-only followers (once WT-17177 is fixed) is completely absent**

*(Currently skipped, FIXME-WT-17177; keep at LOW pending WT-17177 fix)*

- Scenario: Once WT-17177 is resolved, a follower connection opened with `readonly=true` should be able to read data from the stable btree. Test that: (a) reads succeed, (b) writes are rejected with `EACCES`, (c) statistics are reported correctly, (d) the connection can be closed cleanly.
- Risk: If there is no test for the positive path, the implementation of WT-17177 has no automated regression guard. Any subsequent change that accidentally re-introduces the rejection could go undetected.
- Suggested test: Create `test_layered_readonly01.py` that: opens a leader, writes and checkpoints, opens a second connection as `role="follower",readonly=true`, reads all keys, attempts a write and asserts `EACCES`.

**Gap 20 [MEDIUM]: hook_disagg.py silently skips all tests with read-only connections**

- Scenario: The hook skip for `readonly=true` means no format-test scenario exercises disagg tables under a read-only connection. Once WT-17177 is fixed, the hook skip should be lifted or narrowed.
- Risk: After WT-17177 is resolved, if nobody updates the hook, entire test classes remain skipped indefinitely, creating a permanent false sense of untestable coverage.
- Suggested action: Add a `FIXME-WT-17177` comment in the hook at line 94 with instructions to remove the skip and add an appropriate test after the ticket is resolved.

---

### Error Handling

**Covered:**

- `test_layered64` tests a corrupted `metadata_checksum` on `conn.reconfigure(checkpoint_meta=...)`.
- `test_layered31` tests that setting a non-existent `checkpoint_meta` value raises `WT_NOTFOUND`.
- `test_layered68` tests that a `checkpoint_meta` with an incompatible address-cookie version raises a clear error.
- `test_layered88` tests that `readonly=true` with disagg config is rejected.

**Gap 21 [HIGH]: No test injects storage-layer failures (e.g., page-log write errors)**

- Scenario: Inject a simulated write error at the page-log layer (e.g., via a mock or a fault-injecting page log extension) and verify that WiredTiger returns a clean error to the caller rather than silently corrupting data or hanging.
- Risk: The disagg block layer wraps an external page-log API. If that API returns an unexpected error code, the disagg code may not propagate it correctly, leading to a silent data loss or assertion failure in production.
- Suggested test: Implement a fault-injecting page-log wrapper and call it from a test that verifies the error propagation path.

**Gap 22 [MEDIUM]: No test for the behavior when the page log reports fewer pages than expected**

- Scenario: After a leader writes and checkpoints 100 pages, manually remove some entries from the page log (simulating an incomplete flush to cold storage), then open a follower and attempt to read all data. Verify a clear error is returned.
- Risk: If the follower silently serves `WT_NOTFOUND` or returns garbage data for missing pages instead of an error, the application has no way to detect that storage is incomplete.
- Suggested test: Use the page-log API to truncate entries, then verify the follower's read behavior.

**Gap 23 [LOW]: Error path when reconfigure is called with both `last_materialized_lsn` and `role` in the same call**

- Scenario: Call `conn.reconfigure('disaggregated=(role="leader",last_materialized_lsn=100)')`. According to the source (`conn_reconfig.c:435`), the code path skips HS reconfiguration when only `last_materialized_lsn` is set. A combined call may hit an untested code path.
- Risk: Combined config keys could trigger unexpected behavior in the config-parsing fast path.
- Suggested test: Assert that combining these two sub-keys in one reconfigure call either succeeds cleanly or returns a clear error.

---

## Priority-Ranked Gap List

### CRITICAL

1. **Gap 1** — No atomic committed transaction spanning two layered tables. Data atomicity across layered tables is a fundamental correctness property with no test coverage.

### HIGH

2. **Gap 4** — `disagg_block_get` and `disagg_block_put` counters (core I/O path stats) are never asserted.
3. **Gap 5** — `disagg_role_leader` counter is never asserted.
4. **Gap 6** — `disagg_conn_reconfig` counter is never asserted.
5. **Gap 11** — No concurrent reader + active writer scenario on the same layered table.
6. **Gap 14** — `cursor.dup()` on a layered cursor is completely untested.
7. **Gap 21** — No fault injection for storage-layer write errors.
8. **Gap 12** — Concurrent plain-insert write conflict detection (WT_ROLLBACK) is untested on layered tables.

### MEDIUM

9. **Gap 2** — Rollback atomicity across two layered tables is untested.
10. **Gap 7** — `disagg_step_up_time` and `disagg_step_down_time` are never asserted.
11. **Gap 8** — `disagg_abandon_checkpoint_failed/succeed` counters are never asserted.
12. **Gap 9** — `disagg_database_size` counter is never asserted.
13. **Gap 13** — No 10+ simultaneous session concurrency stress test on layered tables.
14. **Gap 15** — `cursor.dup()` on a cross-layer boundary position is untested.
15. **Gap 16** — Connection close with open cursors on layered tables is untested.
16. **Gap 17** — Connection close with in-flight drain is untested.
17. **Gap 20** — hook_disagg.py skip for read-only connections has no post-fix removal path.
18. **Gap 22** — Follower behavior when page-log entries are missing is untested.
19. **Gap 3** — Write-conflict detection across two layered tables in the same transaction is untested.

### LOW

20. **Gap 10** — HS-related disagg stat counters (`disagg_block_hs_*`, `disagg_block_read_ahead_frontier`) are never asserted.
21. **Gap 18** — Bad disagg config values at `wiredtiger_open` are sparsely tested.
22. **Gap 19** — The positive read-only follower path (post WT-17177 fix) has no test — keep at LOW pending WT-17177 fix.
23. **Gap 23** — Combined `role` + `last_materialized_lsn` in a single reconfigure call is untested.
