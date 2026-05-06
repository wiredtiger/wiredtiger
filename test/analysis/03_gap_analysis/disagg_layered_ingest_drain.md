# Gap Analysis: WiredTiger Disaggregated Layered Ingest-Drain Mechanism

**Scope:** Test coverage for layered ingest table drain process  
**Primary source:** `src/conn/conn_layered_ingest.c`, drain sections of `src/conn/conn_layered.c`  
**Related tests:** test_layered27, test_layered34, test_layered37, test_layered38, test_layered39, test_layered45, test_layered47, test_layered49, test_layered60

---

## Executive Summary

The layered ingest-drain mechanism is a critical path for disaggregated storage: it transfers accumulated writes from the ingest btree into the stable btree (backed by the page log) and is the key operation during follower→leader promotion. Current tests cover the happy path but miss **10 major scenarios** spanning memory pressure, concurrent writes, error paths, multithreaded drain, crash recovery, and edge cases with prepared transactions and tombstones.

**Risk summary:** 2 CRITICAL, 2 HIGH, 6 MEDIUM gaps.

---

## Architecture Overview

A layered table has two btrees:
- **Ingest btree** (`.wt_ingest`): receives all writes, held in-memory/local disk
- **Stable btree** (`.wt_stable`): backed by the page log; readable by followers

The **drain process** (`__wti_layered_drain_ingest_tables`) periodically copies ingest content into the stable btree via checkpoint, enabling followers to read the latest data. During leader step-up, a full drain completes before the connection is marked ready.

### Key functions in `conn_layered_ingest.c`

| Function | Lines | Role |
|---|---|---|
| `__wti_layered_drain_ingest_tables` | 617–709 | Orchestrates single/multithreaded drain |
| `__layered_copy_ingest_table` | 309–531 | Iterates ingest version cursor, migrates to stable |
| `__layered_clear_ingest_table` | 97–119 | Truncates ingest btree after successful copy |
| `__layered_move_updates` | 67–90 | Moves one key's update chain to stable |
| `__layered_fix_prepared_transaction` | 290–302 | Redirects active prepared txns from ingest to stable |
| `__layered_update_ingest_table_prune_timestamp` | 728–865 | Advances prune timestamp for GC |
| `__layered_drain_worker_run` | 540–566 | Worker thread: dequeue work, call copy+clear |

---

## Current Test Coverage

### What is tested

- **test_layered27**: Basic drain — insert/update/remove sequences, follower→leader promotion, data correctness after drain
- **test_layered38**: Garbage collection and cursor pinning during drain (WT-14994)
- **test_layered47**: Prune timestamp correctness regression tests (WT-15158, WT-15192)
- **test_layered37**: Pinned ingest page eviction prevention
- **test_layered49**: Tombstone retention during eviction
- **test_layered60**: Empty table creation during concurrent checkpoint
- **test_layered45**: Delta skip logic with prepared transactions
- **test_layered34, test_layered39**: Materialization frontier and eviction gates

### Summary of what is covered

- Basic follower→leader promotion and ingest drain (happy path)
- GC and cursor pinning blocking drain
- Prune timestamp correctness (regression coverage for specific bugs)
- Prepared transaction handling with `PRESERVE_PREPARED=true`
- Tombstone lifecycle
- Ingest page eviction prevention

---

## Coverage Gaps

---

### GAP-1: Drain Under Memory Pressure (HIGH)

**What is missing:**
No test exercises ingest drain when `cache_size` is small relative to ingest data size. No test covers `scr_alloc` failures during cursor creation, or drain slowing under eviction pressure.

**Code at risk:**
- Line 349: `__wt_open_cursor()` for `ingest_version_cursor`
- Lines 353–355: `__wt_scr_alloc()` for key/tmp_key/value buffers
- Line 85: `__wt_row_modify()` insertion under eviction pressure

**Production danger:**
Large replicas (100s GB ingest) stepping up could exhaust memory mid-drain. There is no graceful degradation — allocation failures propagate as errors and fail the step-up. No visibility into drain progress under memory stress.

**Proposed test:** `test_layered_drain_memory_pressure`
- Configure `cache_size=256MB`
- Insert 1M records (~1GB on disk) into ingest
- Step up; verify drain completes without OOM
- Check statistics for eviction/restore counts during drain

---

### GAP-2: Concurrent Writes During Drain (MEDIUM)

**What is missing:**
No test for writes to ingest while the version cursor is iterating. No test for a race between version cursor advancement and user transaction commits. No test for update chain consistency under concurrency.

**Code at risk:**
- Line 359: `ingest_version_cursor->next()` racing with ingest inserts
- Line 85: `__wt_row_modify()` for stable while ingest is being modified
- Line 82: Tombstone assertion could fail if delete races with drain iteration

**Production danger:**
Drain should block writes during step-up, but if enforcement is incomplete, the copy could miss updates inserted after the cursor was positioned. Update chains could be corrupted if concurrent deletions/inserts interfere with the version cursor walk.

**Proposed test:** `test_layered_drain_concurrent_writes`
- Start a background writer thread during drain
- Verify no data loss from concurrent operations
- Check update chains are not corrupted post-drain

---

### GAP-3: Error Paths and Partial Failure (CRITICAL)

**What is missing:**
No test for `__layered_copy_ingest_table()` failure mid-copy. No test for truncate failure (e.g., disk full, I/O error). No test for worker thread failure during multi-table drain. No test for recovery after partial drain.

**Code at risk:**
- Line 554: Copy failure — work item freed but entry not cleaned
- Line 557: Truncate failure — stable has data (correct), ingest still populated (data duplication)
- Line 561: Assert empty ingest (diagnostic only, not enforced in recovery)
- Lines 701–708: Cleanup on error (thread group destroy may hang if workers are stuck)

**Production danger:**
**Data loss/duplication scenario:** If copy succeeds but truncate fails on disk full:
- Stable now has the ingest data (correct)
- Ingest still has the same data (bad)
- On restart as follower, ingest data is visible alongside stable — the same records exist twice
- A subsequent leader drain would re-apply the same updates

**Hang scenario:** If a worker crashes, the drain supervisor waits indefinitely in the queue loop. Step-up would hang, blocking replica promotion.

**Proposed test:** `test_layered_drain_errors`
- Inject truncate failure (ENOSPC) after successful copy; verify error propagated, ingest remains populated
- Retry drain; verify idempotent recovery
- Inject copy failure; verify stable remains unmodified
- Test worker crash in multithreaded drain (drain_threads=4)

---

### GAP-4: Prepared Transactions During Drain (MEDIUM)

**What is missing:**
No test for concurrent commit/rollback while `__layered_fix_prepared_transaction()` runs. No test for the `PRESERVE_PREPARED=false` code path (lines 404–446). No test for the "prepared fast-truncate ops" assumption documented at line 287. No test for prepared+aborted update chain processing (lines 465–479).

**Code at risk:**
- Lines 290–302: `__layered_fix_prepared_transaction` explicitly assumes "no concurrent commit/rollback" (comment at line 286–287)
- Line 301: Session array walk could observe mid-commit state changes
- Lines 256–268: Marks ingest update `WT_TXN_ABORTED` while redirecting to stable
- Lines 412–446: Prepared update restoration (only tested with `preserve_prepared=true`)

**Production danger:**
The code comment explicitly marks this as a temporary solution. If a prepared transaction commits while `__layered_fix_prepared_transaction` runs:
- Commit applies to stable (good)
- Original ingest update is marked aborted (may violate isolation guarantees)

`PRESERVE_PREPARED=false` (the default, line 332) means prepared updates are dropped during drain — but this default path is untested in existing tests.

**Proposed test:** `test_layered_drain_prepared_txns`
- Scenario A: Commit prepared txn during drain
- Scenario B: Rollback prepared txn during drain
- Scenario C: With `PRESERVE_PREPARED=false` (default configuration)
- Verify no data loss or isolation violations in each scenario

---

### GAP-5: Multithreaded Drain (HIGH)

**What is missing:**
No test for `drain_threads > 1` configuration. No test for concurrent `__layered_move_updates()` calls on the stable btree from multiple workers. No test for work queue contention, thread group teardown with partial failure, or deadlock between drain threads and other operations.

**Code at risk:**
- Line 647: Multithreaded drain decision (`thread_count > 1`)
- Lines 655–658: Thread group creation
- Lines 543–553: Worker dequeue (queue lock contention)
- Line 85: Concurrent `row_modify` on stable btree from multiple workers
- Lines 701–705: Thread group cleanup with potential hangs

**Production danger:**
Multithreaded drain is a configuration option (`disaggregated.drain_threads`) but is **completely untested**. If one worker fails, others continue; the stable btree could end up in an inconsistent partial state. Stable btree latch contention between workers could deadlock or cause severe throughput regression. A hang during step-up would block replica promotion indefinitely.

**Proposed test:** `test_layered_drain_multitable`
- Configuration: `drain_threads=4`
- Create 4–10 layered tables with varying ingest sizes
- Step up; verify all tables drained without deadlock or hang
- Check worker pool statistics show threads were actually active
- Include a scenario with mixed table sizes to verify scheduling fairness

---

### GAP-6: Crash Recovery Mid-Drain (CRITICAL)

**What is missing:**
No test for crash during copy (before truncate). No test for crash during truncate (mid-operation). No test for crash after truncate but before prune timestamp reset. No recovery logic verification for incomplete drain on restart.

**Code at risk:**
- Lines 554–566 in `__layered_drain_worker_run()`: no crash injection in the window between `__layered_copy_ingest_table` success and `__layered_clear_ingest_table` success
- No persistent drain-in-progress marker anywhere in the code
- No WAL entry for drain state transitions
- Restart has no mechanism to detect that drain was incomplete

**Production danger:**
**Duplicate data scenario:** Crash after copy, before truncate:
- Ingest still has data (clear was not reached)
- Stable has data (copy succeeded)
- On restart as follower, both ingest and stable data are visible simultaneously
- The same records exist in two places; semantics are undefined

**Incomplete truncate:** Crash during multi-page truncate:
- Some pages cleared, some remain in ingest
- On restart, the ingest btree is in an inconsistent state
- Next drain attempt might re-process partially-cleared data

**Proposed test:** `test_layered_drain_crash_recovery`
- Requires crash injection (SIGKILL at specific points via hooks)
- Scenario A: Kill process after copy, before truncate; verify behavior on restart
- Scenario B: Kill process mid-truncate; verify truncate is safely idempotent or recoverable
- Scenario C: Kill after truncate, before prune timestamp reset; verify prune state on restart

---

### GAP-7: Delete-Heavy / All-Tombstone Workloads (MEDIUM)

**What is missing:**
No test for 100% tombstone ingest (all deletes, no inserts). No test for delete-heavy followup (90% deletes, 10% updates). Existing tombstone test (WT-15721 regression) covers only consecutive tombstones in a specific narrow case, not bulk delete workloads.

**Code at risk:**
- Lines 457–458: Tombstone allocation when update type is delete
- Line 82: Assertion "value exists on stable to delete before applying tombstone"
- Lines 504–513: Update chain building with only tombstones
- Lines 25–58: `__layered_assert_tombstone_has_value_on_stable_btree()`

**Production danger:**
The assertion at line 82 would crash the drain process (and fail step-up) if a key is deleted in ingest but has no corresponding value in stable. This could happen in a delete-only workload if the key was never flushed to stable before deletion. All-tombstone ingest after a delete-heavy bulk operation could make a follower permanently unable to promote.

**Proposed test:** `test_layered_drain_tombstones`
- Scenario A: All-tombstone ingest (1000 records, all deleted) — verify drain completes, stable has zero records
- Scenario B: Delete-heavy workload (90% deleted, 10% updated) — verify correct record counts in stable
- Scenario C: Tombstone under eviction pressure — force evict ingest pages during drain iteration
- Scenario D: Large consecutive tombstone chains at scale — verify WT-15721 fix holds

---

### GAP-8: PRESERVE_PREPARED=false Behavior (MEDIUM)

**What is missing:**
Default connection behavior (`PRESERVE_PREPARED=false`) means prepared updates are dropped during drain, but no test explicitly exercises this. Existing tests that use prepared transactions set `preserve_prepared=true` (non-default). Version cursor filtering with `preserve_prepared=false` is not exercised.

**Code at risk:**
- Line 332: `preserve_prepared = F_ISSET(S2C(session), WT_CONN_PRESERVE_PREPARED)`
- Lines 404–405: Condition when `preserve_prepared=false` — prepared updates are skipped
- Line 346: Version cursor config changes with `preserve_prepared=false`
- Lines 412–446: Prepared update restoration path (only reachable when `preserve_prepared=true`)

**Production danger:**
If `preserve_prepared=false` handling is broken, prepared updates would be silently dropped during drain without error — committed transactions lose data with no diagnostic. Since this is the default, it affects all non-explicitly-configured deployments.

**Proposed test:** `test_layered_drain_no_preserve_prepared`
- Do NOT set `preserve_prepared=true` (use default `false`)
- Insert 100 records, prepare 10 updates with `prepare_timestamp > last_checkpoint_timestamp`
- Checkpoint before the prepare timestamp
- Step up; drain should drop prepared updates per policy
- Verify drain completes without crash and stable has only the committed (non-prepared) data

---

### GAP-9: Prune Timestamp Update Race with Drain (MEDIUM)

**What is missing:**
No test for `__layered_update_ingest_table_prune_timestamp()` running concurrently with drain. No test for GC running during a drain operation. No test for the prune timestamp advancing (from checkpoint) while drain is in flight.

**Code at risk:**
- Lines 728–865: Prune timestamp update called from checkpoint context during drain
- Line 854: `__wt_atomic_store_uint64_relaxed(&btree->prune_timestamp, prune_timestamp)`
- Line 151 (drain): `__wt_atomic_store_uint64_relaxed(&btree->prune_timestamp, WT_TS_NONE)` — reset after drain
- Lines 788, 803: Session `inuse` reads while drain manipulates session reference counts (lines 266–267)

**Production danger:**
The prune timestamp is atomic `uint64`, but the logic depends on ordering between the drain reset (to `WT_TS_NONE`) and the checkpoint update. The assertion at line 842 (`prune_timestamp >= btree_prune_timestamp`) could fire if drain resets while a concurrent checkpoint is computing the new prune timestamp. GC might attempt to delete pages that the drain cursor is currently positioned on.

**Proposed test:** `test_layered_drain_prune_race`
- Multiple tables with full ingest data
- Configure checkpoint to run very frequently (every 100ms)
- Step up in background; drain should take several seconds
- Checkpoint triggers prune timestamp updates while drain is running
- Verify no data loss, no assertion failures, and prune timestamp is monotonically non-decreasing

---

### GAP-10: Lock Contention and Deadlock Prevention (MEDIUM)

**What is missing:**
No test for manager lock contention with the early-release race (FIXME-WT-14734). No test for queue lock contention with many workers. No test for deadlock between drain and concurrent schema operations (drop table while drain is in flight). No documentation or test for latch ordering: `manager → queue → stable btree → dhandle`.

**Code at risk:**
- Lines 631, 639: Manager lock is held during queue population, then released after only 8 lines — entries could change after release
- Lines 543–553, 674–676: Queue lock held during worker dequeue
- Line 85: Stable btree page latch taken during concurrent `row_modify`
- Line 669: Pinned dhandle incremented; corresponding drop at line 571

**Production danger:**
Step-up is on the critical path for replica promotion. A deadlock here would hang a node indefinitely. If a table is dropped mid-drain, the work item references a freed entry and the worker could access freed memory. The FIXME at line 639 acknowledges this race explicitly.

**Proposed test:** `test_layered_drain_deadlock_prevention`
- Scenario A: Concurrent schema operations (drop table mid-drain with 4 drain threads) — verify no deadlock (30s timeout = failure), verify drain either completes or table-not-found is handled gracefully
- Scenario B: User threads reading stable btree while workers modify it — verify no deadlock and all threads make forward progress
- Scenario C: Manager lock contention — repeatedly open/close tables while drain is in flight — verify no deadlock

---

## Implementation Roadmap

### Priority 1 — CRITICAL (implement first)

| Test | Gap | Why critical |
|---|---|---|
| `test_layered_drain_errors` | GAP-3 | Copy→truncate failure leaves data in both stable+ingest; no recovery defined |
| `test_layered_drain_crash_recovery` | GAP-6 | No persistent drain-in-progress marker; crash leaves persistent inconsistency |

### Priority 2 — HIGH (implement next)

| Test | Gap | Why high priority |
|---|---|---|
| `test_layered_drain_memory_pressure` | GAP-1 | Large-ingest step-up is a real production scenario |
| `test_layered_drain_multitable` | GAP-5 | Multithreaded drain feature is completely untested; deadlock risk |

### Priority 3 — MEDIUM (complete coverage)

| Test | Gap |
|---|---|
| `test_layered_drain_concurrent_writes` | GAP-2 |
| `test_layered_drain_prepared_txns` | GAP-4 |
| `test_layered_drain_tombstones` | GAP-7 |
| `test_layered_drain_no_preserve_prepared` | GAP-8 |
| `test_layered_drain_prune_race` | GAP-9 |
| `test_layered_drain_deadlock_prevention` | GAP-10 |

---

## Duplicate Cases Found

No strict duplicates were identified among the drain-related tests. However:

- `test_layered27` and `test_layered34` both exercise drain after a checkpoint boundary; `test_layered34` adds materialization frontier tracking but the pre-drain setup is duplicated
- `test_layered37` and `test_layered49` both prevent eviction of ingest pages but from different angles (pinning vs tombstone retention); these are complementary, not duplicate

---

## Key FIXMEs in Source That Correspond to Gaps

| FIXME | Location | Gap |
|---|---|---|
| `FIXME-WT-14734` — manager lock released too early | conn_layered_ingest.c:639 | GAP-10 |
| No persistent drain-in-progress flag | (absent) | GAP-6 |
| `PRESERVE_PREPARED` temporary solution comment | conn_layered_ingest.c:286 | GAP-4 |
| Assertion at line 82 assumes value exists on stable | conn_layered_ingest.c:82 | GAP-7 |
