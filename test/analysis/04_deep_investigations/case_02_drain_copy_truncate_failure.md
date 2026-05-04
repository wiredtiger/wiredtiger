# Case 02: Drain Copy→Truncate Failure and Crash Mid-Drain

**Priority:** CRITICAL  
**Source files:** `src/conn/conn_layered_ingest.c:540–576, 97–119, 309–531, 617–709`  
**Related FIXMEs:** FIXME-WT-14734 (manager lock), FIXME-WT-14735 (skip empty ingest)  
**Status:** Two sub-cases; both confirmed as real gaps with no current test coverage

---

## Architecture: The Copy→Truncate Sequence

Drain moves data from each ingest btree to the corresponding stable btree in `__layered_drain_worker_run` (`conn_layered_ingest.c:540–576`):

```c
// Step 1: copy all ingest updates to stable btree
WT_ERR_MSG_CHK(session, __layered_copy_ingest_table(session, work_item->entry), ...);  // line 554

// Step 2: truncate the ingest btree (separate transaction)
WT_ERR_MSG_CHK(session, __layered_clear_ingest_table(session, work_item->entry->ingest_uri), ...);  // line 557

// Step 3: (debug) assert ingest is empty
// Step 4: reset prune timestamp
// Step 5: decrement pinned dhandle refcount  ← ONLY REACHED ON FULL SUCCESS

err:
    __wt_free(session, work_item);   // line 575
    return (ret);
```

The two operations are **not atomic**. `__layered_clear_ingest_table` (lines 97–119) is itself a separate WiredTiger transaction:

```c
WT_RET(__wt_txn_begin(session, NULL));
F_SET(session->txn, WT_TXN_TS_NOT_SET);   // timestamps not needed — no concurrent txns
WT_RET(session->iface.truncate(&session->iface, uri, NULL, NULL, NULL));
WT_RET(__wt_txn_commit(session, NULL));
```

---

## Sub-case A: Copy fails (partial write to stable)

### What happens

`__layered_copy_ingest_table` iterates the ingest version cursor key-by-key and calls `__wt_row_modify` on the stable btree for each key. It can fail at multiple points:

- Line 349: `__wt_open_cursor` for the ingest version cursor (allocation, open failure)
- Lines 353–355: `__wt_scr_alloc` for key/tmp_key/value scratch buffers
- Line 359: `ingest_version_cursor->next` (I/O error, page corruption)
- Line 85: `__wt_row_modify` on the stable btree (page write failure)

On any of these failures, `WT_ERR` jumps out of the copy loop. The stable btree may have received 0 to N-1 of the ingest keys. The function returns the error to `__layered_drain_worker_run`, which jumps to `err:`.

**State after copy failure:**
- Stable btree: 0 to N-1 keys written (partial, not checkpointed)
- Ingest btree: fully populated (unchanged)
- `entry->pinned_dhandle`: refcount incremented at line 669 but **never decremented** (lines 568-572 not reached) → permanent refcount leak, layered table dhandle cannot be closed

**What the caller sees:**
`__wti_layered_drain_ingest_tables` at line 696 calls `__layered_drain_worker_run` and gets the error. It jumps to `err:` at line 699 which destroys the thread group and clears the work queue. The error propagates to `__disagg_step_up`, which triggers `__wt_panic` ("failed to step-up as primary") via the path documented in Case 01 Sub-case A.

So copy failure → panic. But the partial stable writes exist in memory and will be wiped on the next open. This is arguably acceptable (panic destroys the connection), BUT the dhandle refcount leak means the dhandle is never closed cleanly.

---

## Sub-case B: Copy succeeds, truncate fails (data duplication)

### What happens

`__layered_copy_ingest_table` succeeds. Stable btree now has all ingest data in memory (not yet checkpointed). Then `__layered_clear_ingest_table` runs:

```c
WT_RET(__wt_txn_begin(session, NULL));
WT_RET(session->iface.truncate(..., uri, ...));   // ← can fail: I/O, ENOSPC
WT_RET(__wt_txn_commit(session, NULL));
```

If the truncate fails (e.g., disk full writing the truncate log entry, I/O error on ingest pages), `WT_RET` propagates the error up. The transaction is begun but NOT committed. The ingest btree is still fully populated.

**State after truncate failure:**
- Stable btree: has all ingest data in memory (correct)
- Ingest btree: still fully populated (bad — same data in two places)
- `entry->pinned_dhandle`: refcount leak (same as copy failure)

**On subsequent checkpoint:**
The stable btree checkpoint persists the data (stable btree is not READONLY at this point — this is mid-step-up where `leader=true` is already set). The checkpoint completes successfully.

**On restart as follower:**
Stable btree has the data (from checkpoint). Ingest btree also has the data. When a new leader steps up, the drain would re-apply all the ingest records on top of the stable btree. For insert-only workloads this means duplicate records at potentially conflicting timestamps. For update workloads this means updates applied twice to the same key with undefined outcome.

**No detection or compensation mechanism exists:**
- There is no "drain-in-progress" marker written before copy starts
- There is no "copy-completed, truncate-not-done" flag
- On restart, the system has no way to know whether ingest data is "fresh writes" or "copy succeeded but truncate failed"

### Existing tests

All drain tests exercise only the success path:

| Test | What it covers |
|---|---|
| `test_layered27` | Insert/update/remove sequences, step_up triggers drain, data verified in stable after checkpoint |
| `test_layered38` | GC and cursor pinning during drain — blocks but eventually succeeds |
| `test_layered47` | Prune timestamp correctness — regression tests, drain always succeeds |
| `test_layered34` | Materialization frontier after drain — drain always succeeds |
| All others (`test_layered37`, `49`, `60`, `45`) | Specific regression scenarios — drain always succeeds |

**No test injects a failure during copy or truncate.** Confirmed by grepping all `test_layered*.py` files: zero occurrences of `ENOSPC`, fault injection, or forced copy/truncate failure.

---

## Sub-case C: Crash mid-drain (persistent inconsistency)

### What happens

Unlike the error-return case, a SIGKILL (crash) during drain cannot be gracefully handled. Three crash windows exist:

**Window 1: Crash during copy (after partial stable write)**
- Ingest fully populated (crash before truncate → WAL replays ingest as intact)
- Stable has partial writes that are NOT checkpointed → on restart, stable reverts to its last checkpoint (no partial data)
- Net result: ingest has data, stable does not → **clean state, restart is safe, no data loss**

**Window 2: Crash during truncate (`begin_txn` committed, mid-page clear)**
- Ingest transaction is open (not committed) → WAL replay rolls it back → ingest is fully populated
- Stable was checkpointed (if checkpoint ran after copy completed) or not
- If stable was NOT checkpointed: same as Window 1, clean
- If stable WAS checkpointed: stable has data, ingest has data → **duplication scenario**

**Window 3: Crash after truncate commit, before prune timestamp reset**
- Ingest is empty (truncate committed and survived crash)
- Stable has data from copy
- Prune timestamp is stale — on restart, GC may not correctly retain old ingest versions
- Net result: data correct but prune timestamp may cause premature GC

**There is no persistent drain-in-progress marker.** The WiredTiger metadata has no record of whether a drain was started, copy-completed, or truncate-completed. On restart, the code has no way to distinguish between:
1. Ingest with data that was never drained (normal state → should drain on step_up)
2. Ingest with data that was fully copied to stable but truncate crashed (should truncate, not re-copy)

### Why this matters in practice

The drain is called during `__disagg_step_up` — itself on the critical path for replica promotion. In production:
- A node crashes mid-promotion (step_up was interrupted)
- On restart, the node's ingest btree may contain data that is already in stable
- The next leader step_up re-drains, writing duplicate/conflicting updates

The analogous non-disagg scenario (WAL corruption) is extensively tested by `csuite/timestamp_abort`. No equivalent exists for disagg drain.

---

## Related resource leak (all error paths)

In `__layered_drain_worker_run`, the `entry->pinned_dhandle` refcount increment at line 669 is only decremented at lines 568-572 (success path). On any error (copy failure, truncate failure, assert failure), the dhandle refcount is leaked:

```c
// Line 669 (success guaranteed): increments refcount
WT_ERR(__wt_cursor_uri_incr_use(session, entry->layered_uri, &entry->pinned_dhandle));

// Lines 568-572 (ONLY REACHED ON FULL SUCCESS): decrements refcount
WT_WITH_DHANDLE(session, work_item->entry->pinned_dhandle, {
    work_item->entry->pinned_dhandle = NULL;
    __wt_cursor_dhandle_decr_use(session);
});

err:
    __wt_free(session, work_item);   // frees the queue item but NOT the dhandle refcount
    return (ret);
```

After a failed drain, the pinned dhandle refcount is 1 too high. Since drain failure leads to `__wt_panic`, the connection is destroyed and this never manifests as an observable bug in practice — but it is a latent issue if retry semantics are ever added.

---

## Proposed tests

### `test_layered_drain_errors`

**What it should do:**

The only practical approach without OS-level fault injection is to create conditions where truncate fails predictably. A more realistic approach is to verify the semantic invariants:

```python
# Verify the pre-condition: ingest and stable are consistent after a restart
# following a simulated partial drain

# Setup: leader writes data to ingest
conn = wiredtiger_open(home, 'disaggregated=(role="leader"), ...')
session.create('layered:test', ...)
cursor['key1'] = 'value1'
...
cursor['key100'] = 'value100'

# Simulate "copy succeeded, truncate failed" by:
# (a) manually reading stable after drain to verify data arrived
# (b) NOT truncating ingest (simulating the failure)
# (c) opening a new leader connection and verifying step_up handles duplicate data

conn.reconfigure('disaggregated=(role="follower")')
conn.reconfigure('disaggregated=(role="leader")')   # drain runs again
# Assert: no crash, data in stable is correct (no duplicates, correct values)
# Assert: ingest is empty after second drain (truncate completed)
```

Since fault injection into `__layered_clear_ingest_table` is not available from Python, the most achievable test verifies that **repeated drain of the same ingest content is idempotent** — i.e., if stable already has the data and ingest also has it, a second drain produces correct results. This is the property that would be needed for correct recovery.

### `test_layered_drain_crash_recovery`

Requires SIGKILL injection:
- Requires `wtscenario` crash/recovery infrastructure (like `test_timestamp_abort`)
- Crash at random points during a drain-heavy step_up
- Verify on restart: no data loss, no duplication, consistent state

---

## Summary

| Sub-case | Severity | Bug confirmed? | Tests covering it | Action |
|---|---|---|---|---|
| Copy failure → partial stable write | HIGH | Yes (by code reading) | None | Document + refcount fix |
| Truncate failure after copy → duplication | CRITICAL | Yes — no compensation exists | None | New test for idempotency |
| Crash mid-drain → persistent inconsistency | CRITICAL | Yes — no drain-in-progress marker | None | csuite crash test |
| `pinned_dhandle` refcount leak on error | LOW | Yes (code path verified) | None | Fix on error path |
