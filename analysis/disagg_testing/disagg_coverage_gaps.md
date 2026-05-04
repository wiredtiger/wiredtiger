## Coverage report: [WT-17223](https://jira.mongodb.org/browse/WT-17223)

Commit: [473b5815d95cc3a98a1851e4ec300731c0156c04](https://github.com/wiredtiger/wiredtiger/commit/473b5815d95cc3a98a1851e4ec300731c0156c04)

Run date: 2026-05-04. Overall: **75.4% lines**, **57.8% branches** (286 files).

Analysis covers only disagg-relevant components (per `analysis/disagg_components.md`).
Error-branch gaps are omitted unless they affect a path exercised during normal operation
or could cause silent incorrect behavior.
Cross-referenced against `analysis/unsupported_disagg.md`.
Coverage verified line-by-line from `coverage_report/full_coverage_report.json`.

Coverage source scope: the coverage run uses only `base01` through the disagg hook
(`--hook disagg ... base`) plus `test_key_provider_disagg01/02.py`. None of the 103
`test_layered*.py` files are included; see `analysis/test_coverage/coverage_analysis.md`.
This means follower-specific paths and role-transition paths are systematically
under-exercised.

Gaps for unsupported features (fast truncate, prepared transactions, elegant step-down,
modify::ops, compact/salvage) are tracked separately in `disagg_unsupported_gaps.md`.

---

## Priority legend

| Priority | Meaning |
|---|---|
| **Critical** | Supported feature with a real correctness or silent-corruption risk |
| **Important** | Supported feature, gap is in a path exercised during normal operation |
| **Moderate** | Edge case, retry path, or less-frequent branch in a supported feature |
| **Low** | Debug path, dead code, or ENOTSUP stub for a supported feature |

---

## Priority summary

**Critical**
- `cur_layered.c` — tombstone encode/decode: value starting with the tombstone byte never tested (169M checks, condition always false → silent corruption risk)
- `block_disagg_read.c` — materialization-frontier check never triggered (FIXME-WT-15818: warns but never fails)

**Important**
- `cur_layered.c` — `__clayered_reopen_stable`: cursor position not preserved when stable cursor is reopened while positioned
- `cur_layered.c` — `__clayered_lookup`: key found in truncate list path never triggered (1.9M calls to outer function, body always skipped)
- `cur_layered.c` — `__clayered_search_near_int`: stable cursor iteration path on truncated-key boundary never exercised
- `cur_layered.c` — `__clayered_largest_key`: both-cursor comparison never needed (stable always wins or ingest-only in all tests)
- `conn_layered.c` — `__disagg_discard_old_checkpoint_check`: no-checkpoint early exit never taken (7,846 calls, always finds a checkpoint)
- `conn_layered_ingest.c` — `__layered_drain_clear_work_queue`: non-empty queue cleanup never triggered
- `history/hs_cursor.c` — `__wt_hs_btree_truncate`: HS cleanup on table drop always finds empty HS (3,348 calls, always WT_NOTFOUND)

**Moderate**
- `cur_layered.c` — `__clayered_can_advance_stable`: final `return (false)` path not reached
- `cur_layered.c` — `__clayered_position_alternate`: `READ_UNCOMMITTED` compare call never reached
- `cur_layered.c` — `__clayered_search_near_move_ingest_to_opposite_side`: `READ_UNCOMMITTED` branch
- `cur_layered.c` — `__clayered_bound`: failure-path bounds cleanup
- `conn_layered_page_log.c` — `__disagg_get_page`: retry loop (page always found on first try)
- `block_disagg_read.c` — checksum mismatch + corrupt/panic path never triggered
- `history/hs_verify.c` — `__hs_verify` (full-HS scan): whole-database HS consistency check always finds empty HS
- `checkpoint/checkpoint_parallel.c` — thread-create failure path and txn-running panic path uncovered

**Low** (debug path, dead code, or ENOTSUP stub for a supported feature)
- `conn_layered.c` — FIXME-WT-16524 `pl_abandon_checkpoint == NULL` check (code comment says remove it)
- `conn_layered.c` — `__disagg_begin_checkpoint` follower/no-npage_log early return (follower never calls begin_checkpoint in tests)
- `conn_layered.c` — `__shared_metadata_op_to_string` (only reachable from `WT_VERBOSE_DEBUG2`-gated paths)
- `conn_layered_page_log.c` — `__wt_disagg_put_crypt_helper` error branch
- `block_disagg_read.c` — victim-cache (`WT_BLOCK_DISAGG_MODIFIED`) read path
- `block_disagg_read.c` — `__wti_block_disagg_corrupt` and `__block_disagg_read_err` (0 calls — require data corruption to reach)
- `conn/conn_page_history.c` — entire `debug_mode.page_history` diagnostic never enabled
- `support/hash_map.c` — used only by the never-enabled page_history diagnostic

---

## File coverage

| File | Line coverage | Uncovered lines |
|---|---|---|
| `src/cursor/cur_layered.c` | 56% | 789 |
| `src/conn/conn_layered.c` | 61% | 446 |
| `src/conn/conn_layered_ingest.c` | 57% | 247 |
| `src/conn/conn_layered_page_log.c` | 59% | 240 |
| `src/conn/conn_layered_table_manager.c` | 57% | 53 |
| `src/block_disagg/block_disagg_read.c` | 37% | 122 |
| `src/block_disagg/block_disagg_mgr.c` | 55% | 71 |
| `src/block_disagg/block_disagg_open.c` | 52% | 53 |
| `src/block_disagg/block_disagg_write.c` | 61% | 68 |
| `src/block_disagg/block_disagg_ckpt.c` | 61% | 55 |
| `src/block_disagg/block_disagg_addr.c` | 59% | 70 |
| `src/block_disagg/block_disagg_unsup.c` | 17% | 67 (all ENOTSUP stubs — see unsupported doc) |
| `src/cursor/cur_hs.c` | 52% | 391 |
| `src/history/hs_cursor.c` | 54% | 92 |
| `src/history/hs_verify.c` | 51% | 67 |
| `src/reconcile/rec_hs.c` | 60% | 283 |
| `src/conn/conn_page_history.c` | 9% | 298 |
| `src/support/hash_map.c` | 3% | 120 |
| `src/checkpoint/checkpoint_parallel.c` | 55% | 148 |
| `src/txn/txn_truncate.c` | 21% | 168 (mostly fast-truncate PuP — see unsupported doc) |
| `src/prepared_discover/prepared_discover_txn.c` | 0% | 132 (all PuP — see unsupported doc) |
| `src/prepared_discover/prepared_discover_walk.c` | 0% | 291 (all PuP — see unsupported doc) |
| `src/schema/schema_create.c` | 49% | 551 |
| `src/schema/schema_drop.c` | 57% | 149 |
| `src/tiered/tiered_handle.c` | 40% | 335 |
| `src/tiered/tiered_work.c` | 11% | 173 |
| `src/tiered/tiered_config.c` | 54% | 59 |

---

## cur\_layered.c

### 1. [Critical — CORRUPTION RISK] `__clayered_deleted_encode` / `__clayered_deleted_decode` — tombstone-prefix encoding never triggered

*(Verified: `__clayered_is_deleted_encoded` checked 169,289,523 times; true-branch 0 in both encode and decode.)*

A value whose first byte matches the tombstone marker (`__wt_tombstone.data[0]`) must be
re-encoded by appending an extra byte before it is written, so it is not silently misread
as a deletion by a reader. `__clayered_deleted_decode` must strip that byte on the way out.

The check fires on every write (169M calls) and every read decode, but the condition is
never true. If a real application value starts with the tombstone byte, the encode/decode
pair would be exercised, but no test inserts such a value. An undetected regression in this
path would cause a data value to be silently interpreted as a tombstone (delete).

```c
static WT_INLINE int
__clayered_deleted_encode(...)
{
    if (__clayered_is_deleted_encoded(value)) { // 80,867,203 checks — ALWAYS FALSE
        WT_RET(__wt_scr_alloc(session, value->size + 1, tmpp));
        // ... append tombstone byte ...
    }
}

static WT_INLINE void
__clayered_deleted_decode(WT_ITEM *value)
{
    if (__clayered_is_deleted_encoded(value))
        --value->size; // ---> 88,180,965 checks — ALWAYS FALSE
}
```

---

### 2. [Important] `__clayered_reopen_stable` — cursor position not preserved across stable reopen

*(Verified: called 16 times; `F_ISSET(old_stable, WT_CURSTD_KEY_EXT)` and
`clayered->current_cursor == old_stable` conditions both 0.)*

When a follower advances to a new checkpoint while a layered cursor has an external key set
(`WT_CURSTD_KEY_EXT`), the key must be copied to the new stable cursor. When the layered
cursor's `current_cursor` points to the old stable cursor, it must be redirected to the new
one. Neither case is tested, so we don't know whether cursor position is correctly preserved
across a stable cursor swap.

```c
    } else if (F_ISSET(old_stable, WT_CURSTD_KEY_EXT)) { // ---> 16 calls, NEVER TAKEN
        WT_ITEM_SET(clayered->stable_cursor->key, old_stable->key);
        if (F_ISSET(old_stable, WT_CURSTD_VALUE_EXT))
            WT_ITEM_SET(clayered->stable_cursor->value, old_stable->value);
    }

    if (clayered->current_cursor == old_stable) { // ---> NEVER TAKEN
        WT_CURSOR *new_stable = clayered->stable_cursor;
        if (F_ISSET(cursor, WT_CURSTD_KEY_INT)) {
            WT_ITEM_SET(cursor->key, new_stable->key);
            F_CLR(cursor, WT_CURSTD_VALUE_INT);
        }
        clayered->current_cursor = new_stable;
    }
```

---

### 3. [Important] `__clayered_lookup` — key found in truncate list never triggered

*(Verified: `__wt_truncate_delete_visible_check` called 466,437 times; `ret == 0` never true.)*

`__wt_truncate_delete_visible_check` is called on every lookup that misses ingest, to check
whether the key falls within a committed truncation range. It is called 466K times but never
returns 0 (key found in a truncated range). This means we never exercise the path where a
lookup correctly hides a key because it falls within a pending truncation.

```c
        if (!found) {
            WT_ERR_NOTFOUND_OK(__wt_truncate_delete_visible_check(session,
                                 (WT_LAYERED_TABLE *)clayered->dhandle, &cursor->key, NULL),
              true);
            if (ret == 0) { // ---> 466,437 checks — NEVER TRUE
                found = true;
                ret = WT_NOTFOUND;
            }
        }
```

---

### 4. [Important] `__clayered_search_near_int` — stable cursor iteration on truncated-key boundary

*(Verified: the truncate-iteration block entered 2 times; inner `if (ret == 0)` branches 0.)*

When the stable cursor lands on a key inside a truncated range, `__clayered_constituent_iter_helper`
should step it forward (or backward if forward fails) to find the next valid key. The block
is entered 2 times but neither the forward-step nor the backward-step branch is ever taken.

```c
                WT_ERR_NOTFOUND_OK(
                  __clayered_constituent_iter_helper(clayered, clayered->stable_cursor, true), true);
                if (ret == 0)       // ---> NOT COVERED
                    stable_cmp = 1;
                else {
                    WT_ERR_NOTFOUND_OK(
                      __clayered_constituent_iter_helper(clayered, clayered->stable_cursor, false),
                      true);
                    if (ret == 0)   // ---> NOT COVERED
                        stable_cmp = -1;
                }
```

---

### 5. [Important] `__clayered_largest_key` — comparison when both cursors have data

*(Verified: called 12 times; 1 ingest-only, 11 stable-only; else-branch 0.)*

The function is called 12 times but only when one cursor has data. The comparison path
that picks the larger key when both ingest and stable have data is never reached.

```c
    } else { // ---> NOT COVERED — both ingest and stable found
        __clayered_get_collator(clayered, &collator);
        WT_ERR(__wt_compare(session, collator,
          &ingest_cursor->key, &stable_cursor->key, &cmp));
        if (cmp <= 0)
            larger_cursor = stable_cursor;
        else
            larger_cursor = ingest_cursor;
    }
```

---

### 6. [Moderate] `__clayered_can_advance_stable` — final `return (false)` path

*(Verified: the final fall-through return is unreachable in tests.)*

Every call takes the early `return (true)` branch (cursor has a read timestamp or snapshot).
The fall-through path that returns `false` (no snapshot, not an iteration, no read timestamp)
is never exercised.

```c
    return (false); // ---> NOT COVERED — default fall-through
```

---

### 7. [Moderate] `__clayered_position_alternate` — `READ_UNCOMMITTED` comparison

*(Verified: called 1,608 times; isolation != READ_UNCOMMITTED always taken, 123 early returns.)*

Higher-isolation callers always take the early return. The `READ_UNCOMMITTED` path that
re-compares after stepping is never reached.

```c
            if (session->txn->isolation != WT_ISO_READ_UNCOMMITTED)
                return (0);   // ---> ALWAYS TAKEN (123 times)

            WT_RET(__clayered_cursor_compare(clayered, alternate, current, &cmp)); // ---> NEVER REACHED
```

---

### 8. [Moderate] `__clayered_search_near_move_ingest_to_opposite_side` — `READ_UNCOMMITTED` branch

*(Confirmed from `analysis/some_testing_gaps.md`. Still uncovered.)*

```c
            if (ret == 0) // ---> NOT COVERED
                WT_ERR(
                  __wt_compare(session, collator, &ingest_cursor->key, &cursor->key, ingest_cmp));
```

---

### 9. [Moderate] `__clayered_bound` — failure-path bounds cleanup

When `__clayered_copy_bounds` fails, previously applied bounds must be cleaned up. This
failure path (both lower and upper bound cleanup branches) is never reached in tests.

```c
    if (ret != 0) {
        if (F_ISSET(cursor, WT_CURSTD_BOUND_UPPER)) {   // ---> NOT COVERED
            __wt_buf_free(session, &cursor->upper_bound);
            WT_CLEAR(cursor->upper_bound);
        }
        if (F_ISSET(cursor, WT_CURSTD_BOUND_LOWER)) {
            __wt_buf_free(session, &cursor->lower_bound);
            WT_CLEAR(cursor->lower_bound);
        }
        F_CLR(cursor, WT_CURSTD_BOUND_ALL);
        WT_TRET(__clayered_copy_bounds(clayered));
    }
```

---

### 10. [Low] `__disagg_begin_checkpoint` follower / no page-log early return

*(Verified: checked 21,507 times; early return never taken — always leader with npage_log.)*

Tests only run checkpoints from the leader role. The follower and no-page-log paths are
never exercised.

```c
    if (disagg->npage_log == NULL || !conn->layered_table_manager.leader)
        return (0); // ---> 21,507 checks — NEVER TAKEN
```

---

### 11. Additional items confirmed from `analysis/some_testing_gaps.md`

All original items remain uncovered:
- **`__wt_clayered_open` err label** — confirmed uncovered. Low priority (error path on open failure).
- **`__clayered_open_stable_follower` EBUSY retry** — confirmed uncovered. Moderate (retry on checkpoint race during follower open).
- **`__clayered_search_near_int` ingest_cmp==0 / READ_UNCOMMITTED branch** — confirmed uncovered. Moderate.
- **`__clayered_modify_follower` positioned case** — confirmed uncovered. Moderate. (When cursor is already positioned, skip lookup and use current value.)

---

## conn\_layered.c

### 1. [Important] `__disagg_discard_old_checkpoint_check` — no-checkpoint early exit never taken

*(Verified: `__wt_ckpt_last_name` called 7,846 times; `WT_NOTFOUND` path never taken.)*

There is always at least one checkpoint in tests. The guard that allows discarding the
old-checkpoint check when no checkpoint exists is never exercised.

```c
    if (ret == WT_NOTFOUND) {
        WT_ASSERT(session, checkpoint_name_new == NULL);
        return (0); // ---> 7,846 checks — NEVER TAKEN
    }
```

---

### 2. [Low] FIXME-WT-16524 — optional `pl_abandon_checkpoint` NULL check

*(Verified: checked 7,140 times; `pl_abandon_checkpoint` always non-NULL.)*

The PALI always provides this operation in tests. The fallback warning path exists only for
implementations that don't implement `pl_abandon_checkpoint`. The comment says to remove
the check once WT-16524 is resolved.

```c
    if (disagg->npage_log->page_log->pl_abandon_checkpoint == NULL) {
        __wt_verbose_warning(..., "Abandon checkpoint operation is not supported");
        return (0); // ---> NEVER TAKEN
    }
```

---

### 3. [Low] `__shared_metadata_op_to_string` — entire function uncovered

Only called from `WT_VERBOSE_DEBUG2`-gated paths. Debug level never reached in tests.

---

## conn\_layered\_ingest.c

### 1. [Important] `__layered_drain_clear_work_queue` — non-empty queue cleanup

*(Confirmed from `analysis/some_testing_gaps.md`. Still uncovered.)*

Called on the error path of `__wti_layered_drain_ingest_tables`. No test causes a drain to
fail mid-queue, so queued work items are never cleaned up through this path.

```c
    if (!TAILQ_EMPTY(&conn->layered_drain_data.work_queue)) {
        TAILQ_FOREACH_SAFE(work_item, &conn->layered_drain_data.work_queue, q, work_item_tmp)
        { // ---> NOT COVERED
            TAILQ_REMOVE(&conn->layered_drain_data.work_queue, work_item, q);
            if (work_item->ingest_dhandle != NULL)
                WT_WITH_DHANDLE(session, work_item->ingest_dhandle,
                  __wt_cursor_dhandle_decr_use(session));
            __wt_free(session, work_item);
        }
    }
```

---

## conn\_layered\_page\_log.c

### 1. [Moderate] `__disagg_get_page` — retry loop never triggered

*(Verified: called 2,201 times; always finds the page immediately — `count == 1` on first try.)*

The retry loop with delay exists to handle transient page-materialization delays on the
follower. No test exercises a case where the page is not yet available on the first call.

```c
        if (retry > 0) {
            __wt_verbose_notice(session, WT_VERB_READ, "retry #%" PRIu32 " for page_id ...", retry, ...);
            __wt_sleep(0, WT_MIN(10000 + retry * 5000, 500000)); // ---> NOT COVERED
        }
```

---

### 2. [Low] `__wt_disagg_put_crypt_helper` — error branch

*(Verified: called 257 times; error branch `else` never taken.)*

The encryption helper is called during checkpoint metadata writes when a key provider is
configured. The error path (clean up key references on failure) is never triggered.

```c
    } else { // ---> NOT COVERED
        crypt.r.error = ret;
        crypt.keys.data = NULL;
        crypt.keys.size = 0;
    }
```

---

## block\_disagg\_read.c

### 1. [Critical — CORRECTNESS RISK] Materialization-frontier check never triggered

*(Verified: `__block_disagg_check_lsn_frontier` called 5,054,704 times;
`lsn > last_materialized_lsn` condition 0.)*

`__block_disagg_check_lsn_frontier` verifies that a follower does not read a page whose
LSN exceeds `last_materialized_lsn`. The check runs on every page read but the warning
condition is never true. We therefore have no evidence that the frontier is being updated
correctly — a bug in frontier tracking would be invisible in tests. A violated frontier
means the follower could serve stale data.

```c
    if (last_materialized_lsn != WT_DISAGG_LSN_NONE &&
      last_materialized_lsn != WT_DISAGG_START_LSN && lsn > last_materialized_lsn) {
        /* FIXME-WT-15818 Consider crashing upon this check failure. */
        WT_STAT_CONN_INCR(session, disagg_block_read_ahead_frontier);
        __wt_verbose_warning(..., "LSN frontier warning: ..."); // ---> NEVER TRIGGERED
    }
```

---

### 2. [Moderate] Checksum mismatch + corrupt / panic path never triggered

*(Verified: lines 226–302 all 0 — the entire data-integrity error path.)*

No test delivers a page with a wrong checksum or wrong magic number. The path that logs the
error, dumps the corrupt page, sets `WT_CONN_DATA_CORRUPTION`, and panics is entirely dead.
These are exactly the lines we rely on to detect page-level corruption in production.

```c
            /* checksum mismatch path: */
✗           if (!F_ISSET(session, WT_SESSION_QUIET_CORRUPT_FILE))
✗               __block_disagg_read_err(session, ..., "calculated checksum ... doesn't match ...");
            /* } else header checksum mismatch: */
✗           __block_disagg_read_err(session, ..., "header checksum ... doesn't match ...");

corrupt:
✗           F_SET_ATOMIC_32(S2C(session), WT_CONN_DATA_CORRUPTION);
✗           WT_ERR_PANIC(session, WT_ERROR, "%s: fatal read error ...", block_disagg->name);
```

---

### 3. [Low] Victim-cache (`WT_BLOCK_DISAGG_MODIFIED`) read path

*(Verified: line 217 — `from_cache = true` — 0 executions across 5.4M page reads.)*

When a page is served from the victim cache, the modified-page flag is set and the
size-assertion at the end of the read is skipped. No test exercises a victim-cache read hit.

---

### 4. [Low] `__wti_block_disagg_corrupt` and `__block_disagg_read_err` — 0 calls

Both functions are only reachable when a corruption is detected, which never happens in
tests. They are the corruption-reporting utilities that the paths in item 2 call. Their
0-call count is a direct consequence of gap 2.

---

## checkpoint/checkpoint\_parallel.c

### 1. [Moderate] Thread-create failure and txn-running panic paths uncovered

*(Verified: 9 connections use parallel checkpoints; all thread-group creates succeed; no
thread ever stops while holding a running transaction.)*

Two failure paths in `__wt_checkpoint_parallel_thread_create` and
`__checkpoint_parallel_thread_stop` are uncovered:

**Thread-create failure** (`err:` label at L323):
If `__wt_thread_group_create` fails, the partially initialized state must be torn down.
No test causes thread creation to fail.
```c
    if (0) {
err:                                                    // ---> NOT COVERED
        WT_TRET(__wt_checkpoint_parallel_thread_destroy(session));
    }
```

**Thread-stop with running transaction** (L262):
A checkpoint reconciliation thread should never stop while a transaction is in progress.
The `WT_RET_PANIC` guard is checked 34 times but the condition is never true.
```c
    if (F_ISSET(session->txn, WT_TXN_RUNNING))
        WT_RET_PANIC(session, ...,                      // ---> NOT COVERED
          "thread stopping while a transaction is running ...");
```

**Destroy-failure warning** (L365–368):
`__wt_checkpoint_parallel_finish` is called at destroy time (9 calls). If it fails, the
error is logged and cleared. The error path is never triggered.
```c
    if (ret != 0) {
        __wt_verbose_warning(session, WT_VERB_CHECKPOINT, "...", ...); // ---> NOT COVERED
        ret = 0;
    }
```

---

## history/hs\_cursor.c

### 1. [Important] `__wt_hs_btree_truncate` — HS cleanup on table drop always finds empty HS

*(Verified: called 3,348 times; first search always WT_NOTFOUND; truncation lines 301–328 all ✗.)*

Called during btree drop to wipe all HS entries for the dropped btree. In tests, the dropped
btrees never have HS entries, so the truncation path is never reached. In production, a table
dropped after heavy write activity (multiple versions evicted to HS) must correctly clean up
both the local and shared HS for that btree.

```c
3348    WT_ERR_NOTFOUND_OK(__wt_curhs_search_near_after(session, hs_cursor_start), true);
3348    if (ret == WT_NOTFOUND) {
3348        ret = 0;
3348        goto done;   // ---> ALWAYS TAKEN — actual truncation never runs
    }

    /* Open a history store stop cursor. */
✗       WT_ERR(__wt_curhs_open(session, btree_id, NULL, NULL, &hs_cursor_stop));
✗       WT_ERR(truncate_session->truncate(
          truncate_session, NULL, hs_cursor_start, hs_cursor_stop, NULL));
```

`__curhs_range_truncate` and `__wt_curhs_range_truncate` are 0-call as a direct consequence.

---

## history/hs\_verify.c

### 1. [Moderate] `__hs_verify` — full-HS consistency scan always finds empty HS

*(Verified: called 14 times for 2 HS IDs × 7 verify calls; first `next()` always WT_NOTFOUND.)*

`__wt_hs_verify` iterates both the local HS (`WT_HS_ID`) and shared HS (`WT_HS_ID_SHARED`).
For both, the opening `hs_cursor->next()` always returns `WT_NOTFOUND`, so the entire
verification loop — which checks that every HS entry has a matching key in the data store —
never runs.

The *per-btree* verify (`__wt_hs_verify_one`) IS tested with data: 95 btrees have HS entries
and `__hs_verify_id` iterates 87K HS entries. The gap is the *whole-database* sequential
scan, which is the path that would detect orphaned HS entries.

```c
14      WT_ERR_NOTFOUND_OK(hs_cursor->next(hs_cursor), true);
14      if (ret == WT_NOTFOUND) {
14          ret = 0;
14          goto err;          // ---> ALWAYS TAKEN for both HS IDs
    }

    /* Go through the history store and validate each btree. */
✗   while (ret == 0) {
✗       WT_ERR(hs_cursor->get_key(hs_cursor, &btree_id, ...));
✗       WT_ERR(__hs_verify_id(session, hs_cursor, ds_cursor, btree_id));
    }
```

---

## conn/conn\_page\_history.c

### 1. [Low] Entire `debug_mode.page_history` diagnostic never enabled

*(Verified: 9% line coverage; all tracking functions return at `!page_history->enabled`.)*

This disagg-specific diagnostic tracks per-page read/eviction patterns to help diagnose
materialization regressions. The feature is gated by `debug_mode.page_history`, never set
to true in any test.

```c
9426841     if (!page_history->enabled)
9427722         return (0);   // ---> ALWAYS TAKEN in __wt_conn_page_history_track_evict

5820144     if (!page_history->enabled)
5820606         return (0);   // ---> ALWAYS TAKEN in __wt_conn_page_history_track_read
```

The hash-map allocation, entry tracking, background reporter thread, and per-page statistics
output (92% of the file) are never executed.

---

## support/hash\_map.c

### 1. [Low] All functions uncovered — used only by the never-enabled page_history diagnostic

*(Verified: 3% coverage — only 4 lines covered, all variable declarations.)*

`hash_map.c` implements a simple hash map used exclusively by `conn_page_history.c` to
track per-page statistics. Because `page_history` is never enabled, no hash-map function
is ever called. Worth enabling in at least one test when `page_history` testing is added.

---

## Note on items from `analysis/some_testing_gaps.md`

The original analysis was correct. All 17 items remain uncovered. One additional note:

> **conn\_layered\_page\_log.c** — `WT_ASSERT(session, count <= 1)` in `__disagg_get_page`:
> Given that `count > 1` indicates corrupt page-log data, this should probably be
> `WT_ASSERT_ALWAYS` so it fires in release builds too.

---

## Covered items — confirmed not gaps

The following were discussed during coverage analysis and confirmed to be covered:

| Item | Status |
|---|---|
| All PALI operations (`plh_put`, `plh_get`, `plh_discard`, `plh_get_page_ids`, `pl_begin_checkpoint`, `pl_complete_checkpoint`, `pl_abandon_checkpoint`) | Covered |
| `__disagg_pick_up_checkpoint` | Covered (7,070 calls) |
| `__disagg_step_up` core path | Covered |
| `block_disagg_read.c` — cold-storage tier (`WT_PAGE_LOG_COLD`) | Covered (2 calls) |
| `cur_hs.c` — follower HS-open with checkpoint name (L51–58) | Covered (49,748 calls) |
| `cur_hs.c` — `__wt_curhs_next_hs_id` shared HS path | Covered (1,516,278 calls) |
| `bt_page.c` — all disagg blocks | Covered |
| `txn_timestamp.c` — all disagg blocks | Covered |
