## Coverage report: [WT-17223](https://jira.mongodb.org/browse/WT-17223)

Commit: [473b5815d95cc3a98a1851e4ec300731c0156c04](https://github.com/wiredtiger/wiredtiger/commit/473b5815d95cc3a98a1851e4ec300731c0156c04)

Run date: 2026-05-04. Overall: **75.4% lines**, **57.8% branches** (286 files).

Analysis covers only disagg-relevant components (per `analysis/disagg_components.md`).
Error-branch gaps are omitted unless they can cause data corruption or silent wrong-direction divergence.
Cross-referenced against `analysis/unsupported_disagg.md`.
Coverage verified line-by-line from `coverage_report/full_coverage_report.json`.

---

## Priority legend

| Priority | Meaning |
|---|---|
| **Critical** | Supported feature with a real correctness or silent-corruption risk |
| **Important** | Supported feature, gap is in a path exercised during normal operation |
| **Moderate** | Edge case, retry path, or less-frequent branch |
| **Low** | Feature currently unsupported in disagg (per `unsupported_disagg.md`), debug path, or dead code |

---

## Priority summary

**Critical**
- `cur_layered.c` — tombstone encode/decode path: value starts with tombstone byte is never tested (80M calls, condition always false → silent corruption risk)
- `block_disagg_read.c` — materialization-frontier check is non-fatal (FIXME-WT-15818: warns but never fails)

**Important**
- `cur_layered.c` — `__clayered_reopen_stable`: cursor position not preserved when stable cursor is reopened while positioned
- `cur_layered.c` — `__clayered_lookup`: key found in truncate list path never triggered (466K lookups, always misses)
- `cur_layered.c` — `__clayered_search_near_int`: stable cursor iteration path on truncated-key boundary never exercised
- `cur_layered.c` — `__clayered_largest_key`: both-cursor comparison never needed (stable always wins or ingest-only)
- `conn_layered.c` — `__disagg_discard_old_checkpoint_check`: no-checkpoint early exit never taken
- `conn_layered_ingest.c` — `__layered_drain_clear_work_queue`: non-empty queue cleanup never triggered

**Moderate**
- `cur_layered.c` — `__clayered_can_advance_stable`: final `return (false)` path not reached
- `cur_layered.c` — `__clayered_position_alternate`: `READ_UNCOMMITTED` compare call never reached
- `cur_layered.c` — `__clayered_search_near_move_ingest_to_opposite_side`: `READ_UNCOMMITTED` branch
- `cur_layered.c` — `__clayered_bound`: failure-path bounds cleanup
- `conn_layered_page_log.c` — `__disagg_get_page`: retry loop (page always found on first try)
- `block_disagg_read.c` — victim-cache (WT\_BLOCK\_DISAGG\_MODIFIED) read path

**Low** (unsupported feature, dead code, or trivial cleanup)
- `cur_layered.c` — `__clayered_adjust_state` step-down write rejection (elegant step-down not supported, PuP)
- `cur_layered.c` — `__clayered_reposition_truncate_iterate` body (fast truncate flag always false, PuP)
- `cur_layered.c` — `__clayered_truncate_follower` + helpers (follower truncation gated by fast-truncate flag, PuP)
- `cur_layered.c` — `__clayered_modify_leader`/`__clayered_modify_follower` delete-encoded branch (modify has known failing test, PuP)
- `conn_layered.c` — FIXME-WT-16524 `pl_abandon_checkpoint == NULL` check (code comment says remove it)
- `conn_layered.c` — `__disagg_begin_checkpoint` follower/no-npage_log early return
- `conn_layered.c` — `__shared_metadata_op_to_string` (only reachable from debug-level verbose logging)
- `conn_layered_ingest.c` — prepared-transaction drain rollback paths (prepared txn not supported, PuP)
- `conn_layered_page_log.c` — `__wt_disagg_put_crypt_helper` error branch
- `block_disagg_read.c` — cold-storage tier path

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
| `src/block_disagg/block_disagg_unsup.c` | 17% | 67 (all stubs) |
| `src/tiered/tiered_handle.c` | 40% | 335 |
| `src/tiered/tiered_work.c` | 11% | 173 |
| `src/tiered/tiered_config.c` | 54% | 59 |

---

## cur\_layered.c

### 1. [Critical — CORRUPTION RISK] `__clayered_deleted_encode` — tombstone-prefix encoding never triggered

*(Confirmed from `analysis/some_testing_gaps.md`. Verified: 80,867,203 calls, true-branch always 0.)*

`__clayered_is_deleted_encoded` is checked on every write, but the condition is never true in any test.
A value whose first byte matches the tombstone marker must be re-encoded by appending an extra byte
so it isn't silently treated as a deletion. If this path is never exercised, that invariant is unverified.

```c
    if (__clayered_is_deleted_encoded(value)) { // ---> CHECKED 80M TIMES, ALWAYS FALSE
        WT_RET(__wt_scr_alloc(session, value->size + 1, tmpp));
        tmp = *tmpp;
        memcpy(tmp->mem, value->data, value->size);
        memcpy((uint8_t *)tmp->mem + value->size, __wt_tombstone.data, 1);
        final_value->data = tmp->mem;
        final_value->size = value->size + 1;
    }
```

The matching decode:

```c
static WT_INLINE void
__clayered_deleted_decode(WT_ITEM *value)
{
    if (__clayered_is_deleted_encoded(value))
        --value->size; // ---> NEVER REACHED
}
```

---

### 2. [Important] `__clayered_reopen_stable` — cursor position not preserved across stable reopen

*(Confirmed from `analysis/some_testing_gaps.md`. Verified: `__clayered_reopen_stable` is called 16 times but neither branch is taken.)*

When a follower advances to a new checkpoint while a cursor has an external key set, the key
should be copied to the new stable cursor. When the layered cursor is currently positioned
on the old stable cursor, `current_cursor` must be updated to the new one. Neither is tested.

```c
    } else if (F_ISSET(old_stable, WT_CURSTD_KEY_EXT)) { // ---> NOT COVERED (16 calls, always skip)
        WT_ITEM_SET(clayered->stable_cursor->key, old_stable->key);
        if (F_ISSET(old_stable, WT_CURSTD_VALUE_EXT))
            WT_ITEM_SET(clayered->stable_cursor->value, old_stable->value);
    }

    if (clayered->current_cursor == old_stable) { // ---> NOT COVERED
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

*(Confirmed from `analysis/some_testing_gaps.md`. Verified: checked 466,437 times, `ret == 0` never true.)*

`__wt_truncate_delete_visible_check` is called 466K times but never returns 0 (key found in a
truncated range). This means we never test the path where a lookup correctly hides a key
because it falls in a pending truncation.

```c
        if (!found) {
            WT_ERR_NOTFOUND_OK(__wt_truncate_delete_visible_check(session,
                                 (WT_LAYERED_TABLE *)clayered->dhandle, &cursor->key, NULL),
              true);
            if (ret == 0) { // ---> CHECKED 466K TIMES, NEVER TRUE
                found = true;
                ret = WT_NOTFOUND;
            }
        }
```

---

### 4. [Important] `__clayered_search_near_int` — stable cursor iteration on truncated-key boundary

*(Confirmed from `analysis/some_testing_gaps.md`. Verified: block entered 2 times, all iteration lines 0.)*

When the stable cursor lands on a key inside a truncated range, `__clayered_constituent_iter_helper`
should step it forward (or backward if forward fails) to find the next valid key. The block is
entered 2 times but the `if (ret == 0)` branches within are never reached.

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

*(Confirmed from `analysis/some_testing_gaps.md`. Verified: function called 12 times, else branch 0.)*

The function is called 12 times: once ingest-only, 11 times stable-only. Never with both. 
The comparison path that picks the larger key is never reached.

```c
    } else { // ---> NOT COVERED — both ingest and stable found
        __clayered_get_collator(clayered, &collator);
        if (stable_cursor == NULL)
            larger_cursor = ingest_cursor;
        else {
            WT_ERR(__wt_compare(session, collator,
              &ingest_cursor->key, &stable_cursor->key, &cmp));
            if (cmp <= 0)
                larger_cursor = stable_cursor;
            else
                larger_cursor = ingest_cursor;
        }
    }
```

---

### 6. [Moderate] `__clayered_can_advance_stable` — final `return (false)` path

*(Confirmed from `analysis/some_testing_gaps.md`. Verified: the final return is unreachable in tests.)*

The function is called and the early `return (true)` branch is taken every time. The fall-through
path that returns `false` (cursor has no snapshot, not an iteration, no read timestamp set)
is never exercised.

```c
    return (false); // ---> NOT COVERED — default fall-through path
```

---

### 7. [Moderate] `__clayered_position_alternate` — `READ_UNCOMMITTED` comparison

*(Confirmed from `analysis/some_testing_gaps.md`. Verified: function called 1608 times; line 978 returns 0 all 123 times.)*

Higher-isolation-level callers always return at line 979. The `READ_UNCOMMITTED` path
that re-compares after stepping is never reached.

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

### 10. [Low] `__clayered_adjust_state` — step-down write rejection

Elegant step-down not supported until Public Preview (`unsupported_disagg.md`).

```c
    if (!current_leader && session->txn->mod_count != 0) {
        __wt_txn_err_set(session, WT_ROLLBACK);
        WT_RET(WT_ROLLBACK); // ---> NOT COVERED (elegant step-down PuP)
    }
```

---

### 11. [Low] `__clayered_reposition_truncate_iterate` — fast-truncate feature flag always false

Fast truncate not supported until Public Preview (`unsupported_disagg.md`).
The `disagg_fast_truncate_2026` global is always `false`, so the entire function body is dead.

---

### 12. [Low] Follower truncation path: `__clayered_truncate_follower`, `__clayered_range_truncate_ingest`, `__clayered_position_near_key`

These three functions are entirely uncovered (0 calls). `__wt_layered_truncate` dispatches to
the follower path only when `disagg_fast_truncate_2026 == true` (asserted at line 944), so
follower-mode truncation is effectively gated by the fast-truncate feature flag.

---

### 13. [Low] `__clayered_modify_leader` / `__clayered_modify_follower` — delete-encoded branch

Modify ops have a known failing test and are not fully supported until Public Preview
(`unsupported_disagg.md`: "Modify::ops: Maybe — WT-14467").

---

### 14. Items confirmed from `analysis/some_testing_gaps.md`

All 17 original items remain uncovered. Specific notes on reassessment:

- **`__wt_clayered_open` err label** — confirmed uncovered. Low priority (error path).
- **`__clayered_open_stable_follower` EBUSY retry** — confirmed uncovered. Moderate.
- **`__clayered_search_near_int` ingest_cmp==0 branch** — confirmed uncovered. Moderate.
- **`__clayered_modify_follower` positioned case** — confirmed uncovered. Low (modify PuP).

---

## conn\_layered.c

### 1. [Important] `__disagg_discard_old_checkpoint_check` — no-checkpoint early exit never taken

*(Confirmed from `analysis/some_testing_gaps.md`. Verified: condition checked 7846 times, return never taken.)*

```c
    if (ret == WT_NOTFOUND) {
        WT_ASSERT(session, checkpoint_name_new == NULL);
        return (0); // ---> CHECKED 7846 TIMES, NEVER TAKEN
    }
```

---

### 2. [Low] FIXME-WT-16524 — optional `pl_abandon_checkpoint` NULL check

*(Confirmed from `analysis/some_testing_gaps.md`. Verified: checked 7140 times, never NULL.)*

The FIXME comment says this check should be removed. Low priority until WT-16524 is resolved.

```c
    if (disagg->npage_log->page_log->pl_abandon_checkpoint == NULL) {
        __wt_verbose_warning(..., "Abandon checkpoint operation is not supported");
        return (0); // ---> NEVER TAKEN
    }
```

---

### 3. [Low] `__disagg_begin_checkpoint` — follower/no page-log early return

*(Confirmed from `analysis/some_testing_gaps.md`. Verified: condition checked 21507 times, return never taken.)*

In tests `__disagg_begin_checkpoint` is only called on the leader with a page log configured.
The follower path is never tested.

```c
    if (disagg->npage_log == NULL || !conn->layered_table_manager.leader)
        return (0); // ---> CHECKED 21507 TIMES, NEVER TAKEN
```

---

### 4. [Low] `__shared_metadata_op_to_string` — entire function uncovered

Only called from `WT_VERBOSE_DEBUG2`-gated paths that are never reached in tests.

---

## conn\_layered\_ingest.c

### 1. [Important] `__layered_drain_clear_work_queue` — non-empty queue cleanup

*(Confirmed from `analysis/some_testing_gaps.md`. Still uncovered.)*

Called on the error path of `__wti_layered_drain_ingest_tables`. No test causes a drain to fail
mid-queue, so queued work items are never cleaned up by this path.

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

### 2. [Low] Prepared-transaction drain — `is_prepare_rollback` paths

*(Confirmed from `analysis/some_testing_gaps.md`. Still uncovered.)*

Prepared transactions in disagg are not supported until Public Preview
(`unsupported_disagg.md`: "Prepared Txn: No — Public Preview").

---

## conn\_layered\_page\_log.c

### 1. [Moderate] `__disagg_get_page` — retry loop (page always materialized on first try)

`__disagg_get_page` is called 2201 times and always finds the page immediately (`count == 1`).
The retry loop (delayed materialization) and retry-exhaustion error are never triggered.

```c
        /* Otherwise retry up to 100 times to account for page materialization delay. */
        if (retry > 100) {
            __wt_verbose_error(..., "read failed for page ID ...");
            return (EIO);  // ---> NOT COVERED
        }
        __wt_sleep(0, 10000 + retry * 5000); // ---> NOT COVERED
        ++retry;
```

---

### 2. [Low] `__wt_disagg_put_crypt_helper` — error branch

*(Confirmed from `analysis/some_testing_gaps.md`. Still uncovered.)*

```c
    } else { // ---> NOT COVERED
        crypt.r.error = ret;
        crypt.keys.data = NULL;
        crypt.keys.size = 0;
    }
```

---

## block\_disagg\_read.c

### 1. [Critical — CORRECTNESS RISK] Materialization-frontier check is non-fatal

`__block_disagg_check_lsn_frontier` warns when a follower reads a page whose LSN exceeds
`last_materialized_lsn`. The code comment says *"FIXME-WT-15818: Consider crashing upon this
check failure."* The check is never triggered in tests, so we don't know whether the frontier
is being tracked correctly. A silent violation means the follower could serve stale data.

```c
    if (last_materialized_lsn != WT_DISAGG_LSN_NONE &&
      last_materialized_lsn != WT_DISAGG_START_LSN && lsn > last_materialized_lsn) {
        /* FIXME-WT-15818 Consider crashing upon this check failure. */
        WT_STAT_CONN_INCR(session, disagg_block_read_ahead_frontier);
        __wt_verbose_warning(...); // ---> NEVER TRIGGERED IN ANY TEST
    }
```

---

### 2. [Moderate] Victim cache / modified-flag path

When a page is served from the victim cache (`WT_BLOCK_DISAGG_MODIFIED` flag), certain size
assertions are skipped. No test exercises a cache-hit read through the disagg block manager.

---

### 3. [Low] Cold-storage tier path

```c
    if (S2BT(session)->storage_tier == WT_BTREE_STORAGE_TIER_COLD)
        F_SET(&get_args, WT_PAGE_LOG_COLD); // ---> NOT COVERED
```

---

## conn\_layered\_table\_manager.c (minor items only)

The table manager is properly covered at 57%. The only uncovered regions are:
- The `err:` path in `__wti_layered_table_manager_init` (init failure, unlikely in practice)
- The duplicate-table-open panic in `__wt_layered_table_manager_add_table` (defensive check)

These are both error/defensive paths with no functional scenario they protect against in current tests.

---

## Note on items from `analysis/some_testing_gaps.md`

The original analysis was correct. All 17 items remain uncovered. One additional note:

> **conn\_layered\_page\_log.c** — `WT_ASSERT(session, count <= 1)` in `__disagg_get_page`:
> Given that `count > 1` indicates corrupt page-log data, this should probably be
> `WT_ASSERT_ALWAYS` so it fires in release builds too.
