## Coverage gaps for unsupported or not-yet-supported disagg features

Commit: [473b5815d95cc3a98a1851e4ec300731c0156c04](https://github.com/wiredtiger/wiredtiger/commit/473b5815d95cc3a98a1851e4ec300731c0156c04)

Run date: 2026-05-04.

These are coverage gaps in code paths that are **currently unsupported** in disaggregated storage,
per `analysis/unsupported_disagg.md`. They are tracked separately so that the main gaps document
(`disagg_coverage_gaps.md`) only reflects supported-feature gaps.

Items are grouped by the unsupported feature. The milestone column indicates when support is
planned, using the same terminology as `unsupported_disagg.md`.

---

## Fast Truncate

**Milestone: Public Preview** (`disagg_fast_truncate_2026` flag, always `false` today)

### `cur_layered.c`

`__wt_layered_truncate` dispatches to the follower truncation path only when
`__wt_process.disagg_fast_truncate_2026 == true` (asserted at entry). The following
functions are entirely uncovered (0 calls):

| Function | Purpose |
|---|---|
| `__clayered_truncate_follower` | Entry point for follower-side range truncation |
| `__clayered_range_truncate_ingest` | Truncates the ingest constituent |
| `__clayered_position_near_key` | Positions cursor on or near a key for truncation |

`__clayered_reposition_truncate_iterate` has a guarded entry:
```c
if (!__wt_process.disagg_fast_truncate_2026)
    return (0); // ---> ALWAYS TAKEN — body never exercised
```

### `txn/txn_truncate.c` (21% coverage overall)

The file contains both a shared utility (`__key_within_truncate_range`, 44 calls — covered)
and the full fast-truncate machinery (all uncovered). Coverage by function:

| Function | Calls | Note |
|---|---|---|
| `__key_within_truncate_range` | 44 | Covered |
| `__disagg_truncate_free` | 0 | Memory release for truncate entry |
| `__txn_insert_truncate_entry_helper` | 0 | Internal insert helper |
| `__wt_insert_truncate_entry` | 0 | Public API: insert a truncate range |
| `__truncate_search` | 0 | Search the truncate list |
| `__wt_layered_table_truncate_detect_write_conflict` | outer: 586,953; loop body: 0 | Early return because flag is false |
| `__wt_truncate_delete_visible_check` | outer: 1,925,492; body: 0 | Early return because flag is false |
| `__wti_mark_committed_truncate_table` | 0 | Mark a truncate as committed |
| `__wti_layered_table_truncate_rollback` | 0 | Rollback a truncate op |

`__wt_layered_table_truncate_detect_write_conflict` and `__wt_truncate_delete_visible_check`
are called millions of times but return immediately:
```c
// In each function:
if (!__wt_process.disagg_fast_truncate_2026)
    return (0); // ---> ALWAYS TAKEN
```

### `txn/txn.c`

The `WT_TXN_OP_FOLLOWER_TRUNCATE` case in the transaction rollback switch is never reached:
```c
case WT_TXN_OP_FOLLOWER_TRUNCATE: // ---> NOT COVERED (fast truncate PuP)
    __wti_layered_table_truncate_rollback(session, op);
    break;
```

---

## Prepared Transactions

**Milestone: Public Preview** (`WT_CONN_PRESERVE_PREPARED` flag)

Prepared transactions in disagg have new semantics: a prepared update is guaranteed to be
included in a checkpoint if it adheres to timestamp rules. The entire
`prepared_discover/` subsystem, and several other paths, are gated by this feature.

### `prepared_discover/prepared_discover_txn.c` (0% coverage — 132 instrumented lines)

This file discovers and allocates prepared-transaction state during a follower step-up.
It is entirely uncovered.

| Function | Purpose |
|---|---|
| `__wt_prepared_discover_find_item` | Look up a prepared txn in the discovery list |
| `__prepare_discover_alloc_upd` | Allocate an update record for a discovered prepared txn |
| `__wt_prepared_discover_add_entry` | Add an entry to the discovery list |
| `__wt_prepared_discover_free` | Free the discovery list |

### `prepared_discover/prepared_discover_walk.c` (0% coverage — 291 instrumented lines)

Walks all btrees on a follower to find in-flight prepared transactions at step-up time.

| Function | Purpose |
|---|---|
| `__prepared_discover_btree_has_prepare` | Check whether a btree has any prepared updates |
| `__prepared_discover_is_follower_stable_walk` | Walk the stable btree to find prepared updates |
| `__wt_prepared_discover_step_up` | Main entry point for prepared-txn discovery at step-up |

### `cursor/cur_hs.c` — `__curhs_update` (0 calls)

The `update` method on the HS cursor is used exclusively to retrofit a stop timestamp onto
an existing HS entry after a prepared transaction commits. Never called because prepared
transactions in disagg are not yet supported.

```c
static int
✗   __curhs_update(WT_CURSOR *cursor)
{
    // Only valid use: set the stop timestamp on an existing HS entry.
    WT_ASSERT(session, WT_TIME_WINDOW_HAS_STOP(&hs_cursor->time_window));
    // ...
}
```

### `reconcile/rec_hs.c` — `WT_CONN_PRESERVE_PREPARED` path (0.004% of iterations)

`check_prepared = true` in only 410 of 9,096,136 reconcile iterations. Of those, 306 are
aborted (txnid == WT_TXN_ABORTED) and only 104 reach the actual preserve logic.
The `squashed = true` path is taken 40 times total.

### `conn_layered_ingest.c` — `is_prepare_rollback` paths

`__layered_fix_prepared_transaction` contains two blocks gated by `is_prepare_rollback == true`:
one to resolve a rolled-back prepared update, and one to preserve it on the update chain.
Neither is ever reached in tests.

---

## Elegant Step-Down

**Milestone: Public Preview**

### `cur_layered.c` — `__clayered_adjust_state`

When a node steps down from leader to follower, any in-flight write operations must be
rejected with `WT_ROLLBACK`. The guard is checked on every write but the rejection path
is never exercised because step-down in tests is done by restarting the node.

```c
    if (!current_leader && session->txn->mod_count != 0) {
        __wt_txn_err_set(session, WT_ROLLBACK);
        WT_RET(WT_ROLLBACK); // ---> NOT COVERED (step-down PuP)
    }
```

---

## Modify::ops

**Milestone: Public Preview** (WT-14467: known failing test)

### `cur_layered.c` — delete-encoded branches in `__clayered_modify_leader` / `__clayered_modify_follower`

When the existing value starts with a tombstone byte (delete-encoded), a modify must first
decode it, apply the modification, and re-encode. This path exists in both the leader and
follower modify implementations and is never triggered.

```c
// __clayered_modify_leader (L2636+):
    if (ret == 0 && __clayered_is_deleted_encoded(&stable->value)) { // ---> 525,883 checks, always false
        __clayered_deleted_decode(&stable->value);
        WT_ERR(__wt_modify_apply_api(stable, entries, nentries));
        WT_ERR(__clayered_deleted_encode(session, &stable->value, &stable->value, &buf));
    }

// __clayered_modify_follower (L2694+):
    if (... || __clayered_is_deleted_encoded(&ingest->value)) { // ---> 1,000 checks, always false
        // ... similar encode/apply/decode sequence ...
    }
```

---

## Compact / Salvage

**Milestone: Never** (not applicable for disagg storage)

### `block_disagg/block_disagg_unsup.c` (17% coverage — all stubs)

All covered lines are the function signatures. Every function body returns `ENOTSUP`
immediately and is never reached because compact and salvage are not supported in disagg.

| Function | Reason not supported |
|---|---|
| `__wti_block_disagg_compact_skip` | Compaction acts on local files — not applicable in disagg |
| `__wti_block_disagg_compact_page_rewrite` | Same reason |
| `__wti_block_disagg_salvage_start` | Salvage reconstructs local btrees — different approach needed for disagg |
| `__wti_block_disagg_salvage_next` | Same reason |
| `__wti_block_disagg_salvage_end` | Same reason |
| `__wti_block_disagg_salvage_valid` | Same reason |
