## Ticket to measure code coverage: [WT-17223](https://jira.mongodb.org/browse/WT-17223) (Coverage report is attached there)

Commit: [473b5815d95cc3a98a1851e4ec300731c0156c04](https://github.com/wiredtiger/wiredtiger/commit/473b5815d95cc3a98a1851e4ec300731c0156c04)

## Summary

1. The most untested area for foundations related files that I’ve found is `cur_layered.c` \- it really needs some improvement (14 uncovered cases including the entire functions)  
2. In many many places we don’t test returning error messages \- probably assuming that they’d lead to a server crash and so it’s not very important to test it. However, in some cases the server could try to do something else when getting an error code, so it’s definitely worth revisiting.  
3. All the other files mentioned in the doc have some places worth checking \- but not many of them.  
4. I checked `conn_layered.c`, `conn_layered_ingest.c`, `conn_layered_page_log.c` , `cur_layered.c` and all the different `schema_` files.

## conn\_layered.c

1. Testing no checkpoint in `__disagg_discard_old_checkpoint_check`


```c

    WT_ERR_NOTFOUND_OK(__wt_ckpt_last_name(session, cfg_current, checkpoint_name, &checkpoint_order,
                         &checkpoint_time),
      true);
    /* Early exit if we can't find the configuration of last checkpoint. */
    if (ret == WT_NOTFOUND) {
        WT_ASSERT(session, *checkpoint_name == NULL);
        return (0); // ----> NOT COVERED AT ALL
    }
```

2. FIXME-WT-16524 \- we should probably remove the check

```c
   /*
     * FIXME-WT-16524: This function is no longer an optional operation for testing, remove this
     * check.
     */
    if (disagg->npage_log->page_log->pl_abandon_checkpoint == NULL) {
        __wt_verbose_warning(session, WT_VERB_DISAGGREGATED_STORAGE, "%s",
          "Abandon checkpoint operation is not supported by the current PALI implementation");
        return (0);
    }
```

3. Why do we return 0 if we call `__disagg_begin_checkpoint` on a follower:

```c

    /* Only the leader can begin a global checkpoint. */
    if (disagg->npage_log == NULL || !conn->layered_table_manager.leader)
        return (0);

```

## conn\_layered\_ingest.c

1. Is not tested when the queue is not empty

```c
static void
__layered_drain_clear_work_queue(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn = S2C(session);
    __wt_spin_lock(session, &conn->layered_drain_data.queue_lock);
    if (!TAILQ_EMPTY(&conn->layered_drain_data.work_queue)) {
        WT_LAYERED_DRAIN_ENTRY *work_item = NULL, *work_item_tmp = NULL;
        TAILQ_FOREACH_SAFE(work_item, &conn->layered_drain_data.work_queue, q, work_item_tmp)
        { // ---> NOT COVERED AT ALL
            TAILQ_REMOVE(&conn->layered_drain_data.work_queue, work_item, q);
            if (work_item->ingest_dhandle != NULL)
                WT_WITH_DHANDLE(
                  session, work_item->ingest_dhandle, __wt_cursor_dhandle_decr_use(session));
            __wt_free(session, work_item);
        }
    }
    WT_ASSERT_ALWAYS(session, TAILQ_EMPTY(&conn->layered_drain_data.work_queue),
      "Layered drain work queue failed to drain");
    __wt_spin_unlock(session, &conn->layered_drain_data.queue_lock);
    __wt_spin_destroy(session, &conn->layered_drain_data.queue_lock);
}

```

2. `__layered_fix_prepared_transaction` doesn’t check for prepared transactions

```c
else if (!prepare_resolved) {
                    /* Only resolve the updates from the same prepared transaction once. */
                    if (is_prepare_rollback) { // ---> NOT COVERED AT ALL
                        /*
                         * The original transaction id is stored in start timestamp and the rollback
                         * timestamp is stored in durable timestamp.
                         */
                        WT_TXN_TIME_POINT txn_time_point;
                        txn_time_point.id = start_ts;
                        txn_time_point.prepared_id = start_prepared_id;
                        txn_time_point.prepare_timestamp = start_prepare_ts;
                        txn_time_point.rollback_timestamp = durable_start_ts;
                        WT_ERR(__wt_txn_resolve_prepared_op(session, stable_btree, &txn_time_point,
                          key, WT_RECNO_OOB, false, &prepare_cursor));
                    } else {
                        WT_TXN_TIME_POINT txn_time_point;
                        txn_time_point.id = start_txn;
                        txn_time_point.prepared_id = start_prepared_id;
                        txn_time_point.prepare_timestamp = start_prepare_ts;
                        txn_time_point.commit_timestamp = start_ts;
                        txn_time_point.durable_timestamp = durable_start_ts;
                        WT_ERR(__wt_txn_resolve_prepared_op(session, stable_btree, &txn_time_point,
                          key, WT_RECNO_OOB, true, &prepare_cursor));
                    }
                    prepare_resolved = true;
                }
```

```c
              if (is_prepare_rollback) { // ---> NOT COVERED AT ALL
                    /* Prepared transactions must have a prepared id in disagg. */
                    WT_ASSERT(session,
                      !prepare && preserve_prepared && start_prepared_id != WT_PREPARED_ID_NONE);
                    /*
                     * The original transaction id is stored in start timestamp and the rollback
                     * timestamp is stored in durable timestamp.
                     */
                    upd->txnid = WT_TXN_ABORTED;
                    upd->prepare_state = WT_PREPARE_INPROGRESS;
                    upd->prepare_ts = start_prepare_ts;
                    upd->prepared_id = start_prepared_id;
                    upd->upd_saved_txnid = start_ts;
                    upd->upd_rollback_ts = durable_start_ts;
```

## conn\_layered\_page\_log.c

1. Should we make this assert ASSERT\_ALWAYS in `__disagg_get_page` ?

```c
WT_ASSERT(session, count <= 1); /* Corrupt data. */
```

2. We don’t test a failure case in `__wt_disagg_put_crypt_helper` :

```c
   } else { // ---> NOT COVERED AT ALL
        crypt.r.error = ret;
        /* On error, remove references of crypt key before calling back. */
        crypt.keys.data = NULL;
        crypt.keys.size = 0;
    }
```

## cur\_layered.c

1. `__clayered_deleted_encode` doesn’t test “is deleted” branch

```c
static WT_INLINE int
__clayered_deleted_encode(
  WT_SESSION_IMPL *session, const WT_ITEM *value, WT_ITEM *final_value, WT_ITEM **tmpp)
{
    WT_ITEM *tmp;

    /*
     * If value requires encoding, get a scratch buffer of the right size and create a copy of the
     * data with the first byte of the tombstone appended.
     */
    if (__clayered_is_deleted_encoded(value)) { // ---> NOT COVERED AT ALL
        WT_RET(__wt_scr_alloc(session, value->size + 1, tmpp));
        tmp = *tmpp;

        memcpy(tmp->mem, value->data, value->size);
        memcpy((uint8_t *)tmp->mem + value->size, __wt_tombstone.data, 1);
        final_value->data = tmp->mem;
        final_value->size = value->size + 1;
    } else {
        final_value->data = value->data;
        final_value->size = value->size;
    }

    return (0);
}
```

And `deleted_decode` too:

```c
static WT_INLINE void
71		88180965	__clayered_deleted_decode(WT_ITEM *value)
72			{
88180965	    if (__clayered_is_deleted_encoded(value))
74		✗	        --value->size;
75		88352016	}
```

2. For `__clayered_open_stable_follower` we don’t test for EBUSY

```c
   ret = __clayered_open_stable_int(clayered, last_ckpt_uri->data);
    if (ret == EBUSY) { // ---> NOT COVERED AT ALL
        /* Retry to ensure we open the same checkpoint for the HS and the stable table. */
        __wt_free(session, checkpoint_name);
        goto retry;
    }
```

3. `__clayered_can_advance_stable` doesn’t have coverage for the default return and the iteration branch:

```c
   txn_shared = WT_SESSION_TXN_SHARED(session);
    if (txn_shared != NULL && txn_shared->read_timestamp != WT_TS_NONE)
        return (true);
    else {
        /* if this is an iteration, we won't reopen the cursor, we're done. */
        if (iteration)
            return (false); // ---> NOT COVERED AT ALL
	 // ...
    }

    return (false); // ---> NOT COVERED AT ALL
```

4. `__clayered_reopen_stable` still doesn’t cover `F_ISSET(old_stable, WT_CURSTD_KEY_EXT)` and `clayered->current_cursor == old_stable` :

```c
   } else if (F_ISSET(old_stable, WT_CURSTD_KEY_EXT)) { // ---> NOT COVERED AT ALL
        WT_ITEM_SET(clayered->stable_cursor->key, old_stable->key);
        if (F_ISSET(old_stable, WT_CURSTD_VALUE_EXT))
            WT_ITEM_SET(clayered->stable_cursor->value, old_stable->value);
    }

    /* Add any bounds for the new cursor. */
    WT_ERR(__clayered_copy_bounds(clayered));

    if (clayered->current_cursor == old_stable) { // ---> NOT COVERED AT ALL
        WT_CURSOR *cursor = (WT_CURSOR *)clayered;
        WT_CURSOR *new_stable = clayered->stable_cursor;
        if (F_ISSET(cursor, WT_CURSTD_KEY_INT)) {
            /* Reset the cursor key to point to the new stable cursor. */
            WT_ITEM_SET(cursor->key, new_stable->key);
            /* Clear the value as the new stable cursor may point to a different one. */
            F_CLR(cursor, WT_CURSTD_VALUE_INT);
        }
        clayered->current_cursor = new_stable;
    }
```

5. That’s kind of expected, but we don’t have step down testing for `__clayered_adjust_state` :

```c
       if (!current_leader && session->txn->mod_count != 0) {
            __wt_txn_err_set(session, WT_ROLLBACK);
            /* Write operations are not allowed after stepping down from leader role. */
            WT_RET(WT_ROLLBACK); // ---> NOT COVERED AT ALL
        }
```

6. `__clayered_reposition_truncate_iterate` has very poor coverage:

```c

    if (!__wt_process.disagg_fast_truncate_2026)
        return (0); // ---> WE ALWAYS RETURN HERE NOT TESTING ANYTHING ELSE
```

7. 0 test coverage for  
   1. \_\_clayered\_position\_near\_key   
   2. \_\_clayered\_range\_truncate\_ingest  
   3. \_\_clayered\_truncate\_follower  
8. `__clayered_position_alternate` never reaches the point of calling `__clayered_cursor_compare`   
9. `__clayered_bound` never tests the failure case  
10. `__clayered_lookup` doesn’t have the `__wt_truncate_delete_visible_check` returns true coverage

```c
       /* Only consult the truncate list when ingest has no entry for this key. */
        if (!found) {
            WT_ERR_NOTFOUND_OK(__wt_truncate_delete_visible_check(session,
                                 (WT_LAYERED_TABLE *)clayered->dhandle, &cursor->key, NULL),
              true);
            if (ret == 0) { // ---> NOT COVERED AT ALL
                found = true;
                ret = WT_NOTFOUND;
            }
        }
```

11. `__clayered_search_near_move_ingest_to_opposite_side` has no coverage for `WT_ISO_READ_UNCOMMITTED`:

```c
           if (ret == 0) // ---> NOT COVERED AT ALL
                WT_ERR( 
                  __wt_compare(session, collator, &ingest_cursor->key, &cursor->key, ingest_cmp));
```

12. `__clayered_search_near_int` testing doesn’t cover the following case

```c
// ---> NOT COVERED AT ALL           
WT_ERR_NOTFOUND_OK(
              __clayered_constituent_iter_helper(clayered, clayered->stable_cursor, true), true);
            if (ret == 0)
                stable_cmp = 1;
            else {
                WT_ERR_NOTFOUND_OK(
                  __clayered_constituent_iter_helper(clayered, clayered->stable_cursor, false),
                  true);
                if (ret == 0)
                    stable_cmp = -1;
            }
```

 

```c

            if (closest == NULL) {
                if (ingest_cmp == 0) { // ---> NOT COVERED AT ALL
                    WT_ASSERT(session, session->txn->isolation == WT_ISO_READ_UNCOMMITTED);
                    closest = clayered->ingest_cursor;
```

13. `__clayered_largest_key` doesn’t cover the case when both ingest and stable exist:

```c
   if (ingest_found && !stable_found)
        larger_cursor = ingest_cursor;
    else if (!ingest_found && stable_found) {
        larger_cursor = stable_cursor;
    } else { // ---> NOT COVERED AT ALL
        __clayered_get_collator(clayered, &collator);
        if (stable_cursor == NULL)
            larger_cursor = ingest_cursor;
        else {
            WT_ERR(__wt_compare(session, collator, &ingest_cursor->key, &stable_cursor->key, &cmp));
            if (cmp <= 0)
                larger_cursor = stable_cursor;
            else
                larger_cursor = ingest_cursor;
        }
    }
```

14. `__clayered_modify_leader` doesn’t cover the “deleted encoded” branch:

```c
   /*
     * Similarly, a delete-encoded value alters the original value and also cannot serve as the base
     * value for a modify. In these cases, perform a full update instead.
     */
    if (ret == 0 && __clayered_is_deleted_encoded(&stable->value)) {
        __clayered_deleted_decode(&stable->value);
        WT_ERR(__wt_modify_apply_api(stable, entries, nentries));
        WT_ERR(__clayered_deleted_encode(session, &stable->value, &stable->value, &buf));
        F_SET(stable, WT_CURSTD_VALUE_EXT);
        WT_ERR(stable->update(stable));
    } else
```

15. `__clayered_modify_follower` doesn’t cover the positioned case.

```c
   /* Do a search if we're not positioned. */
    if (!F_ISSET(&clayered->iface, WT_CURSTD_KEY_INT))
        WT_ERR_NOTFOUND_OK(__clayered_lookup(session, clayered, &value), true);
    else // ---> NOT COVERED AT ALL
        WT_ITEM_SET(value, cursor->value);
```

16. `__clayered_modify_follower` doesn’t cover the “deleted encoded” branch:

```c
        */
        if (ret == WT_NOTFOUND || __wt_clayered_deleted(&ingest->value) ||
          __clayered_is_deleted_encoded(&ingest->value)) {
            __clayered_deleted_decode(&ingest->value);
            WT_ERR(__wt_modify_apply_api(ingest, entries, nentries));
            WT_ERR(__clayered_deleted_encode(session, &ingest->value, &ingest->value, &buf));
            F_SET(ingest, WT_CURSTD_VALUE_EXT);
            WT_ERR(ingest->update(ingest));
        } else
```

17. `__wt_clayered_open` doesn’t check for the expectation to release the expectation to release a dhandle if we fail

```c

    if (0) {
err:
        /* Our caller expects to release the data handles if we fail. */
        clayered->dhandle = NULL;
        __wt_cursor_dhandle_decr_use(session);
        if (clayered != NULL)
            WT_TRET(__clayered_close(cursor));
        WT_TRET(__wt_session_release_dhandle(session));

        *cursorp = NULL;
    }
```

