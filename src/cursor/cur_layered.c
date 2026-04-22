/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

static int __clayered_copy_bounds(WT_CURSOR_LAYERED *);
static int __clayered_lookup(WT_SESSION_IMPL *, WT_CURSOR_LAYERED *, WT_ITEM *);
static int __clayered_open_cursors(WT_SESSION_IMPL *, WT_CURSOR_LAYERED *, bool);
static int __clayered_reset_cursors(WT_CURSOR_LAYERED *, bool);
static int __clayered_search_near(WT_CURSOR *, int *);
static int __clayered_adjust_state(WT_CURSOR_LAYERED *, bool, bool *);

/*
 * __clayered_deleted --
 *     Check whether the current value is a tombstone in the layered cursor.
 */
static WT_INLINE bool
__clayered_deleted(WT_CURSOR_LAYERED *clayered, const WT_ITEM *item)
{
    /*
     * We only use tombstone value for ingest table. Therefore, if we don't have an ingest table,
     * the returned value must be a proper value.
     */
    if (clayered->ingest_cursor == NULL)
        return (false);

    /* If the value is returned from the stable table, it must be a proper value. */
    if (clayered->current_cursor != clayered->ingest_cursor)
        return (false);

    return (__wt_clayered_deleted(item));
}

/*
 * __clayered_deleted_encode --
 *     Encode values that are in the encoded name space.
 */
static WT_INLINE int
__clayered_deleted_encode(
  WT_SESSION_IMPL *session, const WT_ITEM *value, WT_ITEM *final_value, WT_ITEM **tmpp)
{
    WT_ITEM *tmp;

    /*
     * If value requires encoding, get a scratch buffer of the right size and create a copy of the
     * data with the first byte of the tombstone appended.
     */
    if (value->size >= __wt_tombstone.size &&
      memcmp(value->data, __wt_tombstone.data, __wt_tombstone.size) == 0) {
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

/*
 * __clayered_deleted_decode --
 *     Decode values that start with the tombstone.
 */
static WT_INLINE void
__clayered_deleted_decode(WT_ITEM *value)
{
    if (value->size > __wt_tombstone.size &&
      memcmp(value->data, __wt_tombstone.data, __wt_tombstone.size) == 0)
        --value->size;
}

/*
 * __clayered_get_collator --
 *     Retrieve the collator for a layered cursor. Wrapped in a function, since in the future the
 *     collator might live in a constituent cursor instead of the handle.
 */
static void
__clayered_get_collator(WT_CURSOR_LAYERED *clayered, WT_COLLATOR **collatorp)
{
    *collatorp = ((WT_LAYERED_TABLE *)clayered->dhandle)->collator;
}

/*
 * __clayered_primary_ingest --
 *     Newest ingest cursor (follower writes land here), or NULL.
 */
static WT_INLINE WT_CURSOR *
__clayered_primary_ingest(WT_CURSOR_LAYERED *clayered)
{
    if (clayered->ingest_cursors == NULL || clayered->n_ingest_cursors == 0)
        return (NULL);
    return (clayered->ingest_cursors[clayered->n_ingest_cursors - 1]);
}

static int __clayered_rollover_ingest(WT_SESSION_IMPL *session, WT_CURSOR_LAYERED *clayered);

/*
 * __clayered_cursor_is_ingest --
 *     Return if a constituent cursor is one of the ingest tables.
 */
static bool
__clayered_cursor_is_ingest(WT_CURSOR_LAYERED *clayered, WT_CURSOR *c)
{
    u_int i;

    if (c == NULL || clayered->ingest_cursors == NULL)
        return (false);
    for (i = 0; i < clayered->n_ingest_cursors; i++)
        if (clayered->ingest_cursors[i] == c)
            return (true);
    return (false);
}

/*
 * __clayered_cursor_compare --
 *     Compare two constituent cursors in a layered tree
 */
static int
__clayered_cursor_compare(WT_CURSOR_LAYERED *clayered, WT_CURSOR *c1, WT_CURSOR *c2, int *cmpp)
{
    WT_COLLATOR *collator;
    WT_SESSION_IMPL *session;

    session = CUR2S(clayered);

    WT_ASSERT_ALWAYS(session, F_ISSET(c1, WT_CURSTD_KEY_SET) && F_ISSET(c2, WT_CURSTD_KEY_SET),
      "Can only compare cursors with keys available in layered tree");

    __clayered_get_collator(clayered, &collator);
    return (__wt_compare(session, collator, &c1->key, &c2->key, cmpp));
}

/*
 * __clayered_enter --
 *     Start an operation on a layered cursor.
 */
static WT_INLINE int
__clayered_enter(WT_CURSOR_LAYERED *clayered, bool reset, bool update, bool iteration)
{
    WT_DECL_RET;
    WT_SESSION_IMPL *session;
    bool external_state_change;

    session = CUR2S(clayered);
    /*
     * FIXME-WT-15058: When inside a read committed isolation, the file cursor code expects to
     * release the snapshot when the count of active cursors is zero. Reset the constituent cursors
     * to adhere to that behavior. Ideally we should not be changing the active cursors counter
     * outside of the file cursor code.
     */
    if (reset && __wt_txn_read_committed_should_release_snapshot(session)) {
        WT_ASSERT(session, !F_ISSET(&clayered->iface, WT_CURSTD_KEY_INT | WT_CURSTD_VALUE_INT));
        WT_RET(__clayered_reset_cursors(clayered, false));
    }

    WT_RET(__clayered_adjust_state(clayered, iteration, &external_state_change));

    if (external_state_change || clayered->ingest_cursors == NULL ||
      (need_read_stable && clayered->stable_cursor == NULL &&
        clayered->checkpoint_meta_lsn != WT_DISAGG_LSN_NONE))
        WT_RET(__clayered_open_cursors(session, clayered));

    if (!F_ISSET(clayered, WT_CLAYERED_ACTIVE)) {
        /*
         * Opening this layered cursor has opened a number of btree cursors, ensure other code
         * doesn't think this is the first cursor in a session.
         */
        ++session->ncursors;
        WT_RET(__cursor_enter(session));
        F_SET(clayered, WT_CLAYERED_ACTIVE);
    }

    return (0);
}

/*
 * __clayered_leave --
 *     Finish an operation on a layered cursor.
 */
static void
__clayered_leave(WT_CURSOR_LAYERED *clayered)
{
    WT_SESSION_IMPL *session;

    session = CUR2S(clayered);

    if (F_ISSET(clayered, WT_CLAYERED_ACTIVE)) {
        --session->ncursors;
        __cursor_leave(session);
        F_CLR(clayered, WT_CLAYERED_ACTIVE);
    }
}

/*
 * __clayered_close_cursors --
 *     Close any btree cursors that are not needed.
 */
static int
__clayered_close_cursors(WT_CURSOR_LAYERED *clayered)
{
    WT_CURSOR *c;
    WT_SESSION_IMPL *session;

    session = CUR2S(clayered);
    clayered->current_cursor = NULL;
    if (clayered->ingest_cursors != NULL) {
        u_int i;

        for (i = 0; i < clayered->n_ingest_cursors; i++)
            if ((c = clayered->ingest_cursors[i]) != NULL)
                WT_RET(c->close(c));
        __wt_free(session, clayered->ingest_cursors);
        clayered->ingest_cursors = NULL;
        clayered->n_ingest_cursors = 0;
    }
    if ((c = clayered->stable_cursor) != NULL) {
        WT_RET(c->close(c));
        clayered->stable_cursor = NULL;
    }

    /* Some flags persist across closes of constituents. */
    F_CLR(clayered, ~(WT_CLAYERED_ACTIVE | WT_CLAYERED_RANDOM));
    return (0);
}

/*
 * __clayered_configure_random --
 *     Make a configuration string that either empty or includes any random configuration as
 *     appropriate.
 */
static int
__clayered_configure_random(
  WT_SESSION_IMPL *session, WT_CURSOR_LAYERED *clayered, WT_ITEM *random_config)
{
    /*
     * If the layered cursor is configured with next_random, we'll need to open any constituent
     * cursors with the same configuration that is relevant for random cursors.
     */
    if (F_ISSET(clayered, WT_CLAYERED_RANDOM))
        WT_RET(__wt_buf_fmt(session, random_config,
          "next_random=true,next_random_seed=%" PRId64 ",next_random_sample_size=%" PRIu64,
          clayered->next_random_seed, (uint64_t)clayered->next_random_sample_size));

    return (0);
}

/*
 * __clayered_open_stable --
 *     Open the stable cursor using the given role.
 */
static int
__clayered_open_stable(WT_CURSOR_LAYERED *clayered, bool leader)
{
    WT_CURSOR *c;
    WT_DECL_ITEM(random_config);
    WT_DECL_ITEM(stable_uri_buf);
    WT_DECL_RET;
    WT_LAYERED_TABLE *layered;
    WT_SESSION_IMPL *session;
    const char *cfg[4] = {WT_CONFIG_BASE(CUR2S(clayered), WT_SESSION_open_cursor), "", NULL, NULL};
    const char *checkpoint_name, *stable_uri;

    session = CUR2S(clayered);
    c = &clayered->iface;
    layered = (WT_LAYERED_TABLE *)clayered->dhandle;
    checkpoint_name = NULL;

    WT_RET(__wt_scr_alloc(session, 0, &random_config));
    /* Get the configuration for random cursors, if any. */
    WT_ERR(__clayered_configure_random(session, clayered, random_config));

    if (random_config->size > 0)
        cfg[1] = random_config->data;

retry:
    stable_uri = layered->stable_uri;
    if (!leader) {
        /*
         * We may have a stable chunk with no checkpoint yet. If that's the case then open a cursor
         * on stable without a checkpoint. It will never return an invalid result (it's content is
         * by definition trailing the ingest cursor). It is just slightly less efficient, and also
         * not an accurate reflection of what we want in terms of sharing checkpoints across
         * different WiredTiger instances eventually.
         */

        /* Look up the most recent data store checkpoint. This fetches the exact name to use. */
        WT_ERR_NOTFOUND_OK(
          __wt_meta_checkpoint_last_name(session, stable_uri, &checkpoint_name, NULL, NULL), true);

        if (ret == WT_NOTFOUND) {
            /*
             * We've never picked up a checkpoint, open a regular btree on the stable URI. If we're
             * a follower and we never picked up a checkpoint, then no checkpoint has ever occurred
             * on this Btree. Everything we need will be satisfied by the ingest table until the
             * next checkpoint is picked up. So technically, opening this (empty) stable table is
             * wasteful, but it's a corner case, it will be resolved at the next checkpoint, and it
             * keeps the code easy.
             *
             * FIXME-WT-16476: how to close this dhandle later as it is a live btree handle? We may
             * get this dhandle when the node steps up.
             */
            F_SET(clayered, WT_CLAYERED_STABLE_NO_CKPT);
        } else {
            if (stable_uri_buf == NULL)
                WT_ERR(__wt_scr_alloc(session, 0, &stable_uri_buf));
            /*
             * Use a URI with a "/<checkpoint name> suffix. This is interpreted as reading from the
             * stable checkpoint, but without it being a traditional checkpoint cursor.
             */
            WT_ERR(
              __wt_buf_fmt(session, stable_uri_buf, "%s/%s", layered->stable_uri, checkpoint_name));
            stable_uri = stable_uri_buf->data;
        }
        cfg[2] = "read_only=true";
    }

    ret = __wt_open_cursor(session, stable_uri, c, cfg, &clayered->stable_cursor);

    if (ret == EBUSY && !leader) {
        __wt_free(session, checkpoint_name);
        /* FIXME-WT-16476: no need to yield if we no longer take the checkpoint lock. */
        __wt_yield();
        goto retry;
    }

    /* Opening a cursor can return both of these, unfortunately. FIXME-WT-15816. */
    if ((ret == ENOENT || ret == WT_NOTFOUND) && !leader)
        /*
         * This is fine on followers, we simply may not have seen a checkpoint with this table yet.
         * Defer the open.
         */
        ret = 0;
    WT_ERR(ret);

    if (clayered->stable_cursor != NULL) {
        F_SET(clayered->stable_cursor, WT_CURSTD_OVERWRITE | WT_CURSTD_RAW);

        /* Layered cursor is not compatible with cursor_copy config. */
        F_CLR(clayered->stable_cursor, WT_CURSTD_DEBUG_COPY_KEY | WT_CURSTD_DEBUG_COPY_VALUE);

        if (F_ISSET(c, WT_CURSTD_DEBUG_RESET_EVICT))
            F_SET(clayered->stable_cursor, WT_CURSTD_DEBUG_RESET_EVICT);
    }

err:
    __wt_scr_free(session, &random_config);
    __wt_scr_free(session, &stable_uri_buf);
    __wt_free(session, checkpoint_name);

    return (ret);
}

/*
 * __clayered_can_stable_upgrade --
 *     Return true if the stable cursor can be upgraded at this time. For the most part we mirror
 *     our decision about when we can upgrade by when a snapshot is allowed to be upgraded.
 */
static bool
__clayered_ingest_check_close(WT_SESSION_IMPL *session, WT_CURSOR_LAYERED *clayered)
{
    /* See if there's nothing to do for the ingest cursors. */
    if (clayered->ingest_cursors == NULL)
        return (false);

    bool leader = S2C(session)->disagg_layered_leader;
    /*
     * Layered cursor is positioned on an ingest cursor. Changing it may lose the layered cursor
     * position.
     */
    if (F_ISSET(&clayered->iface, WT_CURSTD_KEY_INT) &&
      __clayered_cursor_is_ingest(clayered, clayered->current_cursor)) {
        /* This should not happen on the leader at the moment. */
        WT_ASSERT(session, !leader);
        return (false);
    }

    /* For the ingest table, we'll need to close it or open it. Either way it's a change. */
    if (leader == clayered->leader)
        return (false);

    return (true);
}

/*
 * __clayered_can_advance_stable --
 *     Return true if the stable cursor can be advanced to a newer checkpoint at this time.
 */
static bool
__clayered_can_advance_stable(WT_CURSOR_LAYERED *clayered, bool iteration)
{
    WT_SESSION_IMPL *session;
    WT_TXN_SHARED *txn_shared;
    bool can_upgrade;

    session = CUR2S(clayered);
    can_upgrade = false;

    /*
     * First, layered cursors are sometimes paired with read timestamps. When using read timestamps,
     * it's always safe to update cursors, even during iterations. That's because the view at a
     * timestamp is always consistent, the history store covers that.
     */
    txn_shared = WT_SESSION_TXN_SHARED(session);
    if (txn_shared != NULL && txn_shared->read_timestamp != WT_TS_NONE)
        can_upgrade = true;
    else {
        /* if this is an iteration, we won't upgrade the cursor, we're done. */
        if (iteration)
            return (0);

        /*
         * There are other points when it is appropriate to update cursors. If we don't currently
         * have a transactional snapshot, or if the snapshot has changed, we can update.
         *
         * Why shouldn't we update when in a transaction? We may have read some values, and we'd
         * expect to see the same values if we read them again. Reading from a newer checkpoint can
         * violate that.
         */
        if (!F_ISSET(session->txn, WT_TXN_HAS_SNAPSHOT) ||
          (__wt_session_gen(session, WT_GEN_HAS_SNAPSHOT) != clayered->snapshot_gen))
            can_upgrade = true;
    }

    return (false);
}

/*
 * __clayered_advance_stable --
 *     Advance the stable cursor to a newer checkpoint.
 */
static int
__clayered_advance_stable(
  WT_SESSION_IMPL *session, WT_CURSOR_LAYERED *clayered, bool current_leader)
{
    WT_CURSOR *old_stable;
    WT_DECL_RET;

    /*
     * We can't just close the stable cursor here, as we need to retain any position that the
     * current stable cursor has. It's easier to keep the old cursor open briefly while we copy the
     * position.
     */
    old_stable = clayered->stable_cursor;
    clayered->stable_cursor = NULL;

    WT_ERR(__clayered_open_stable(clayered, current_leader));
    WT_ASSERT(session, clayered->stable_cursor != NULL);

    /*
     * If the old cursor has a position, copy it to the newly opened cursor. Prepared updates are
     * always ignored on the stable cursor, making it safe to check the WT_CURSTD_KEY_INT flag.
     */
    if (F_ISSET(old_stable, WT_CURSTD_KEY_INT)) {
        WT_ERR_NOTFOUND_OK(__wt_cursor_dup_position(old_stable, clayered->stable_cursor), true);
        /*
         * If the key is removed from the new checkpoint, the layered cursor must be positioned on
         * the ingest table.
         */
        WT_ASSERT_ALWAYS(session,
          ret == 0 || !F_ISSET(&clayered->iface, WT_CURSTD_KEY_INT) ||
            __clayered_cursor_is_ingest(clayered, clayered->current_cursor),
          "upgrading a positioned stable cursor");
        /*
         * If the key is removed in the new checkpoint, clear the iteration flag to reposition it to
         * the correct location.
         */
        if (ret == WT_NOTFOUND)
            F_CLR(clayered, WT_CLAYERED_ITERATE_NEXT | WT_CLAYERED_ITERATE_PREV);
    } else if (F_ISSET(old_stable, WT_CURSTD_KEY_EXT)) {
        WT_ITEM_SET(clayered->stable_cursor->key, old_stable->key);
        if (F_ISSET(old_stable, WT_CURSTD_VALUE_EXT))
            WT_ITEM_SET(clayered->stable_cursor->value, old_stable->value);
    }

    /* Add any bounds for the new cursor. */
    WT_ERR(__clayered_copy_bounds(clayered));

    if (clayered->current_cursor == old_stable) {
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

err:
    if (ret == 0) {
        /* Close the old cursor. */
        WT_TRET(old_stable->close(old_stable));
        WT_STAT_CONN_DSRC_INCR(session, layered_curs_advance_stable);
    } else {
        /* Give up the advancement if we fail. */
        if (clayered->stable_cursor != NULL)
            WT_TRET(clayered->stable_cursor->close(clayered->stable_cursor));
        clayered->stable_cursor = old_stable;
    }

    return (ret);
}

/*
 * __clayered_adjust_state --
 *     Update the state of the cursor to match the state of the disaggregated system. In particular,
 *     if the system has changed in a way that makes constituent cursors out of date, either reopen
 *     them or close them, and let them be opened later as needed.
 */
static int
__clayered_adjust_state(WT_CURSOR_LAYERED *clayered, bool iteration, bool *state_updated)
{
    WT_CONNECTION_IMPL *conn;
    WT_CURSOR *old_stable;
    WT_SESSION_IMPL *session;
    uint64_t last_checkpoint_meta_lsn, snapshot_gen;
    bool change_ingest, change_stable, current_leader;

    *state_updated = false;
    session = CUR2S(clayered);
    conn = S2C(session);
    current_leader = conn->disagg_layered_leader;

    /* Get the current checkpoint LSN. This only matters if we are a follower. */
    if (!current_leader)
        last_checkpoint_meta_lsn =
          __wt_atomic_load_uint64_acquire(&conn->disaggregated_storage.last_checkpoint_meta_lsn);
    else
        last_checkpoint_meta_lsn = WT_DISAGG_LSN_NONE;

    /*
     * Has any state changed? What is not checked here is the possibility that a step down and step
     * up have both occurred since the last check. We don't have a way to detect that (or its
     * opposite) at the moment. If we did, we'd want to issue a rollback if the stable cursor has
     * any changes. FIXME-WT-14545.
     */
    if (current_leader == clayered->leader &&
      last_checkpoint_meta_lsn == clayered->checkpoint_meta_lsn)
        return (0);

    change_ingest = false;
    snapshot_gen = clayered->snapshot_gen;

    /* Is this a step up or step down? */
    if (current_leader != clayered->leader) {
        /* For the ingest table, we'll need to close it or open it. Either way it's a change. */
        change_ingest = true;

        /*
         * If we're stepping down, then we currently have a R/W stable cursor and all writes would
         * go to it. Any writes we were about to make or have made to this table could never be
         * committed at this point.
         */
        if (!current_leader && session->txn->mod_count != 0) {
            __wt_txn_err_set(session, WT_ROLLBACK);
            /* Write operations are not allowed after stepping down from leader role. */
            WT_RET(WT_ROLLBACK);
        }

        /*
         * It turns out that the right choice for step up and step down is always to reopen the
         * stable cursor whenever we can.
         *
         * For step up, we're currently using a readonly stable cursor at a checkpoint. We can
         * reopen the stable cursor, we'd get a R/W cursor. We don't need the ability to write, as
         * this request was kicked off on the follower, so it must be all reads. But we want to
         * discard the stable cursor when we can, as long as we're not breaking transactional
         * semantics for cursors.
         *
         * For step down, we're currently using a R/W stable cursor. After the check above, we know
         * we've done read operations to this point. So again, we should upgrade if we can.
         */
    }
    /*
     * Even if the leader hasn't changed, we can get here if we have a new checkpoint on the
     * follower. And again, we'd like to reopen the stable cursor if we can.
     */
    change_stable = __clayered_can_stable_upgrade(clayered, iteration);

    /* See if there's nothing to do for the ingest cursor. */
    if (clayered->ingest_cursor == NULL)
        change_ingest = false;

    /* A random stable cursor shouldn't be reopened, it may have additional state. */
    if (clayered->stable_cursor == NULL || F_ISSET(clayered, WT_CLAYERED_RANDOM))
        change_stable = false;

    if (change_ingest) {
        /*
         * To reopen the ingest tables, all we need to do here is close them. They will be reopened
         * when needed. There's never a situation where we need to save their position.
         */
        u_int i;

        for (i = 0; i < clayered->n_ingest_cursors; i++) {
            WT_CURSOR *ing;

            if ((ing = clayered->ingest_cursors[i]) == NULL)
                continue;
            WT_RET(ing->close(ing));
            if (clayered->current_cursor == ing)
                clayered->current_cursor = NULL;
            clayered->ingest_cursors[i] = NULL;
        }
        __wt_free(session, clayered->ingest_cursors);
        clayered->ingest_cursors = NULL;
        clayered->n_ingest_cursors = 0;
        WT_STAT_CONN_DSRC_INCR(session, layered_curs_reopen_ingest);
    }

    if (change_stable) {
        /*
         * We can't just close the stable cursor here, as we need to retain any position that the
         * current stable cursor has. It's easier to keep the old cursor open briefly while we copy
         * the position.
         */
        old_stable = clayered->stable_cursor;
        clayered->stable_cursor = NULL;
        snapshot_gen = __wt_session_gen(session, WT_GEN_HAS_SNAPSHOT);

        WT_RET(__clayered_open_stable(clayered, current_leader));
        WT_STAT_CONN_DSRC_INCR(session, layered_curs_upgrade_stable);

        /* If the old cursor has a position, copy it to the newly opened cursor. */
        if (F_ISSET(old_stable, WT_CURSTD_KEY_SET))
            WT_RET(__wt_cursor_dup_position(old_stable, clayered->stable_cursor));

        if (clayered->current_cursor == old_stable) {
            WT_CURSOR *cursor = (WT_CURSOR *)clayered;
            WT_CURSOR *new_stable = clayered->stable_cursor;
            if (F_ISSET(cursor, WT_CURSTD_KEY_INT)) {
                /* Reset the cursor key to point to the new stable cursor. */
                WT_RET(new_stable->get_key(new_stable, &cursor->key));
                /* Clear the value as the new stable cursor may point to a different one. */
                F_CLR(cursor, WT_CURSTD_VALUE_INT);
            }
            clayered->current_cursor = new_stable;
        }

        /* Close the old cursor. */
        WT_RET(old_stable->close(old_stable));

        /* Add any bounds for the new cursor. */
        WT_RET(__clayered_copy_bounds(clayered));
    }

    /* Update the state of the layered cursor. */
    clayered->leader = current_leader;
    clayered->checkpoint_meta_lsn = last_checkpoint_meta_lsn;
    clayered->snapshot_gen = snapshot_gen;
    *state_updated = (change_ingest || change_stable);

    return (0);
}

/*
 * __clayered_open_one_ingest --
 *     Open a single ingest cursor by URI.
 */
static int
__clayered_open_one_ingest(WT_SESSION_IMPL *session, WT_CURSOR_LAYERED *clayered,
  const char *ingest_uri, WT_CURSOR **cursorp)
{
    WT_CURSOR *c, *cursor;
    WT_DECL_ITEM(random_config);
    WT_DECL_RET;
    const char *ckpt_cfg[3] = {WT_CONFIG_BASE(session, WT_SESSION_open_cursor), "", NULL};

    c = &clayered->iface;

    WT_RET(__wt_scr_alloc(session, 0, &random_config));
    /* Get the configuration for random cursors, if any. */
    WT_ERR(__clayered_configure_random(session, clayered, random_config));
    if (random_config->size > 0)
        ckpt_cfg[1] = random_config->data;

    WT_ERR(__wt_open_cursor(session, ingest_uri, c, ckpt_cfg, &cursor));
    F_SET(cursor, WT_CURSTD_OVERWRITE | WT_CURSTD_RAW);

    if (F_ISSET(c, WT_CURSTD_DEBUG_RESET_EVICT))
        F_SET(cursor, WT_CURSTD_DEBUG_RESET_EVICT);

    *cursorp = cursor;

err:
    __wt_scr_free(session, &random_config);
    return (ret);
}

/*
 * __clayered_open_cursors --
 *     Open cursors for the current set of files.
 */
static int
__clayered_open_cursors(WT_SESSION_IMPL *session, WT_CURSOR_LAYERED *clayered, bool update)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_LAYERED_TABLE *layered;
    u_int i;
    u_int n_ingest_snapshot;
    const char **ingest_uris_snapshot;
    bool chunk_lock_held;
    bool leader;

    c = &clayered->iface;
    conn = S2C(session);
    layered = (WT_LAYERED_TABLE *)clayered->dhandle;

    WT_ASSERT(session, layered->n_ingest_uris > 0);

    /*
     * The ingest list can be rotated/garbage-collected by other threads under the layered ingest
     * chunk lock. Snapshot the current ingest URIs (strings live for the handle lifetime) so we
     * don't race with the pointer array being replaced/freed while we open cursors.
     */
    ingest_uris_snapshot = NULL;
    n_ingest_snapshot = 0;
    chunk_lock_held = false;
    __wt_spin_lock(session, &layered->ingest_chunk_lock);
    chunk_lock_held = true;
    n_ingest_snapshot = layered->n_ingest_uris;
    WT_ASSERT(session, n_ingest_snapshot > 0);
    WT_ERR(
      __wt_calloc(session, (size_t)n_ingest_snapshot, sizeof(const char *), &ingest_uris_snapshot));
    for (i = 0; i < n_ingest_snapshot; i++)
        ingest_uris_snapshot[i] = layered->ingest_uris[i];
    __wt_spin_unlock(session, &layered->ingest_chunk_lock);
    chunk_lock_held = false;

    if (clayered->ingest_cursors != NULL && clayered->n_ingest_cursors == n_ingest_snapshot) {
        for (i = 0; i < clayered->n_ingest_cursors; i++)
            if (clayered->ingest_cursors[i] == NULL)
                break;
        if (i == clayered->n_ingest_cursors) {
            if (!F_ISSET(clayered, WT_CLAYERED_READ_STABLE))
                goto done;
            if (clayered->stable_cursor != NULL)
                goto done;
        }
    }

    /*
     * If the key is pointing to memory that is pinned by a chunk cursor, take a copy before closing
     * cursors.
     */
    if (F_ISSET(c, WT_CURSTD_KEY_INT))
        WT_RET(__cursor_needkey(c));

    F_CLR(clayered, WT_CLAYERED_ITERATE_NEXT | WT_CLAYERED_ITERATE_PREV);

    if (clayered->ingest_cursors != NULL && clayered->n_ingest_cursors != n_ingest_snapshot) {
        for (i = 0; i < clayered->n_ingest_cursors; i++)
            if (clayered->ingest_cursors[i] != NULL)
                WT_ERR(clayered->ingest_cursors[i]->close(clayered->ingest_cursors[i]));
        __wt_free(session, clayered->ingest_cursors);
        clayered->ingest_cursors = NULL;
        clayered->n_ingest_cursors = 0;
    }

    if (clayered->ingest_cursors == NULL)
        WT_ERR(
          __wt_calloc(session, n_ingest_snapshot, sizeof(WT_CURSOR *), &clayered->ingest_cursors));
    clayered->n_ingest_cursors = n_ingest_snapshot;

    for (i = 0; i < n_ingest_snapshot; i++)
        if (clayered->ingest_cursors[i] == NULL)
            WT_ERR(__clayered_open_one_ingest(
              session, clayered, ingest_uris_snapshot[i], &clayered->ingest_cursors[i]));

    if (F_ISSET(clayered, WT_CLAYERED_READ_STABLE) && clayered->stable_cursor == NULL) {
        leader = conn->disagg_layered_leader;
        WT_ERR(__clayered_open_stable(clayered, leader));
    }

    if (F_ISSET(clayered, WT_CLAYERED_RANDOM)) {
        /*
         * Cursors configured with next_random only allow the next method to be called. But our
         * implementation of random requires search_near to be called on constituent file cursors,
         * so explicitly allow that here.
         */
        for (i = 0; i < clayered->n_ingest_cursors; i++) {
            WT_ASSERT(session, WT_PREFIX_MATCH(clayered->ingest_cursors[i]->uri, "file:"));
            clayered->ingest_cursors[i]->search_near = __wti_curfile_search_near;
        }

        /*
         * If the stable cursor is not set, and we've succeeded to this point, that means we've
         * deferred opening the stable cursor.
         */
        if (clayered->stable_cursor != NULL) {
            WT_ASSERT(session, WT_PREFIX_MATCH(clayered->stable_cursor->uri, "file:"));
            clayered->stable_cursor->search_near = __wti_curfile_search_near;
        }
    }

    /*
     * Set any boundaries for any newly opened cursors.
     */
    WT_ERR(__clayered_copy_bounds(clayered));

done:
    __wt_free(session, ingest_uris_snapshot);
    return (0);

err:
    if (chunk_lock_held)
        __wt_spin_unlock(session, &layered->ingest_chunk_lock);
    __wt_free(session, ingest_uris_snapshot);
    return (ret);
}

/*
 * __clayered_get_current --
 *     Find the smallest / largest of the cursors and copy its key/value.
 */
static int
__clayered_get_current(WT_SESSION_IMPL *session, WT_CURSOR_LAYERED *clayered, bool smallest)
{
    WT_COLLATOR *collator;
    WT_CURSOR *c, *current;
    u_int i;
    int cmp, pri, best_pri;

    current = NULL;
    best_pri = -2;

    __clayered_get_collator(clayered, &collator);

    /*
     * FIXME-WT-16810: In leader mode, skip ingest as it should be empty. This will need revisiting
     * when asynchronous step-up is supported, because ingest may legitimately contain data for some
     * time after promotion.
     */
    if (!clayered->leader && clayered->ingest_cursors != NULL) {
        for (i = 0; i < clayered->n_ingest_cursors; i++) {
            c = clayered->ingest_cursors[i];
            if (c == NULL || !F_ISSET(c, WT_CURSTD_KEY_INT))
                continue;
            pri = (int)i;
            if (current == NULL) {
                current = c;
                best_pri = pri;
                continue;
            }
            WT_RET(__wt_compare(session, collator, &c->key, &current->key, &cmp));
            if (smallest) {
                if (cmp < 0 || (cmp == 0 && pri > best_pri)) {
                    current = c;
                    best_pri = pri;
                }
            } else if (cmp > 0 || (cmp == 0 && pri > best_pri)) {
                current = c;
                best_pri = pri;
            }
        }
    }

    if (clayered->stable_cursor != NULL && F_ISSET(clayered->stable_cursor, WT_CURSTD_KEY_INT)) {
        c = clayered->stable_cursor;
        pri = -1;
        if (current == NULL) {
            current = c;
            best_pri = pri;
        } else {
            WT_RET(__wt_compare(session, collator, &c->key, &current->key, &cmp));
            if (smallest) {
                if (cmp < 0 || (cmp == 0 && pri > best_pri)) {
                    current = c;
                    best_pri = pri;
                }
            } else if (cmp > 0 || (cmp == 0 && pri > best_pri)) {
                current = c;
                best_pri = pri;
            }
        }
    }

    if (current == NULL) {
        clayered->current_cursor = NULL;
        return (WT_NOTFOUND);
    }

    WT_ASSERT_ALWAYS(
      session, current != NULL, "Both constituents are positioned, but we cannot choose current");
    clayered->current_cursor = current;

    return (0);
}

/*
 * __clayered_compare --
 *     WT_CURSOR->compare implementation for the layered cursor type.
 */
static int
__clayered_compare(WT_CURSOR *a, WT_CURSOR *b, int *cmpp)
{
    WT_COLLATOR *collator;
    WT_CURSOR_LAYERED *clayered;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    /* There's no need to sync with the layered tree, avoid layered enter. */
    clayered = (WT_CURSOR_LAYERED *)a;
    CURSOR_API_CALL(a, session, ret, compare, clayered->dhandle);

    /*
     * Confirm both cursors refer to the same source and have keys, then compare the keys.
     */
    if (strcmp(a->internal_uri, b->internal_uri) != 0)
        WT_ERR_MSG(session, EINVAL, "comparison method cursors must reference the same object");

    /* Both cursors are from the same tree - they share the same collator */
    __clayered_get_collator(clayered, &collator);

    WT_ERR(__wt_compare(session, collator, &a->key, &b->key, cmpp));

err:
    API_END_RET(session, ret);
}

/*
 * __clayered_position_alternate --
 *     Position an alternate cursor to the right position according to the current one.
 */
static int
__clayered_position_alternate(WT_CURSOR_LAYERED *clayered, WT_CURSOR *alternate, bool forward)
{
    int cmp;

    WT_CURSOR *current = clayered->current_cursor;
    WT_SESSION_IMPL *session = CUR2S(clayered);

    WT_ASSERT(session, F_ISSET(current, WT_CURSTD_KEY_SET));
    alternate->set_key(alternate, &current->key);
    WT_RET(alternate->search_near(alternate, &cmp));

    while (forward ? cmp < 0 : cmp > 0) {
        WT_RET(forward ? alternate->next(alternate) : alternate->prev(alternate));

        /*
         * With higher isolation levels, where we have stable reads, we're done: the cursor is now
         * positioned as expected.
         *
         * With read-uncommitted isolation, a new record could have appeared in between the search
         * and stepping forward / back. In that case, keep going until we see a key in the expected
         * range.
         */
        if (session->txn->isolation != WT_ISO_READ_UNCOMMITTED)
            return (0);

        WT_RET(__clayered_cursor_compare(clayered, alternate, current, &cmp));
    }

    return (0);
}

/*
 * __clayered_constituent_iter --
 *     Move the cursor forward or backward.
 */
static int
__clayered_constituent_iter(WT_CURSOR *constituent, bool forward)
{
    return (forward ? constituent->next(constituent) : constituent->prev(constituent));
}

/*
 * __clayered_iterate_constituents --
 *     Move the constituents to the next (or prev) position. If the cursor is unpositioned, position
 *     the constituents. Ingest tables are merged oldest to newest over stable; iteration keeps
 *     every constituent aligned so __clayered_get_current can pick the visible row.
 */
static int
__clayered_iterate_constituents(WT_CURSOR_LAYERED *clayered, uint32_t iter_flag, bool deleted)
{
    WT_CURSOR *c, *c_current, *c_stable;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;
    u_int i;
    int cmp;
    bool any_ingest_ref, current_moved, forward;

    session = CUR2S(clayered);
    current_moved = false;
    forward = (iter_flag == WT_CLAYERED_ITERATE_NEXT);
    c_stable = clayered->stable_cursor;

    /*
     * FIXME-WT-16810: In leader mode, skip iterating through ingest as it should be empty. This
     * will need revisiting when asynchronous step-up is supported, because ingest may legitimately
     * contain data for some time after promotion.
     */
    if (clayered->leader) {
        WT_ASSERT(session, c_stable != NULL);
        return (__clayered_constituent_iter(c_stable, forward));
    }

    WT_ASSERT(session, clayered->n_ingest_cursors > 0);
    WT_ASSERT(session, c_stable != NULL || clayered->n_ingest_cursors > 0);

    /* Only ingest chunk(s), no stable cursor yet. */
    if (c_stable == NULL) {
        if (clayered->n_ingest_cursors == 1)
            return (__clayered_constituent_iter(clayered->ingest_cursors[0], forward));
        goto merge_ingests_only;
    }

    if (clayered->n_ingest_cursors == 0)
        return (__clayered_constituent_iter(c_stable, forward));

    any_ingest_ref = false;
    for (i = 0; i < clayered->n_ingest_cursors; i++) {
        c = clayered->ingest_cursors[i];
        WT_ASSERT(session, c != NULL);
        if (((WT_CURSOR_BTREE *)c)->ref != NULL) {
            any_ingest_ref = true;
            break;
        }
    }

    /*
     * Start of walk: no ingest refs and stable not positioned advance stable first, then each
     * ingest (stable first avoids pinning the wrong page on prepared conflicts).
     */
    if (!any_ingest_ref && !F_ISSET(c_stable, WT_CURSTD_KEY_INT)) {
        WT_ERR_NOTFOUND_OK(__clayered_constituent_iter(c_stable, forward), false);
        for (i = 0; i < clayered->n_ingest_cursors; i++)
            WT_ERR_NOTFOUND_OK(
              __clayered_constituent_iter(clayered->ingest_cursors[i], forward), false);
        goto done;
    }

merge_ingests_only:
    /*
     * Ingest-only merge (no stable): same alignment rules among ingest cursors only.
     */
    if (c_stable == NULL) {
        any_ingest_ref = false;
        for (i = 0; i < clayered->n_ingest_cursors; i++) {
            c = clayered->ingest_cursors[i];
            if (((WT_CURSOR_BTREE *)c)->ref != NULL) {
                any_ingest_ref = true;
                break;
            }
        }
        if (!any_ingest_ref) {
            bool any_key_int;

            any_key_int = false;
            for (i = 0; i < clayered->n_ingest_cursors; i++)
                if (F_ISSET(clayered->ingest_cursors[i], WT_CURSTD_KEY_INT)) {
                    any_key_int = true;
                    break;
                }
            if (!any_key_int) {
                for (i = 0; i < clayered->n_ingest_cursors; i++)
                    WT_ERR_NOTFOUND_OK(
                      __clayered_constituent_iter(clayered->ingest_cursors[i], forward), false);
                goto done;
            }
        }
    }

    if (clayered->current_cursor == NULL) {
        c_current = NULL;
        for (i = 0; i < clayered->n_ingest_cursors; i++) {
            c = clayered->ingest_cursors[i];
            if (((WT_CURSOR_BTREE *)c)->ref != NULL) {
                c_current = c;
                break;
            }
        }
        WT_ASSERT(session, c_current != NULL);
    } else {
        c_current = clayered->current_cursor;
        WT_ASSERT(session,
          F_ISSET(c_current, WT_CURSTD_KEY_INT) ||
            (__clayered_cursor_is_ingest(clayered, c_current) &&
              ((WT_CURSOR_BTREE *)c_current)->ref != NULL));
    }

    if (!F_ISSET(c_current, WT_CURSTD_KEY_INT)) {
        WT_ASSERT(session,
          __clayered_cursor_is_ingest(clayered, c_current) &&
            F_ISSET(clayered, WT_CLAYERED_ITERATE_NEXT | WT_CLAYERED_ITERATE_PREV));
        WT_ERR_NOTFOUND_OK(__clayered_constituent_iter(c_current, forward), false);
        current_moved = true;
    } else if (!F_ISSET(clayered, iter_flag)) {
        if (c_stable != NULL && c_current != c_stable)
            WT_ERR_NOTFOUND_OK(
              __clayered_position_alternate(clayered, c_current, c_stable, forward), false);
        for (i = 0; i < clayered->n_ingest_cursors; i++) {
            c = clayered->ingest_cursors[i];
            if (c != c_current)
                WT_ERR_NOTFOUND_OK(
                  __clayered_position_alternate(clayered, c_current, c, forward), false);
        }
    }

    if (c_stable != NULL && c_current != c_stable && F_ISSET(c_stable, WT_CURSTD_KEY_INT)) {
        WT_ERR(__clayered_cursor_compare(clayered, c_stable, c_current, &cmp));
        if (cmp == 0)
            WT_ERR_NOTFOUND_OK(__clayered_constituent_iter(c_stable, forward), false);
    }
    for (i = 0; i < clayered->n_ingest_cursors; i++) {
        c = clayered->ingest_cursors[i];
        if (c == c_current || !F_ISSET(c, WT_CURSTD_KEY_INT))
            continue;
        WT_ERR(__clayered_cursor_compare(clayered, c, c_current, &cmp));
        if (cmp == 0)
            WT_ERR_NOTFOUND_OK(__clayered_constituent_iter(c, forward), false);
    }

    if (!current_moved)
        WT_ERR_NOTFOUND_OK(__clayered_constituent_iter(c_current, forward), false);

done:
    if (!F_ISSET(clayered, iter_flag)) {
        F_CLR(clayered, WT_CLAYERED_ITERATE_PREV | WT_CLAYERED_ITERATE_NEXT);
        F_SET(clayered, iter_flag);
    }
    return (0);
}

/*
 * __clayered_iterate --
 *     Common function for moving a layered cursor to the next or previous position.
 */
static int
__clayered_iterate(WT_CURSOR_LAYERED *clayered, bool forward, uint32_t iter_flag)
{
    WT_DECL_RET;

    bool deleted = false;
    WT_SESSION_IMPL *session = CUR2S(clayered);
    WT_CURSOR *cursor = &clayered->iface;

    __cursor_novalue(cursor);
    WT_ERR(__clayered_enter(clayered, false, false, true));

    /*
     * FIXME-WT-16158: We currently check whether the entry has been deleted on the current cursor,
     * which may be positioned on either the ingest or the stable table. However, only the ingest
     * cursor can return tombstoned entries. This logic can be reworked to perform the deletion
     * check only on the ingest cursor and to call get_current() only after the next non-deleted
     * entry has been found.
     */
    do {
        WT_ERR(__clayered_iterate_constituents(clayered, iter_flag));
        WT_ERR(__clayered_get_current(session, clayered, iter_flag == WT_CLAYERED_ITERATE_NEXT));
        if (__clayered_cursor_is_ingest(clayered, clayered->current_cursor))
            deleted = __wt_clayered_deleted(&clayered->current_cursor->value);
        else
            deleted = false;
    } while (deleted);

err:
    __clayered_leave(clayered);
    if (ret == 0)
        __clayered_deleted_decode(&cursor->value);
    else {
        F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);
        if (ret != WT_PREPARE_CONFLICT)
            __clayered_reset_cursors(clayered, false);
    }
    return (ret);
}

/*
 * __clayered_next --
 *     WT_CURSOR->next method for the layered cursor type.
 */
static int
__clayered_next(WT_CURSOR *cursor)
{
    WT_CURSOR_LAYERED *clayered;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    clayered = (WT_CURSOR_LAYERED *)cursor;

    CURSOR_API_CALL(cursor, session, ret, next, clayered->dhandle);

    WT_STAT_CONN_DSRC_INCR(session, layered_curs_next);

    WT_ERR(__clayered_iterate(clayered, true, WT_CLAYERED_ITERATE_NEXT));

    if (__clayered_cursor_is_ingest(clayered, clayered->current_cursor))
        WT_STAT_CONN_DSRC_INCR(session, layered_curs_next_ingest);
    else
        WT_STAT_CONN_DSRC_INCR(session, layered_curs_next_stable);

err:
    API_END_RET(session, ret);
}

/*
 * __layered_prev --
 *     WT_CURSOR->prev method for the layered cursor type.
 */
static int
__layered_prev(WT_CURSOR *cursor)
{
    WT_CURSOR_LAYERED *clayered;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    clayered = (WT_CURSOR_LAYERED *)cursor;

    CURSOR_API_CALL(cursor, session, ret, prev, clayered->dhandle);

    WT_STAT_CONN_DSRC_INCR(session, layered_curs_prev);

    WT_ERR(__clayered_iterate(clayered, false, WT_CLAYERED_ITERATE_PREV));

    if (__clayered_cursor_is_ingest(clayered, clayered->current_cursor))
        WT_STAT_CONN_DSRC_INCR(session, layered_curs_prev_ingest);
    else
        WT_STAT_CONN_DSRC_INCR(session, layered_curs_prev_stable);

err:
    API_END_RET(session, ret);
}

/*
 * __clayered_reset_cursors --
 *     Reset any positioned constituent cursors. If skip_ingest is true, the ingest cursor is about
 *     to be used, so there is no need to reset it.
 */
static int
__clayered_reset_cursors(WT_CURSOR_LAYERED *clayered, bool skip_ingest)
{
    WT_CURSOR *c;
    WT_DECL_RET;

    /* Fast path if the cursor is not positioned. */
    if (clayered->current_cursor == NULL &&
      !F_ISSET(clayered, WT_CLAYERED_ITERATE_NEXT | WT_CLAYERED_ITERATE_PREV))
        return (0);

    c = clayered->stable_cursor;
    if (c != NULL && F_ISSET(c, WT_CURSTD_KEY_SET))
        WT_TRET(c->reset(c));

    if (!skip_ingest && clayered->ingest_cursors != NULL) {
        u_int i;

        for (i = 0; i < clayered->n_ingest_cursors; i++) {
            c = clayered->ingest_cursors[i];
            if (c != NULL && ((WT_CURSOR_BTREE *)c)->ref != NULL)
                WT_TRET(c->reset(c));
        }
    }

    clayered->current_cursor = NULL;
    F_CLR(clayered, WT_CLAYERED_ITERATE_NEXT | WT_CLAYERED_ITERATE_PREV);

    return (ret);
}

/*
 * __clayered_reset --
 *     WT_CURSOR->reset method for the layered cursor type.
 */
static int
__clayered_reset(WT_CURSOR *cursor)
{
    WT_CURSOR_LAYERED *clayered;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    /*
     * Don't use the normal __clayered_enter path: that is wasted work when all we want to do is
     * give up our position.
     */
    clayered = (WT_CURSOR_LAYERED *)cursor;
    CURSOR_API_CALL_PREPARE_ALLOWED(cursor, session, reset, clayered->dhandle);

    /* Reset any bounds on the top level cursor, and propagate that to constituents */
    __wt_cursor_bound_reset(cursor);
    WT_ERR(__clayered_copy_bounds(clayered));

    F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);

    WT_TRET(__clayered_reset_cursors(clayered, false));

    /* In case we were left positioned, clear that. */
    __clayered_leave(clayered);

err:
    API_END_RET(session, ret);
}

/*
 * __clayered_copy_constituent_bound --
 *     Copy the top level bound into a single constituent cursor
 */
static int
__clayered_copy_constituent_bound(WT_CURSOR_LAYERED *clayered, WT_CURSOR *constituent)
{
    WT_CURSOR *base_cursor;
    WT_SESSION_IMPL *session;

    session = CUR2S(clayered);
    base_cursor = (WT_CURSOR *)clayered;

    if (constituent == NULL)
        return (0);

    /*
     * It doesn't matter if the bound in question is already set on the constituent. It is legal to
     * reset it. Note that the inclusive flag is additive to upper/lower, so no need to check it as
     * well.
     */
    if (F_ISSET(base_cursor, WT_CURSTD_BOUND_UPPER))
        WT_RET(__wt_buf_set(session, &constituent->upper_bound, base_cursor->upper_bound.data,
          base_cursor->upper_bound.size));
    else {
        __wt_buf_free(session, &constituent->upper_bound);
        WT_CLEAR(constituent->upper_bound);
    }
    if (F_ISSET(base_cursor, WT_CURSTD_BOUND_LOWER))
        WT_RET(__wt_buf_set(session, &constituent->lower_bound, base_cursor->lower_bound.data,
          base_cursor->lower_bound.size));
    else {
        __wt_buf_free(session, &constituent->lower_bound);
        WT_CLEAR(constituent->lower_bound);
    }
    /* Copy across all the bound configurations. */
    F_CLR(constituent, WT_CURSTD_BOUND_ALL);
    F_SET(constituent, F_MASK(base_cursor, WT_CURSTD_BOUND_ALL));
    return (0);
}

/*
 * __clayered_copy_bounds --
 *     A method for copying (or clearing) bounds on constituent cursors within a layered cursor
 */
static int
__clayered_copy_bounds(WT_CURSOR_LAYERED *clayered)
{
    u_int i;

    if (clayered->ingest_cursors != NULL)
        for (i = 0; i < clayered->n_ingest_cursors; i++)
            WT_RET(__clayered_copy_constituent_bound(clayered, clayered->ingest_cursors[i]));
    WT_RET(__clayered_copy_constituent_bound(clayered, clayered->stable_cursor));
    return (0);
}

/*
 * __clayered_bound --
 *     WT_CURSOR->bound method for the layered cursor type.
 */
static int
__clayered_bound(WT_CURSOR *cursor, const char *config)
{
    WT_COLLATOR *collator;
    WT_CURSOR_LAYERED *clayered;
    WT_DECL_CONF(WT_CURSOR, bound, conf);
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    clayered = (WT_CURSOR_LAYERED *)cursor;

    /*
     * The bound interface operates on an unpositioned cursor, so skip entering the layered cursor
     * for this API.
     */
    CURSOR_API_CALL(cursor, session, ret, bound, clayered->dhandle);

    WT_ERR(__wt_conf_compile_api_call(session, WT_CONFIG_REF(session, WT_CURSOR_bound),
      WT_CONFIG_ENTRY_WT_CURSOR_bound, config, &_conf, sizeof(_conf), &conf));

    __clayered_get_collator(clayered, &collator);
    /* Setup bounds on this top level cursor */
    WT_ERR(__wti_cursor_bound(cursor, conf, collator));

    /*
     * Copy those bounds into the constituents. Note that the constituent cursors may not be open
     * yet, and that would be fine, the layered cursor open interface handles setting up configured
     * bounds as well.
     */
    WT_ERR(__clayered_copy_bounds(clayered));

err:
    if (ret != 0) {
        /* Free any bounds we set on the top level cursor before the error */
        if (F_ISSET(cursor, WT_CURSTD_BOUND_UPPER)) {
            __wt_buf_free(session, &cursor->upper_bound);
            WT_CLEAR(cursor->upper_bound);
        }
        if (F_ISSET(cursor, WT_CURSTD_BOUND_LOWER)) {
            __wt_buf_free(session, &cursor->lower_bound);
            WT_CLEAR(cursor->lower_bound);
        }
        F_CLR(cursor, WT_CURSTD_BOUND_ALL);
        /* Ensure the bounds are cleaned up on any constituents */
        WT_TRET(__clayered_copy_bounds(clayered));
    }
    API_END_RET(session, ret);
}

/*
 * __clayered_cache --
 *     WT_CURSOR->cache method for the layered cursor type.
 */
static int
__clayered_cache(WT_CURSOR *cursor)
{
    WT_CURSOR_LAYERED *clayered;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    clayered = (WT_CURSOR_LAYERED *)cursor;
    session = CUR2S(cursor);

    WT_TRET(__wti_cursor_cache(cursor, clayered->dhandle));
    WT_TRET(__wt_session_release_dhandle(session));

    API_RET_STAT(session, ret, cursor_cache);
}

/*
 * __clayered_reopen_int --
 *     Helper for __clayered_reopen, called with the session data handle set.
 */
static int
__clayered_reopen_int(WT_CURSOR *cursor)
{
    WT_DATA_HANDLE *dhandle;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;
    bool is_dead;

    session = CUR2S(cursor);
    dhandle = session->dhandle;

    /*
     * Lock the handle: we're only interested in open handles, any other state disqualifies the
     * cache.
     */
    ret = __wt_session_lock_dhandle(session, 0, &is_dead);
    if (!is_dead && ret == 0 && !WT_DHANDLE_CAN_REOPEN(dhandle)) {
        WT_RET(__wt_session_release_dhandle(session));
        ret = __wt_set_return(session, EBUSY);
    }

    /*
     * The data handle may not be available, fail the reopen, and flag the cursor so that the handle
     * won't be unlocked when subsequently closed.
     */
    if (is_dead || ret == EBUSY) {
        F_SET(cursor, WT_CURSTD_DEAD);
        ret = WT_NOTFOUND;
    }
    __wti_cursor_reopen(cursor, dhandle);

    /*
     * The layered handle may have been reopened since we last accessed it. Reset fields in the
     * cursor that point to memory owned by the handle.
     */
    if (ret == 0) {
        WT_LAYERED_TABLE *layered = (WT_LAYERED_TABLE *)session->dhandle;
        cursor->internal_uri = session->dhandle->name;
        cursor->key_format = layered->key_format;
        cursor->value_format = layered->value_format;

        WT_STAT_CONN_DSRC_INCR(session, cursor_reopen);
    }
    return (ret);
}

/*
 * __clayered_reopen --
 *     WT_CURSOR->reopen method for the layered cursor type.
 */
static int
__clayered_reopen(WT_CURSOR *cursor, bool sweep_check_only)
{
    WT_DATA_HANDLE *dhandle;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;
    bool can_sweep;

    session = CUR2S(cursor);
    dhandle = ((WT_CURSOR_LAYERED *)cursor)->dhandle;

    if (sweep_check_only) {
        /*
         * The sweep check returns WT_NOTFOUND if the cursor should be swept. Generally if the
         * associated data handle cannot be reopened it should be swept. But a handle being operated
         * on by this thread should not be swept. The situation where a handle cannot be reopened
         * but also cannot be swept can occur if this thread is in the middle of closing a cursor
         * for a handle that is marked as dropped. During the close, a few iterations of the session
         * cursor sweep are run. The sweep calls this function to see if a cursor should be swept,
         * and it may thus be asking about the very cursor being closed.
         */
        can_sweep = !WT_DHANDLE_CAN_REOPEN(dhandle) && dhandle != session->dhandle;
        return (can_sweep ? WT_NOTFOUND : 0);
    }

    /*
     * Temporarily set the session's data handle to the data handle in the cursor. Reopen may be
     * called either as part of an open API call, or during cursor sweep as part of a different API
     * call, so we need to restore the original data handle that was in our session after the reopen
     * completes.
     */
    WT_WITH_DHANDLE(session, dhandle, ret = __clayered_reopen_int(cursor));
    API_RET_STAT(session, ret, cursor_reopen);
}

/*
 * __clayered_lookup_constituent --
 *     The cursor-agnostic parts of layered table lookups.
 */
static int
__clayered_lookup_constituent(WT_CURSOR *c, WT_CURSOR_LAYERED *clayered, WT_ITEM *value)
{
    WT_CURSOR *cursor;
    WT_DECL_RET;

    cursor = &clayered->iface;

    c->set_key(c, &cursor->key);
    if ((ret = c->search(c)) == 0) {
        WT_RET(c->get_key(c, &cursor->key));
        WT_RET(c->get_value(c, value));
        clayered->current_cursor = c;
    }

    return (ret);
}

/*
 * __clayered_lookup --
 *     Position a layered cursor.
 */
static int
__clayered_lookup(WT_SESSION_IMPL *session, WT_CURSOR_LAYERED *clayered, WT_ITEM *value)
{
    WT_CONNECTION_IMPL *conn;
    WT_CURSOR *c, *cursor;
    WT_DECL_RET;
    bool found, reset_ignore_prepare;

    c = NULL;
    conn = S2C(session);
    cursor = &clayered->iface;
    found = false;
    reset_ignore_prepare = false;

    if (!conn->disagg_layered_leader) {
        int ing_i;

        for (ing_i = (int)clayered->n_ingest_cursors - 1; ing_i >= 0; ing_i--) {
            c = clayered->ingest_cursors[(u_int)ing_i];
            WT_ERR_NOTFOUND_OK(__clayered_lookup_constituent(c, clayered, value), true);
            if (ret == 0) {
                found = true;
                if (__wt_clayered_deleted(value))
                    ret = WT_NOTFOUND;
                break;
            }
        }
    } else {
        /* Be sure we'll make a search attempt further down.  */
        WT_ASSERT(
          session, F_ISSET(clayered, WT_CLAYERED_OPEN_READ) && clayered->stable_cursor != NULL);
    }

    /*
     * If the key didn't exist in the ingest constituent and the cursor is setup for reading, check
     * the stable constituent.
     */
    if (!found && F_ISSET(clayered, WT_CLAYERED_OPEN_READ) && clayered->stable_cursor != NULL) {
        c = clayered->stable_cursor;
        /*
         * Temporarily set ignore prepared flag when searching for update in the stable cursor. In
         * disaggregated storage, the stable table may contain prepared updates that is rolled back
         * on ingest table. If reading this prepared update on stable table, it will cause prepared
         * conflict issue. Therefore for layered cursor operations, we need to ignore these prepared
         * updates to allow reading through to committed data.
         */
        if (!conn->disagg_layered_leader && !F_ISSET(session->txn, WT_TXN_IGNORE_PREPARE)) {
            reset_ignore_prepare = true;
            F_SET(session->txn, WT_TXN_IGNORE_PREPARE);
        }
        WT_ERR_NOTFOUND_OK(__clayered_lookup_constituent(c, clayered, value), true);
        if (ret == 0)
            found = true;
    }

    if (!found)
        F_CLR(c, WT_CURSTD_KEY_SET);

err:
    if (reset_ignore_prepare)
        F_CLR(session->txn, WT_TXN_IGNORE_PREPARE);
    if (ret == 0) {
        F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);
        F_SET(cursor, WT_CURSTD_KEY_INT);
        clayered->current_cursor = c;

        if (value == &cursor->value)
            F_SET(cursor, WT_CURSTD_VALUE_INT);
    } else if (ret != WT_PREPARE_CONFLICT)
        WT_TRET(__clayered_reset_cursors(clayered, false));

    return (ret);
}

/*
 * __clayered_search --
 *     WT_CURSOR->search method for the layered cursor type.
 */
static int
__clayered_search(WT_CURSOR *cursor)
{
    WT_CURSOR_LAYERED *clayered;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    clayered = (WT_CURSOR_LAYERED *)cursor;

    CURSOR_API_CALL(cursor, session, ret, search, clayered->dhandle);
    WT_ERR(__cursor_needkey(cursor));
    __cursor_novalue(cursor);
    WT_ERR(__clayered_enter(clayered, true, false, false));
    F_CLR(clayered, WT_CLAYERED_ITERATE_NEXT | WT_CLAYERED_ITERATE_PREV);

    ret = __clayered_lookup(session, clayered, &cursor->value);

    WT_STAT_CONN_DSRC_INCR(session, layered_curs_search);
    WT_ERR(__clayered_lookup(session, clayered, &cursor->value));
    WT_ITEM_SET(cursor->key, clayered->current_cursor->key);
    if (__clayered_cursor_is_ingest(clayered, clayered->current_cursor))
        WT_STAT_CONN_DSRC_INCR(session, layered_curs_search_ingest);
    else
        WT_STAT_CONN_DSRC_INCR(session, layered_curs_search_stable);

err:
    __clayered_leave(clayered);
    if (ret == 0)
        __clayered_deleted_decode(&cursor->value);
    API_END_RET(session, ret);
}

/*
 * __clayered_search_near --
 *     WT_CURSOR->search_near method for the layered cursor type.
 */
static int
__clayered_search_near_move_ingest_to_opposite_side(WT_SESSION_IMPL *session,
  WT_CURSOR_LAYERED *clayered, WT_CURSOR *ingest_cursor, int stable_cmp, int *ingest_cmp)
{
    WT_COLLATOR *collator;
    WT_CURSOR *cursor;
    WT_DECL_RET;

    cursor = &clayered->iface;

    __clayered_get_collator(clayered, &collator);
    /*
     * When reading with read-uncommitted isolation, concurrent key insertions may occur. Continue
     * the walk until the search key is reached or passed.
     */
    if (stable_cmp > 0) {
        /* Stable is larger. Move ingest forward to find a larger key in ingest. */
        do {
            WT_ERR_NOTFOUND_OK(ingest_cursor->next(ingest_cursor), true);

            if (session->txn->isolation != WT_ISO_READ_UNCOMMITTED) {
                *ingest_cmp = stable_cmp;
                break;
            }

            if (ret == 0)
                WT_ERR(
                  __wt_compare(session, collator, &ingest_cursor->key, &cursor->key, ingest_cmp));
        } while (ret == 0 && *ingest_cmp < 0);
    } else {
        /* Stable is smaller. Move ingest backward to find a smaller key in ingest. */
        do {
            WT_ERR_NOTFOUND_OK(ingest_cursor->prev(ingest_cursor), true);

            if (session->txn->isolation != WT_ISO_READ_UNCOMMITTED) {
                *ingest_cmp = stable_cmp;
                break;
            }

            if (ret == 0)
                WT_ERR(
                  __wt_compare(session, collator, &ingest_cursor->key, &cursor->key, ingest_cmp));
        } while (ret == 0 && *ingest_cmp > 0);
    }
err:
    return (ret);
}

/*
 * __clayered_search_near_int --
 *     search near method for the layered cursor type.
 */
static int
__clayered_search_near_int(WT_SESSION_IMPL *session, WT_CURSOR *cursor, int *exactp)
{
    WT_COLLATOR *collator;
    WT_CURSOR *best, *c, *closest;
    WT_CURSOR_LAYERED *clayered;
    WT_DECL_RET;
    bool *ingest_del = NULL, *ingest_found = NULL;
    int *ingest_cmp = NULL;
    int best_pri;
    int cmp, closest_cmp, stable_cmp;
    bool any_ingest_found, deleted, stable_found;
    u_int i, n;

    clayered = (WT_CURSOR_LAYERED *)cursor;
    n = clayered->n_ingest_cursors;
    closest = NULL;
    closest_cmp = 0;
    stable_cmp = 0;
    stable_found = false;
    any_ingest_found = false;

    WT_ERR(__wt_calloc(session, n, sizeof(int), &ingest_cmp));
    WT_ERR(__wt_calloc(session, n, sizeof(bool), &ingest_found));
    WT_ERR(__wt_calloc(session, n, sizeof(bool), &ingest_del));

    /*
     * FIXME-WT-16810: In leader mode, skip searching ingest as it should be empty.
     */
    if (!clayered->leader) {
        for (i = 0; i < n; i++) {
            c = clayered->ingest_cursors[i];
            c->set_key(c, &cursor->key);
            WT_ERR_NOTFOUND_OK(c->search_near(c, &ingest_cmp[i]), true);
            ingest_found[i] = (ret == 0);
            if (ret == WT_NOTFOUND)
                ret = 0;
            if (ingest_found[i]) {
                any_ingest_found = true;
                ingest_del[i] = __wt_clayered_deleted(&c->value);
            }
        }
    }

    /*
     * Skip stable search_near when the newest ingest has an exact, non-deleted match: the visible
     * row is entirely determined by that ingest, and positioning stable can trigger bogus
     * opposite-side realignment that moves the ingest off its exact key (see layered search_near
     * tests with split stable/ingest keys).
     */
    if (clayered->stable_cursor != NULL) {
        bool skip_stable;

        skip_stable = false;
        if (!clayered->leader && n > 0 && ingest_found[n - 1] && ingest_cmp[n - 1] == 0 &&
          !ingest_del[n - 1])
            skip_stable = true;
        if (!skip_stable) {
            clayered->stable_cursor->set_key(clayered->stable_cursor, &cursor->key);
            WT_ERR_NOTFOUND_OK(
              clayered->stable_cursor->search_near(clayered->stable_cursor, &stable_cmp), true);
            stable_found = (ret == 0);
            if (ret == WT_NOTFOUND)
                ret = 0;
        }
    }

    if (!any_ingest_found && !stable_found) {
        ret = WT_NOTFOUND;
        goto err;
    }

    __clayered_get_collator(clayered, &collator);

    /*
     * Align ingests on the opposite side of the search key from stable. Never adjust an ingest that
     * already has an exact match (cmp == 0): the old two-cursor code only ran this branch when
     * neither constituent had an exact match.
     */
    if (stable_found) {
        for (i = 0; i < n; i++) {
            if (!ingest_found[i])
                continue;
            if (ingest_cmp[i] == 0)
                continue;
            if ((ingest_cmp[i] ^ stable_cmp) >= 0)
                continue;
            WT_ERR_NOTFOUND_OK(__clayered_search_near_move_ingest_to_opposite_side(session,
                                 clayered, clayered->ingest_cursors[i], stable_cmp, &ingest_cmp[i]),
              true);
            if (ret == WT_NOTFOUND) {
                ret = 0;
                ingest_found[i] = false;
            } else {
                c = clayered->ingest_cursors[i];
                ingest_del[i] = __wt_clayered_deleted(&c->value);
            }
        }
    }

    /*
     * 1) Any ingest with an exact key match  prefer the newest (highest index). This includes
     * tombstones so delete masking and iterate-forward/prev behavior match the pre-merge code.
     */
    for (i = n; i > 0;) {
        --i;
        if (!ingest_found[i] || ingest_cmp[i] != 0)
            continue;
        closest = clayered->ingest_cursors[i];
        closest_cmp = 0;
        goto chose;
    }

    /* 2) Exact on stable when no ingest is positioned exactly on the search key. */
    if (stable_found && stable_cmp == 0) {
        closest = clayered->stable_cursor;
        closest_cmp = 0;
        goto chose;
    }

    /* 3) Prefer a key strictly larger than the search term; pick the smallest such key. */
    best = NULL;
    best_pri = -2;
    if (stable_found && stable_cmp > 0) {
        best = clayered->stable_cursor;
        best_pri = -1;
        closest_cmp = stable_cmp;
    }
    for (i = 0; i < n; i++) {
        if (!ingest_found[i] || ingest_cmp[i] <= 0)
            continue;
        c = clayered->ingest_cursors[i];
        if (best == NULL) {
            best = c;
            best_pri = (int)i;
            closest_cmp = ingest_cmp[i];
            continue;
        }
        WT_ERR(__wt_compare(session, collator, &c->key, &best->key, &cmp));
        if (cmp < 0 || (cmp == 0 && (int)i > best_pri)) {
            best = c;
            best_pri = (int)i;
            closest_cmp = ingest_cmp[i];
        }
    }
    if (best != NULL) {
        closest = best;
        goto chose;
    }

    /* 4) Otherwise use a key strictly smaller than the search term; pick the largest such key. */
    best = NULL;
    best_pri = -2;
    if (stable_found && stable_cmp < 0) {
        best = clayered->stable_cursor;
        best_pri = -1;
        closest_cmp = stable_cmp;
    }
    for (i = 0; i < n; i++) {
        if (!ingest_found[i] || ingest_cmp[i] >= 0)
            continue;
        c = clayered->ingest_cursors[i];
        if (best == NULL) {
            best = c;
            best_pri = (int)i;
            closest_cmp = ingest_cmp[i];
            continue;
        }
        WT_ERR(__wt_compare(session, collator, &c->key, &best->key, &cmp));
        if (cmp > 0 || (cmp == 0 && (int)i > best_pri)) {
            best = c;
            best_pri = (int)i;
            closest_cmp = ingest_cmp[i];
        }
    }
    if (best != NULL) {
        closest = best;
        goto chose;
    }

    ret = WT_NOTFOUND;
    goto err;

chose:
    WT_ASSERT_ALWAYS(session, closest != NULL, "Layered search near should have found something");
    clayered->current_cursor = closest;

    cmp = closest_cmp;
    if (closest == clayered->stable_cursor)
        goto done;

    deleted = __wt_clayered_deleted(&closest->value);
    if (deleted) {
        WT_ASSERT(session, !F_ISSET(&clayered->iface, WT_CURSTD_KEY_INT));
        if ((ret = __clayered_iterate(clayered, WT_CLAYERED_ITERATE_NEXT)) == 0) {
            cmp = 1;
            deleted = false;
        }
        WT_ERR_NOTFOUND_OK(ret, false);
    }
    WT_ERR_NOTFOUND_OK(ret, false);

    if (deleted) {
        clayered->current_cursor = NULL;
        WT_ERR(__layered_prev(cursor));
        cmp = -1;
    }
    if (exactp != NULL)
        *exactp = cmp;

    if (!F_ISSET(clayered, WT_CLAYERED_ITERATE_NEXT | WT_CLAYERED_ITERATE_PREV)) {
        if (clayered->stable_cursor != NULL && clayered->current_cursor != clayered->stable_cursor)
            WT_ERR(clayered->stable_cursor->reset(clayered->stable_cursor));
        if (__clayered_cursor_is_ingest(clayered, clayered->current_cursor)) {
            for (i = 0; i < n; i++) {
                c = clayered->ingest_cursors[i];
                if (c != clayered->current_cursor)
                    WT_ERR(c->reset(c));
            }
        } else
            for (i = 0; i < n; i++)
                WT_ERR(clayered->ingest_cursors[i]->reset(clayered->ingest_cursors[i]));
    }

err:
    __wt_free(session, ingest_cmp);
    __wt_free(session, ingest_found);
    __wt_free(session, ingest_del);
    if (ret != 0 && ret != WT_PREPARE_CONFLICT)
        WT_TRET(__clayered_reset_cursors(clayered, false));

    return (ret);
}

/*
 * __clayered_search_near --
 *     WT_CURSOR->search_near method for the layered cursor type.
 */
static int
__clayered_search_near(WT_CURSOR *cursor, int *exactp)
{
    WT_CURSOR_LAYERED *clayered;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    clayered = (WT_CURSOR_LAYERED *)cursor;

    CURSOR_API_CALL(cursor, session, ret, search_near, clayered->dhandle);
    F_CLR(clayered, WT_CLAYERED_ITERATE_NEXT | WT_CLAYERED_ITERATE_PREV);
    WT_ERR(__cursor_copy_release(cursor));
    WT_ERR(__cursor_needkey(cursor));
    __cursor_novalue(cursor);
    WT_ERR(__clayered_enter(clayered, true, true, false));

    WT_ERR(__clayered_search_near_int(session, cursor, exactp));

    WT_ITEM_SET(cursor->key, clayered->current_cursor->key);
    WT_ITEM_SET(cursor->value, clayered->current_cursor->value);

    WT_STAT_CONN_DSRC_INCR(session, layered_curs_search_near);
    /* FIXME-WT-15545: Handle the case of current_cursor being NULL */
    if (__clayered_cursor_is_ingest(clayered, clayered->current_cursor))
        WT_STAT_CONN_DSRC_INCR(session, layered_curs_search_near_ingest);
    else
        WT_STAT_CONN_DSRC_INCR(session, layered_curs_search_near_stable);

err:
    __clayered_leave(clayered);
    if (closest != NULL)
        WT_TRET(closest->reset(closest));

    if (ret == 0) {
        F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);
        F_SET(cursor, WT_CURSTD_KEY_INT | WT_CURSTD_VALUE_INT);
    } else if (ret != WT_PREPARE_CONFLICT) {
        F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);
        clayered->current_cursor = NULL;
    }

    API_END_RET(session, ret);
}

/*
 * __clayered_put --
 *     Put an entry into the desired tree.
 */
static WT_INLINE int
__clayered_put(WT_SESSION_IMPL *session, WT_CURSOR_LAYERED *clayered, const WT_ITEM *key,
  const WT_ITEM *value, bool position, bool reserve)
{
    WT_CONNECTION_IMPL *conn;
    WT_CURSOR *c;
    int (*func)(WT_CURSOR *);

    conn = S2C(session);

    if (conn->disagg_layered_leader)
        c = clayered->stable_cursor;
    else {
        /*
         * Followers write into ingest tables. Optionally rotate the active ingest chunk based on a
         * simple operation count threshold.
         */
        if (conn->disaggregated_storage.layered_ingest_chunk_max_ops != 0) {
            uint64_t max_ops = conn->disaggregated_storage.layered_ingest_chunk_max_ops;
            uint64_t ops = __wt_atomic_add_uint64_relaxed(
              &conn->disaggregated_storage.layered_ingest_chunk_ops, 1);
            if (ops >= max_ops &&
              __wt_atomic_cas_uint64(
                &conn->disaggregated_storage.layered_ingest_chunk_ops, ops, 0)) {
                int rr;

                rr = __clayered_rollover_ingest(session, clayered);
                if (rr != 0) {
                    /*
                     * Rollover lost the threshold counter when the CAS won; restore roughly one
                     * below the threshold so a transient failure does not permanently suppress
                     * rotation.
                     */
                    if (ops > 0)
                        (void)__wt_atomic_add_uint64_relaxed(
                          &conn->disaggregated_storage.layered_ingest_chunk_ops, ops - 1);
                    WT_RET(rr);
                }
            }
        }

        c = __clayered_primary_ingest(clayered);

    c->set_key(c, key);
    func = c->insert;
    if (position)
        func = reserve ? c->reserve : c->update;
    if (func != c->reserve)
        c->set_value(c, value);
    WT_RET(func(c));

    /* If necessary, set the position for future scans. */
    if (position)
        clayered->current_cursor = c;

    return (0);
}

/*
 * __clayered_rollover_ingest --
 *     Follower-only: create a new ingest chunk for a layered table and switch subsequent writes to
 *     the newest ingest.
 */
static int
__clayered_rollover_ingest(WT_SESSION_IMPL *session, WT_CURSOR_LAYERED *clayered)
{
    WT_CONFIG_ITEM key_format, value_format;
    WT_DECL_ITEM(ingest_cfg);
    WT_DECL_ITEM(ingest_list);
    WT_DECL_ITEM(layered_update);
    WT_DECL_ITEM(new_uri_buf);
    WT_DECL_RET;
    WT_LAYERED_TABLE *layered;
    WT_SESSION_IMPL *int_session;
    uint32_t *ids_new;
    uint32_t new_id;
    uint32_t new_n;
    char *dup_meta, *layered_meta, *merged, *new_uri;
    char **uris_new;
    const char *drop_cfg[4];
    const char *layered_uri;
    bool file_created, hold_chunk_lock, meta_updated;

    int_session = NULL;
    uris_new = NULL;
    ids_new = NULL;
    new_id = 0;
    dup_meta = layered_meta = merged = new_uri = NULL;
    layered_uri = clayered->dhandle->name;
    layered = (WT_LAYERED_TABLE *)clayered->dhandle;
    file_created = hold_chunk_lock = meta_updated = false;
    new_n = 0;

    /* Leaders do not use ingest tables for writes. */
    WT_ASSERT(session, !S2C(session)->disagg_layered_leader);
    WT_ASSERT(session, layered->n_ingest_uris > 0);

    if (!WT_PREFIX_MATCH(layered_uri, "layered:"))
        WT_RET_MSG(session, EINVAL, "layered ingest rollover requires a layered: metadata URI");

    if (layered->n_ingest_uris >= WT_LAYERED_INGEST_CHUNKS_MAX)
        WT_RET_MSG(session, ENOSPC, "layered table \"%s\" already has maximum ingest chunks (%u)",
          layered_uri, (unsigned int)WT_LAYERED_INGEST_CHUNKS_MAX);

    __wt_spin_lock(session, &layered->ingest_chunk_lock);
    hold_chunk_lock = true;

    /*
     * Under the chunk lock, another thread may have already rotated; re-check the cap and bail out
     * quietly so the caller can continue with the current primary ingest.
     */
    if (layered->n_ingest_uris >= WT_LAYERED_INGEST_CHUNKS_MAX) {
        ret = 0;
        goto err;
    }

    WT_ERR(__wt_schema_open_internal_session(session, &int_session));

    /*
     * Derive a new ingest URI: file:<base>.<next_suffix>.wt_ingest (see layered create naming).
     *
     * The next suffix must be monotonically greater than any suffix currently or previously in use
     * for this layered table. The primary (newest) ingest chunk always holds the highest-known
     * suffix, even after the ingest GC retires older chunks from the front of the list, so parsing
     * the primary's suffix and incrementing it is a safe, collision-free generator. Using
     * n_ingest_uris directly would collide after GC shortens the list (e.g. list is [foo.2, foo.3],
     * n=2, but foo.2.wt_ingest is still in metadata).
     */
    {
        const char *pfx;
        const char *primary = WT_LAYERED_PRIMARY_INGEST_URI(layered);
        size_t plen, base_len, digits_end, digits_start, j;
        uint32_t cur_suffix, next_suffix;

        WT_ASSERT(session, WT_PREFIX_MATCH(primary, "file:"));
        pfx = primary + strlen("file:");
        plen = strlen(pfx);
        WT_ASSERT(session, plen > strlen(".wt_ingest") && WT_SUFFIX_MATCH(pfx, ".wt_ingest"));
        base_len = plen - strlen(".wt_ingest");

        /* Parse the trailing ".<digits>" suffix of the primary, if any. */
        cur_suffix = 0;
        digits_end = base_len;
        digits_start = digits_end;
        while (digits_start > 0 && pfx[digits_start - 1] >= '0' && pfx[digits_start - 1] <= '9')
            --digits_start;
        if (digits_start < digits_end && digits_start > 0 && pfx[digits_start - 1] == '.') {
            for (j = digits_start; j < digits_end; j++)
                cur_suffix = cur_suffix * 10u + (uint32_t)(pfx[j] - '0');
            base_len = digits_start - 1;
        }
        next_suffix = cur_suffix + 1;

        new_n = layered->n_ingest_uris + 1;
        WT_ERR(__wt_scr_alloc(int_session, 0, &new_uri_buf));
        WT_ERR(__wt_buf_fmt(
          int_session, new_uri_buf, "file:%.*s.%u.wt_ingest", (int)base_len, pfx, next_suffix));
        /*
         * Persist the new URI using the application session: ingest URI strings live on the layered
         * dhandle for the handle lifetime and must not be allocated on a short-lived internal
         * schema session.
         */
        WT_ERR(__wt_strndup(session, new_uri_buf->data, new_uri_buf->size, &new_uri));
    }

    /* Fail fast if metadata already knows about this ingest file. */
    dup_meta = NULL;
    WT_ERR_NOTFOUND_OK(__wt_metadata_search(int_session, new_uri, &dup_meta), false);
    if (dup_meta != NULL) {
        __wt_free(int_session, dup_meta);
        dup_meta = NULL;
        WT_ERR_MSG(session, EEXIST, "ingest chunk URI already exists in metadata: %s", new_uri);
    }

    /* Create the new ingest table using the layered table's key/value formats. */
    WT_ERR(__wt_config_gets(int_session, clayered->dhandle->cfg, "key_format", &key_format));
    WT_ERR(__wt_config_gets(int_session, clayered->dhandle->cfg, "value_format", &value_format));
    WT_ERR(__wt_scr_alloc(int_session, 0, &ingest_cfg));
    WT_ERR(__wt_buf_fmt(int_session, ingest_cfg,
      "key_format=\"%.*s\",value_format=\"%.*s\","
      "block_manager=default,in_memory=true,log=(enabled=false),disaggregated=(page_log=none),"
      "memory_page_max=10TB,cache_resident=true",
      (int)key_format.len, key_format.str, (int)value_format.len, value_format.str));
    WT_WITH_SCHEMA_LOCK(
      int_session, ret = __wt_schema_create(int_session, new_uri, ingest_cfg->data));
    WT_ERR(ret);
    file_created = true;

    /*
     * Persist the expanded ingest list before mutating the in-memory handle so a failure after
     * updating metadata does not strand a new btree without a metadata entry, and a failure before
     * updating in-memory state can roll back metadata to the previous value.
     */
    WT_ERR(__wt_metadata_search(int_session, layered_uri, &layered_meta));
    WT_ERR(__wt_scr_alloc(int_session, 0, &ingest_list));
    WT_ERR(__wt_scr_alloc(int_session, 0, &layered_update));
    {
        uint32_t i;

        WT_ERR(__wt_buf_fmt(int_session, ingest_list, "("));
        for (i = 0; i < layered->n_ingest_uris; i++)
            WT_ERR(__wt_buf_catfmt(
              int_session, ingest_list, "%s%s", i == 0 ? "" : ",", layered->ingest_uris[i]));
        WT_ERR(__wt_buf_catfmt(int_session, ingest_list, ",%s", new_uri));
        WT_ERR(__wt_buf_catfmt(int_session, ingest_list, ")"));
        WT_ERR(__wt_buf_fmt(int_session, layered_update, "ingest=\"%.*s\"", (int)ingest_list->size,
          (const char *)ingest_list->data));
    }
    {
        const char *cfg[4];

        cfg[0] = layered_meta;
        cfg[1] = layered_update->data;
        cfg[2] = NULL;
        cfg[3] = NULL;
        WT_ERR(__wt_config_collapse(int_session, cfg, &merged));
        WT_ERR(__wt_metadata_insert(int_session, layered_uri, merged));
        meta_updated = true;
        __wt_free(int_session, merged);
        merged = NULL;
    }

    /*
     * Update the in-memory layered handle ingest list (and record the btree id for the new ingest).
     * All allocations tied to the layered dhandle use the application session.
     */
    WT_WITH_TABLE_WRITE_LOCK(int_session, {
        uint32_t i;
        WT_BTREE *btree;

        ret = __wt_calloc(session, (size_t)new_n, sizeof(char *), &uris_new);
        if (ret == 0)
            ret = __wt_calloc(session, (size_t)new_n, sizeof(uint32_t), &ids_new);
        if (ret == 0) {
            for (i = 0; i < layered->n_ingest_uris; i++) {
                uris_new[i] = layered->ingest_uris[i];
                ids_new[i] = layered->ingest_btree_ids[i];
            }
            uris_new[new_n - 1] = new_uri;
            new_uri = NULL;
        }
        if (ret == 0)
            ret = __wt_session_get_dhandle(int_session, uris_new[new_n - 1], NULL, NULL, 0);
        if (ret == 0) {
            btree = (WT_BTREE *)int_session->dhandle->handle;
            new_id = btree->id;
            ret = __wt_session_release_dhandle(int_session);
        }
        if (ret == 0) {
            ids_new[new_n - 1] = new_id;

            __wt_free(session, layered->ingest_uris);
            __wt_free(session, layered->ingest_btree_ids);
            layered->ingest_uris = uris_new;
            layered->ingest_btree_ids = ids_new;
            layered->n_ingest_uris = new_n;
            uris_new = NULL;
            ids_new = NULL;
        }
    });
    WT_ERR(ret);

    /*
     * Ensure this cursor starts using the updated ingest list. Do not hold the chunk lock across
     * cursor open/close (may recurse into __clayered_open_cursors).
     */
    __wt_spin_unlock(session, &layered->ingest_chunk_lock);
    hold_chunk_lock = false;
    WT_ERR(__clayered_close_cursors(clayered));
    WT_ERR(__clayered_open_cursors(session, clayered));

    WT_STAT_CONN_DSRC_INCR(session, layered_ingest_chunks_rolled);

err:
    if (ret != 0 && int_session != NULL && meta_updated && layered_meta != NULL)
        WT_WITH_SCHEMA_LOCK(
          int_session, WT_TRET(__wt_metadata_insert(int_session, layered_uri, layered_meta)));
    if (ret != 0 && int_session != NULL && file_created && new_uri != NULL) {
        drop_cfg[0] = WT_CONFIG_BASE(int_session, WT_SESSION_drop);
        drop_cfg[1] = "force=true";
        drop_cfg[2] = NULL;
        drop_cfg[3] = NULL;
        WT_WITH_SCHEMA_LOCK(
          int_session, WT_TRET(__wt_schema_drop(int_session, new_uri, drop_cfg, false)));
    }

    __wt_free(int_session, layered_meta);
    __wt_free(session, new_uri);
    if (ret != 0 && uris_new != NULL && uris_new != layered->ingest_uris) {
        uint32_t i;
        bool owned;

        if (new_n > 0 && uris_new[new_n - 1] != NULL) {
            owned = true;
            for (i = 0; i < layered->n_ingest_uris; i++)
                if (uris_new[new_n - 1] == layered->ingest_uris[i]) {
                    owned = false;
                    break;
                }
            if (owned)
                __wt_free(session, uris_new[new_n - 1]);
        }
        __wt_free(session, uris_new);
        __wt_free(session, ids_new);
    }

    __wt_scr_free(int_session, &ingest_cfg);
    __wt_scr_free(int_session, &ingest_list);
    __wt_scr_free(int_session, &layered_update);
    __wt_scr_free(int_session, &new_uri_buf);
    WT_TRET(__wt_schema_close_internal_session(session, int_session));

    if (hold_chunk_lock)
        __wt_spin_unlock(session, &layered->ingest_chunk_lock);
    return (ret);
}

/*
 * __clayered_remove_follower --
 *     Remove an entry from the ingest table.
 */
static WT_INLINE int
__clayered_remove_follower(
  WT_SESSION_IMPL *session, WT_CURSOR_LAYERED *clayered, const WT_ITEM *key, bool positioned)
{
    WT_CURSOR *const c = __clayered_primary_ingest(clayered);
    WT_DECL_RET;
    WT_ITEM value;

    WT_CLEAR(value);

    if (positioned) {
        if (clayered->current_cursor == c) {
            WT_ITEM value;

            WT_ASSERT(session, F_ISSET(c, WT_CURSTD_KEY_INT));
            /*
             * If we are erasing a record that is already a tombstone, don't write another one: we
             * don't ever want consecutive tombstones on an update chain.
             */
            WT_RET(c->get_value(c, &value));
            if (__wt_clayered_deleted(&value))
                return (WT_NOTFOUND);
        }
    } else
        WT_ASSERT(session, F_ISSET(&clayered->iface, WT_CURSTD_KEY_EXT));

    /* If we are positioned on the stable table, we need to set the key. */
    if (clayered->current_cursor != c) {
        /*
         * Clear the existing cursor position. Don't clear the primary cursor: we're about to use it
         * anyway. No need to do another search if we are already positioned.
         */
        WT_RET(__clayered_reset_cursors(clayered, true));
        c->set_key(c, key);
    }

    c->set_value(c, &__wt_tombstone);
    WT_RET(c->update(c));
    clayered->current_cursor = c;

    return (0);
}

/*
 * __clayered_remove_leader --
 *     Remove an entry from the stable table.
 */
static WT_INLINE int
__clayered_remove_leader(
  WT_SESSION_IMPL *session, WT_CURSOR_LAYERED *clayered, const WT_ITEM *key, bool positioned)
{
    WT_CURSOR *const c = clayered->stable_cursor;

    /* There is no content on the ingest table. We must be positioned on the stable table. */
    if (!positioned) {
        /*
         * Clear the existing cursor position. Don't clear the primary cursor: we're about to use it
         * anyway. We need the cursor still be positioned after the remove. Don't release the cursor
         * if that is the case. Remove only retains the cursor position if it is positioned at the
         * start.
         */
        WT_RET(__clayered_reset_cursors(clayered, true));
        c->set_key(c, key);
    } else
        WT_ASSERT(session, F_ISSET(c, WT_CURSTD_KEY_INT));

    WT_RET(c->remove(c));
    clayered->current_cursor = c;

    return (0);
}

/*
 * __clayered_remove_int --
 *     Remove an entry from the desired tree.
 */
static WT_INLINE int
__clayered_remove_int(
  WT_SESSION_IMPL *session, WT_CURSOR_LAYERED *clayered, const WT_ITEM *key, bool positioned)
{
    return (S2C(session)->disagg_layered_leader ?
        __clayered_remove_leader(session, clayered, key, positioned) :
        __clayered_remove_follower(session, clayered, key, positioned));
}

/*
 * __clayered_copy_duplicate_kv --
 *     Copy the duplicate key value from the constitute cursor.
 */
static int
__clayered_copy_duplicate_kv(WT_CURSOR *cursor)
{
    WT_CURSOR_LAYERED *clayered;
    WT_SESSION_IMPL *session;

    clayered = (WT_CURSOR_LAYERED *)cursor;
    session = CUR2S(cursor);

    WT_ASSERT(session,
      F_ISSET(clayered->current_cursor, WT_CURSTD_KEY_INT) &&
        F_ISSET(clayered->current_cursor, WT_CURSTD_VALUE_INT));
    F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);
    WT_RET(clayered->current_cursor->get_key(clayered->current_cursor, &cursor->key));
    F_SET(cursor, WT_CURSTD_KEY_INT);
    WT_RET(clayered->current_cursor->get_value(clayered->current_cursor, &cursor->value));
    F_SET(cursor, WT_CURSTD_VALUE_INT);
    WT_RET(__wt_cursor_localkey(cursor));
    WT_RET(__cursor_localvalue(cursor));
    WT_RET(clayered->current_cursor->reset(clayered->current_cursor));
    clayered->current_cursor = NULL;

    return (0);
}

/*
 * __clayered_insert --
 *     WT_CURSOR->insert method for the layered cursor type.
 */
static int
__clayered_insert(WT_CURSOR *cursor)
{
    WT_CURSOR_LAYERED *clayered;
    WT_DECL_ITEM(buf);
    WT_DECL_RET;
    WT_ITEM value;
    WT_SESSION_IMPL *session;

    clayered = (WT_CURSOR_LAYERED *)cursor;

    CURSOR_UPDATE_API_CALL(cursor, session, ret, insert, clayered->dhandle);
    WT_ERR(__cursor_needkey(cursor));
    WT_ERR(__cursor_needvalue(cursor));
    WT_ERR(__clayered_enter(clayered, false,
      S2C(session)->disagg_layered_leader || !F_ISSET(clayered, WT_CURSTD_OVERWRITE), false));

    /*
     * It isn't necessary to copy the key out after the lookup in this case because any non-failed
     * lookup results in an error, and a failed lookup leaves the original key intact.
     */
    if (!F_ISSET(cursor, WT_CURSTD_OVERWRITE) &&
      (ret = __clayered_lookup(session, clayered, &value)) != WT_NOTFOUND) {
        if (ret == 0) {
            WT_ERR(__clayered_copy_duplicate_kv(cursor));
            WT_ERR(WT_DUPLICATE_KEY);
        }

        goto err;
    }

    WT_ERR(__clayered_deleted_encode(session, &cursor->value, &value, &buf));
    WT_ERR(__clayered_put(session, clayered, &cursor->key, &value, false, false));

    /*
     * WT_CURSOR.insert doesn't leave the cursor positioned, and the application may want to free
     * the memory used to configure the insert; don't read that memory again (matching the
     * underlying file object cursor insert semantics).
     */
    F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);

    WT_STAT_CONN_DSRC_INCR(session, layered_curs_insert);
err:
    __wt_scr_free(session, &buf);
    __clayered_leave(clayered);
    CURSOR_UPDATE_API_END(session, ret);
    return (ret);
}

/*
 * __clayered_update --
 *     WT_CURSOR->update method for the layered cursor type.
 */
static int
__clayered_update(WT_CURSOR *cursor)
{
    WT_CURSOR_LAYERED *clayered;
    WT_DECL_ITEM(buf);
    WT_DECL_RET;
    WT_ITEM value;
    WT_SESSION_IMPL *session;

    clayered = (WT_CURSOR_LAYERED *)cursor;

    CURSOR_UPDATE_API_CALL(cursor, session, ret, update, clayered->dhandle);
    WT_ERR(__cursor_needkey(cursor));
    WT_ERR(__cursor_needvalue(cursor));
    WT_ERR(__clayered_enter(clayered, false,
      S2C(session)->disagg_layered_leader || !F_ISSET(clayered, WT_CURSTD_OVERWRITE), false));

    if (!F_ISSET(cursor, WT_CURSTD_OVERWRITE)) {
        WT_ERR(__clayered_lookup(session, clayered, &value));
        /*
         * Copy the key out, since the insert resets non-primary chunk cursors which our lookup may
         * have landed on.
         */
        WT_ERR(__cursor_needkey(cursor));
    }
    WT_ERR(__clayered_deleted_encode(session, &cursor->value, &value, &buf));
    WT_ERR(__clayered_put(session, clayered, &cursor->key, &value, true, false));

    /*
     * Set the cursor to reference the internal key/value of the positioned cursor.
     */
    F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);
    WT_ITEM_SET(cursor->key, clayered->current_cursor->key);
    WT_ITEM_SET(cursor->value, clayered->current_cursor->value);
    WT_ASSERT(session, F_MASK(clayered->current_cursor, WT_CURSTD_KEY_SET) == WT_CURSTD_KEY_INT);
    WT_ASSERT(
      session, F_MASK(clayered->current_cursor, WT_CURSTD_VALUE_SET) == WT_CURSTD_VALUE_INT);
    F_SET(cursor, WT_CURSTD_KEY_INT | WT_CURSTD_VALUE_INT);

    WT_STAT_CONN_DSRC_INCR(session, layered_curs_update);

err:
    __wt_scr_free(session, &buf);
    __clayered_leave(clayered);
    CURSOR_UPDATE_API_END(session, ret);
    return (ret);
}

/*
 * __clayered_remove --
 *     WT_CURSOR->remove method for the layered cursor type.
 */
static int
__clayered_remove(WT_CURSOR *cursor)
{
    WT_CURSOR_LAYERED *clayered;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;
    bool positioned;

    clayered = (WT_CURSOR_LAYERED *)cursor;

    /* Remember if the cursor is currently positioned. */
    positioned = F_ISSET(cursor, WT_CURSTD_KEY_INT);

    CURSOR_REMOVE_API_CALL(cursor, session, ret, clayered->dhandle);
    WT_ERR(__cursor_needkey(cursor));
    __cursor_novalue(cursor);

    WT_ERR(__clayered_enter(clayered, false, true, false));
    /*
     * Copy the key out, since the insert resets non-primary chunk cursors which our lookup may have
     * landed on.
     */
    WT_ERR(__cursor_needkey(cursor));
    WT_ERR(__clayered_remove_int(session, clayered, &cursor->key, positioned));

    /*
     * If the cursor was positioned, it stays positioned with a key but no value, otherwise, there's
     * no position, key or value. This isn't just cosmetic, without a reset, iteration on this
     * cursor won't start at the beginning/end of the table.
     */
    F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);
    if (positioned)
        F_SET(cursor, WT_CURSTD_KEY_INT);
    else
        WT_TRET(cursor->reset(cursor));
    WT_STAT_CONN_DSRC_INCR(session, layered_curs_remove);

err:
    __clayered_leave(clayered);
    CURSOR_UPDATE_API_END(session, ret);
    return (ret);
}

/*
 * __clayered_reserve --
 *     WT_CURSOR->reserve method for the layered cursor type.
 */
static int
__clayered_reserve(WT_CURSOR *cursor)
{
    WT_CURSOR_LAYERED *clayered;
    WT_DECL_RET;
    WT_ITEM value;
    WT_SESSION_IMPL *session;
    bool overwrite;

    clayered = (WT_CURSOR_LAYERED *)cursor;
    overwrite = F_ISSET(cursor, WT_CURSTD_OVERWRITE);

    CURSOR_UPDATE_API_CALL(cursor, session, ret, reserve, clayered->dhandle);
    WT_ERR(__cursor_needkey(cursor));
    __cursor_novalue(cursor);
    WT_ERR(__wt_txn_context_check(session, true));

    /* WT_CURSOR.reserve is update-without-overwrite and a special value. */
    F_CLR(cursor, WT_CURSTD_OVERWRITE);
    WT_ERR(__clayered_enter(clayered, false, S2C(session)->disagg_layered_leader, false));
    WT_ERR(__clayered_lookup(session, clayered, &value));
    /*
     * Copy the key out, since the insert resets non-primary chunk cursors which our lookup may have
     * landed on.
     */
    WT_ERR(__cursor_needkey(cursor));
    ret = __clayered_put(session, clayered, &cursor->key, NULL, true, true);

err:
    if (overwrite)
        F_SET(cursor, WT_CURSTD_OVERWRITE);
    __clayered_leave(clayered);
    CURSOR_UPDATE_API_END(session, ret);

    /*
     * The application might do a WT_CURSOR.get_value call when we return, so we need a value and
     * the underlying functions didn't set one up. For various reasons, those functions may not have
     * done a search and any previous value in the cursor might race with WT_CURSOR.reserve (and in
     * cases like layered tables, the reserve never encountered the original key). For simplicity,
     * repeat the search here.
     */
    return (ret == 0 ? cursor->search(cursor) : ret);
}

/*
 * __clayered_largest_key --
 *     WT_CURSOR->largest_key implementation for layered tables.
 */
static int
__clayered_largest_key(WT_CURSOR *cursor)
{
    WT_COLLATOR *collator;
    WT_CURSOR *c, *larger_cursor, *stable_cursor;
    WT_CURSOR_LAYERED *clayered;
    WT_DECL_ITEM(key);
    WT_DECL_RET;
    WT_SESSION_IMPL *session;
    u_int i;
    int cmp, larger_pri;
    bool any_found;

    clayered = (WT_CURSOR_LAYERED *)cursor;
    any_found = false;
    larger_cursor = NULL;
    larger_pri = -2;

    CURSOR_API_CALL(cursor, session, ret, largest_key, clayered->dhandle);
    __cursor_novalue(cursor);
    WT_ERR(__clayered_enter(clayered, false, false, false));

    stable_cursor = clayered->stable_cursor;

    WT_ERR(__wt_scr_alloc(session, 0, &key));

    __clayered_get_collator(clayered, &collator);

    for (i = 0; i < clayered->n_ingest_cursors; i++) {
        c = clayered->ingest_cursors[i];
        WT_ERR_NOTFOUND_OK(c->largest_key(c), true);
        if (ret != 0)
            continue;
        any_found = true;
        if (larger_cursor == NULL) {
            larger_cursor = c;
            larger_pri = (int)i;
            continue;
        }
        WT_ERR(__wt_compare(session, collator, &c->key, &larger_cursor->key, &cmp));
        if (cmp > 0 || (cmp == 0 && (int)i > larger_pri)) {
            larger_cursor = c;
            larger_pri = (int)i;
        }
    }
    ret = 0;

    if (stable_cursor != NULL) {
        WT_ERR_NOTFOUND_OK(stable_cursor->largest_key(stable_cursor), true);
        if (ret == 0) {
            any_found = true;
            c = stable_cursor;
            if (larger_cursor == NULL) {
                larger_cursor = c;
                larger_pri = -1;
            } else {
                WT_ERR(__wt_compare(session, collator, &c->key, &larger_cursor->key, &cmp));
                if (cmp > 0 || (cmp == 0 && larger_pri < 0)) {
                    larger_cursor = c;
                    larger_pri = -1;
                }
            }
        }
        ret = 0;
    }

    if (!any_found) {
        ret = WT_NOTFOUND;
        goto err;
    }

    /* Copy the key as we will reset the cursor after that. */
    WT_ERR(__wt_buf_set(session, key, larger_cursor->key.data, larger_cursor->key.size));
    WT_ERR(cursor->reset(cursor));
    WT_ERR(__wt_buf_set(session, &cursor->key, key->data, key->size));
    /* Set the key as external. */
    F_SET(cursor, WT_CURSTD_KEY_EXT);

err:
    __clayered_leave(clayered);
    __wt_scr_free(session, &key);
    if (ret != 0)
        WT_TRET(cursor->reset(cursor));
    API_END_RET_STAT(session, ret, cursor_largest_key);
}

/*
 * __clayered_close_int --
 *     Close a layered cursor
 */
static int
__clayered_close_int(WT_CURSOR *cursor)
{
    WT_CURSOR_LAYERED *clayered;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;
    bool dead;

    dead = F_ISSET(cursor, WT_CURSTD_DEAD);
    session = CUR2S(cursor);
    WT_ASSERT_ALWAYS(session, session->dhandle->type == WT_DHANDLE_TYPE_LAYERED,
      "Valid layered dhandle is required to close a cursor");
    clayered = (WT_CURSOR_LAYERED *)cursor;

    /*
     * No need to close the constituent cursors if it has been already done during connection->close
     * performing a close of all cursors in the session.
     */
    if (!F_ISSET(cursor, WT_CURSTD_CONSTITUENT_DEAD))
        WT_TRET(__clayered_close_cursors(clayered));

    /* In case we were somehow left positioned, clear that. */
    __clayered_leave(clayered);

    __wt_cursor_close(cursor);

    if (session->dhandle != NULL) {
        /* Decrement the data-source's in-use counter. */
        __wt_cursor_dhandle_decr_use(session);

        /*
         * If the cursor was marked dead, we got here from reopening a cached cursor, which had a
         * handle that was dead at that time, so it did not obtain a lock on the handle.
         */
        if (!dead)
            WT_TRET(__wt_session_release_dhandle(session));
    }
    return (ret);
}

/*
 * __clayered_close --
 *     WT_CURSOR->close method for the layered cursor type.
 */
static int
__clayered_close(WT_CURSOR *cursor)
{
    WT_CURSOR_LAYERED *clayered;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    /*
     * Don't use the normal __clayered_enter path: that is wasted work when closing, and the cursor
     * may never have been used.
     */
    clayered = (WT_CURSOR_LAYERED *)cursor;
    CURSOR_API_CALL_PREPARE_ALLOWED(cursor, session, close, clayered->dhandle);
err:
    if (ret == 0) {
        /*
         * If releasing the cursor fails in any way, it will be left in a state that allows it to be
         * normally closed.
         */
        bool released = false;
        ret = __wti_cursor_cache_release(session, cursor, &released);

        if (released) {
            /*
             * If the cursor has been cached, try to cache the constituent cursors by evoking a
             * cursor close.
             *
             * Note: There no need to close the constituent cursors if it has been already done
             * during connection->close performing a close of all cursors in the session.
             */
            if (!F_ISSET(cursor, WT_CURSTD_CONSTITUENT_DEAD))
                WT_TRET(__clayered_close_cursors(clayered));

            /* In case we were somehow left positioned, clear that. */
            __clayered_leave(clayered);
            goto done;
        }
    }
    /* For cached cursors, free any extra buffers retained now. */
    __wt_cursor_free_cached_memory(cursor);
    cursor->internal_uri = NULL;

    WT_TRET(__clayered_close_int(cursor));
done:
    API_END_RET(session, ret);
}

/*
 * __clayered_next_random --
 *     WT_CURSOR->next_random method for the layered cursor type.
 */
static int
__clayered_next_random(WT_CURSOR *cursor)
{
    WT_CURSOR *c;
    WT_CURSOR_LAYERED *clayered;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;
    int exact;

    c = NULL; /* Workaround for compilers reporting it as used uninitialized. */
    clayered = (WT_CURSOR_LAYERED *)cursor;

    CURSOR_API_CALL(cursor, session, ret, next, clayered->dhandle);
    __cursor_novalue(cursor);
    WT_ERR(__clayered_enter(clayered, false, false, true));

    for (;;) {
        /* FIXME-WT-14736: consider the size of ingest table in the future. */
        if (clayered->stable_cursor != NULL) {
            c = clayered->stable_cursor;
            /*
             * This call to next_random on the layered table can potentially end in WT_NOTFOUND if
             * the layered table is empty. When that happens, use the ingest table.
             */
            WT_ERR_NOTFOUND_OK(__wti_curfile_next_random(c), true);
        } else
            ret = WT_NOTFOUND;

        /* The stable table was either empty or missing. */
        if (ret == WT_NOTFOUND) {
            c = __clayered_primary_ingest(clayered);
            WT_ERR(__wti_curfile_next_random(c));
        }

        F_SET(cursor, WT_CURSTD_KEY_INT);
        WT_ERR(c->get_key(c, &cursor->key));

        /*
         * Search near the current key to resolve any tombstones and position to a valid document.
         * If we see a WT_NOTFOUND here that is valid, as the tree has no documents visible to us.
         */
        WT_ERR(__clayered_search_near(cursor, &exact));
        break;
    }

err:
    __clayered_leave(clayered);
    API_END_RET(session, ret);
}

/*
 * __clayered_modify_leader --
 *     Apply a set of modifications on a leader node.
 */
static int
__clayered_modify_leader(
  WT_SESSION_IMPL *session, WT_CURSOR *cursor, WT_MODIFY *entries, int nentries)
{
    WT_CURSOR_LAYERED *clayered = (WT_CURSOR_LAYERED *)cursor;
    WT_CURSOR *stable = clayered->stable_cursor;

    /* Leaders should always be positioned on the stable table. */
    WT_ASSERT(session, F_ISSET(stable, WT_CURSTD_KEY_INT));

    WT_RET(stable->modify(stable, entries, nentries));

    clayered->current_cursor = stable;

    return (0);
}

/*
 * __clayered_modify_follower --
 *     Apply a set of modifications on a leader node.
 */
static int
__clayered_modify_follower(
  WT_SESSION_IMPL *session, WT_CURSOR *cursor, WT_MODIFY *entries, int nentries)
{
    WT_CURSOR_LAYERED *clayered = (WT_CURSOR_LAYERED *)cursor;
    WT_CURSOR *ingest = __clayered_primary_ingest(clayered);
    WT_DECL_RET;
    WT_DECL_ITEM(buf);
    WT_ITEM value;

    WT_CLEAR(value);

    /* Do a search if we're not positioned. */
    if (!F_ISSET(&clayered->iface, WT_CURSTD_KEY_INT))
        WT_ERR_NOTFOUND_OK(__clayered_lookup(session, clayered, &value), true);
    else
        WT_ITEM_SET(value, cursor->value);

    /* Did the lookup find a value in the ingest table? */
    if (clayered->current_cursor != ingest) {
        /* If not, get the base value from the top-level cursor. */
        ingest->set_key(ingest, &cursor->key);
        ingest->set_value(ingest, &cursor->value);
        WT_RET(__wt_modify_apply_api(ingest, entries, nentries));
        WT_RET(ingest->update(ingest));
    } else
        /* It did -- we can directly modify the ingest table. */
        WT_RET(ingest->modify(ingest, entries, nentries));

    clayered->current_cursor = ingest;

    return (0);
}

/*
 * __clayered_modify_int --
 *     Dispatch a modify call based on leader/follower status.
 */
static int
__clayered_modify_int(WT_SESSION_IMPL *session, WT_CURSOR *cursor, WT_MODIFY *entries, int nentries)
{
    if (S2C(session)->disagg_layered_leader)
        WT_RET(__clayered_modify_leader(session, cursor, entries, nentries));
    else
        WT_RET(__clayered_modify_follower(session, cursor, entries, nentries));

    return (0);
}

/*
 * __clayered_modify --
 *     WT_CURSOR->modify method for layered cursors.
 */
static int
__clayered_modify(WT_CURSOR *cursor, WT_MODIFY *entries, int nentries)
{
    WT_CURSOR *current;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    WT_CURSOR_LAYERED *clayered = (WT_CURSOR_LAYERED *)cursor;

    CURSOR_UPDATE_API_CALL(cursor, session, ret, modify, clayered->dhandle);

    WT_ERR(__cursor_needkey(cursor));
    WT_ERR(__clayered_enter(clayered, false, true, false));

    /* Check for a rational modify vector count. */
    if (nentries <= 0)
        WT_ERR_MSG(session, EINVAL, "Illegal modify vector with %d entries", nentries);

    /* Do a search if we're not positioned. */
    if (!F_ISSET(cursor, WT_CURSTD_KEY_INT) || !F_ISSET(cursor, WT_CURSTD_VALUE_INT))
        WT_ERR(cursor->search(cursor));
    WT_ASSERT(session, F_ISSET(cursor, WT_CURSTD_KEY_INT));

    WT_ERR(__clayered_modify_int(session, cursor, entries, nentries));

    /*
     * Set the cursor to reference the internal key/value of the positioned cursor.
     */
    current = clayered->current_cursor;
    F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);

    /*
     * Assign the new key/value to the top-level cursor.
     */
    WT_ITEM_SET(cursor->key, current->key);
    WT_ITEM_SET(cursor->value, current->value);
    WT_ASSERT(session, F_MASK(current, WT_CURSTD_KEY_SET) == WT_CURSTD_KEY_INT);
    F_SET(cursor, WT_CURSTD_KEY_INT);

    WT_ASSERT(session, F_ISSET(current, WT_CURSTD_VALUE_SET));
    F_SET(cursor, F_MASK(current, WT_CURSTD_VALUE_SET));

    /*
     * Modify maintains a position, key and value. Unlike update, it's not always an internal value.
     */
    WT_ASSERT(session, F_MASK(cursor, WT_CURSTD_KEY_SET) == WT_CURSTD_KEY_INT);
    WT_ASSERT(session, F_MASK(cursor, WT_CURSTD_VALUE_SET) != 0);

    WT_STAT_CONN_DSRC_INCR(session, layered_curs_modify);

err:
    __clayered_leave(clayered);
    CURSOR_UPDATE_API_END_STAT(session, ret, cursor_modify);
    return (ret);
}

/*
 * __wt_clayered_open --
 *     WT_SESSION->open_cursor method for layered cursors.
 */
int
__wt_clayered_open(WT_SESSION_IMPL *session, const char *uri, WT_CURSOR *owner, const char *cfg[],
  WT_CURSOR **cursorp)
{
    WT_CONFIG_ITEM cval;
    WT_CURSOR_STATIC_INIT(iface, __wt_cursor_get_key, /* get-key */
      __wt_cursor_get_value,                          /* get-value */
      __wt_cursor_get_raw_key_value,                  /* get-value */
      __wt_cursor_set_key,                            /* set-key */
      __wt_cursor_set_value,                          /* set-value */
      __clayered_compare,                             /* compare */
      __wt_cursor_equals,                             /* equals */
      __clayered_next,                                /* next */
      __layered_prev,                                 /* prev */
      __clayered_reset,                               /* reset */
      __clayered_search,                              /* search */
      __clayered_search_near,                         /* search-near */
      __clayered_insert,                              /* insert */
      __clayered_modify,                              /* modify */
      __clayered_update,                              /* update */
      __clayered_remove,                              /* remove */
      __clayered_reserve,                             /* reserve */
      __wti_cursor_reconfigure,                       /* reconfigure */
      __clayered_largest_key,                         /* largest_key */
      __clayered_bound,                               /* bound */
      __clayered_cache,                               /* cache */
      __clayered_reopen,                              /* reopen */
      __wt_cursor_checkpoint_id,                      /* checkpoint ID */
      __clayered_close);                              /* close */
    WT_CURSOR *cursor;
    WT_CURSOR_LAYERED *clayered;
    WT_DECL_RET;
    WT_LAYERED_TABLE *layered;
    bool cacheable;

    WT_VERIFY_OPAQUE_POINTER(WT_CURSOR_LAYERED);

    clayered = NULL;
    cursor = NULL;
    cacheable = F_ISSET(session, WT_SESSION_CACHE_CURSORS);

    if (!WT_PREFIX_MATCH(uri, "layered:"))
        return (__wt_unexpected_object_type(session, uri, "layered:"));

    WT_RET(__wt_inmem_unsupported_op(session, "Layered trees"));

    WT_RET(__wt_config_gets_def(session, cfg, "checkpoint", 0, &cval));
    if (cval.len != 0)
        WT_RET_MSG(session, EINVAL, "Layered trees do not support opening by checkpoint");

    WT_RET(__wt_config_gets_def(session, cfg, "bulk", 0, &cval));
    if (cval.val != 0)
        WT_RET_MSG(session, EINVAL, "Layered trees do not support bulk loading");

    /* Get the layered tree, and hold a reference to it until the cursor is closed. */
    WT_RET(__wt_session_get_dhandle(session, uri, NULL, cfg, 0));

    /*
     * Increment the data-source's in-use counter; done now because closing the cursor will
     * decrement it, and all failure paths from here close the cursor.
     */
    __wt_cursor_dhandle_incr_use(session);

    layered = (WT_LAYERED_TABLE *)session->dhandle;
    WT_ASSERT_ALWAYS(session, layered->n_ingest_uris > 0 && layered->key_format != NULL,
      "Layered handle not setup");

    WT_ERR(__wt_calloc_one(session, &clayered));
    clayered->dhandle = session->dhandle;

    cursor = (WT_CURSOR *)clayered;
    *cursor = iface;
    cursor->session = (WT_SESSION *)session;
    cursor->internal_uri = session->dhandle->name;
    cursor->key_format = layered->key_format;
    cursor->value_format = layered->value_format;

    WT_ERR(__wt_config_gets_def(session, cfg, "next_random", 0, &cval));
    if (cval.val != 0) {
        F_SET(clayered, WT_CLAYERED_RANDOM);
        __wti_cursor_set_notsup(cursor);
        cursor->next = __clayered_next_random;

        WT_ERR(__wt_config_gets_def(session, cfg, "next_random_seed", 0, &cval));
        clayered->next_random_seed = cval.val;

        WT_ERR(__wt_config_gets_def(session, cfg, "next_random_sample_size", 0, &cval));
        clayered->next_random_sample_size = (u_int)cval.val;
        cacheable = false;
    }

    /* Set the cache flag before finding a cursor handle. */
    if (cacheable)
        F_SET(cursor, WT_CURSTD_CACHEABLE);

    /* Try to find the cursor in the cache. */
    WT_ERR(__wt_cursor_init(cursor, uri, owner, cfg, cursorp));

    /* Layered cursor is not compatible with cursor_copy config. */
    F_CLR(cursor, WT_CURSTD_DEBUG_COPY_KEY | WT_CURSTD_DEBUG_COPY_VALUE);

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

    return (ret);
}

/*
 * __wt_debug_layered_cursor_page --
 *     Dump the in-memory information for a cursor-referenced page.
 */
int
__wt_debug_layered_cursor_page(void *cursor_arg, const char *ofile)
  WT_GCC_FUNC_ATTRIBUTE((visibility("default")))
{
    const WT_CURSOR *cursor = (const WT_CURSOR *)cursor_arg;
    WT_UNUSED(ofile);

    __wt_verbose_debug1(
      CUR2S(cursor), WT_VERB_DEFAULT, "%s: unsupported cursor type for debug dump", cursor->uri);

    return (0);
}

/*
 * __wt_debug_layered_cursor_tree_hs --
 *     Dump the in-memory information for a cursor-referenced tree's history store page.
 */
int
__wt_debug_layered_cursor_tree_hs(void *cursor_arg, const char *ofile)
  WT_GCC_FUNC_ATTRIBUTE((visibility("default")))
{
    const WT_CURSOR *cursor = (const WT_CURSOR *)cursor_arg;
    WT_UNUSED(ofile);

    __wt_verbose_debug1(
      CUR2S(cursor), WT_VERB_DEFAULT, "%s: unsupported cursor type for debug dump", cursor->uri);

    return (0);
}
