/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

#ifdef __GNUC__
#if __GNUC__ > 4 || (__GNUC__ == 4 && __GNUC_MINOR__ > 1)
/*
 * !!!
 * GCC with -Wformat-nonliteral complains about calls to strftime in this file.
 * There's nothing wrong, this makes the warning go away.
 */
#pragma GCC diagnostic ignored "-Wformat-nonliteral"
#endif
#endif

/*
 * __tiered_server_run_chk --
 *     Check to decide if the tiered storage server should continue running.
 */
static bool
__tiered_server_run_chk(WT_SESSION_IMPL *session)
{
    return (FLD_ISSET(S2C(session)->server_flags, WT_CONN_SERVER_TIERED));
}

/*
 * __flush_tier_wait --
 *     Wait for all previous work units queued to be processed.
 */
static int
__flush_tier_wait(WT_SESSION_IMPL *session, const char **cfg)
{
    WT_CONFIG_ITEM cval;
    WT_CONNECTION_IMPL *conn;
    uint64_t now, start, timeout;
    int yield_count;

    conn = S2C(session);
    yield_count = 0;
    now = start = 0;
    /*
     * The internal thread needs the schema lock to perform its operations and flush tier also
     * acquires the schema lock. We cannot be waiting in this function while holding that lock or no
     * work will get done.
     */
    WT_ASSERT(session, !FLD_ISSET(session->lock_flags, WT_SESSION_LOCKED_SCHEMA));
    WT_RET(__wt_config_gets(session, cfg, "timeout", &cval));
    timeout = (uint64_t)cval.val;
    if (timeout != 0)
        __wt_seconds(session, &start);

    /*
     * It may be worthwhile looking at the add and decrement values and make choices of whether to
     * yield or wait based on how much of the workload has been performed. Flushing operations could
     * take a long time so yielding may not be effective.
     */
    while (!WT_FLUSH_STATE_DONE(conn->flush_state)) {
        if (start != 0) {
            __wt_seconds(session, &now);
            if (now - start > timeout)
                return (EBUSY);
        }
        if (++yield_count < WT_THOUSAND)
            __wt_yield();
        else {
            __wt_cond_signal(session, conn->tiered_cond);
            __wt_cond_wait(session, conn->flush_cond, 200, NULL);
        }
    }
    return (0);
}

/*
 * __flush_tier_once --
 *     Perform one iteration of tiered storage maintenance.
 */
static int
__flush_tier_once(WT_SESSION_IMPL *session, uint32_t flags)
{
    WT_CKPT ckpt;
    WT_CONFIG_ITEM cval;
    WT_CONNECTION_IMPL *conn;
    WT_CURSOR *cursor;
    WT_DECL_RET;
    uint64_t ckpt_time, flush_time;
    const char *key, *value;

    WT_UNUSED(flags);
    __wt_verbose(session, WT_VERB_TIERED, "FLUSH_TIER_ONCE: Called flags %" PRIx32, flags);

    conn = S2C(session);
    cursor = NULL;
    /*
     * For supporting splits and merge:
     * - See if there is any merging work to do to prepare and create an object that is
     *   suitable for placing onto tiered storage.
     * - Do the work to create said objects.
     * - Move the objects.
     */
    conn->flush_state = 0;

    /*
     * We hold the checkpoint lock so we know no other thread can be doing a checkpoint at this time
     * but our time can move backward with respect to the time set by a different thread that did a
     * checkpoint. Update time value for most recent flush_tier, taking the more recent of now or
     * the checkpoint time.
     */
    WT_ASSERT(session, FLD_ISSET(session->lock_flags, WT_SESSION_LOCKED_CHECKPOINT));
    __wt_seconds(session, &flush_time);
    /*
     * XXX If/when flush tier no longer requires the checkpoint lock, all of these global values and
     * their settings need consideration to make sure they don't race with a checkpoint.
     */
    conn->flush_ckpt_complete = false;
    conn->flush_most_recent = WT_MAX(flush_time, conn->ckpt_most_recent);
    conn->flush_ts = conn->txn_global.last_ckpt_timestamp;

    /*
     * Walk the metadata cursor to find tiered tables to flush. This should be optimized to avoid
     * flushing tables that haven't changed.
     */
    WT_RET(__wt_metadata_cursor(session, &cursor));
    while (cursor->next(cursor) == 0) {
        cursor->get_key(cursor, &key);
        cursor->get_value(cursor, &value);
        /* For now just switch tiers which just does metadata manipulation. */
        if (WT_PREFIX_MATCH(key, "tiered:")) {
            __wt_verbose(
              session, WT_VERB_TIERED, "FLUSH_TIER_ONCE: %s %s 0x%" PRIx32, key, value, flags);
            if (!LF_ISSET(WT_FLUSH_TIER_FORCE)) {
                /*
                 * Check the table's last checkpoint time and only flush trees that have a
                 * checkpoint more recent than the last flush time.
                 */
                WT_ERR(__wt_meta_checkpoint(session, key, NULL, &ckpt));
                /*
                 * XXX If/when flush tier no longer requires the checkpoint lock, this needs
                 * consideration.
                 */
                ckpt_time = ckpt.sec;
                __wt_meta_checkpoint_free(session, &ckpt);
                WT_ERR(__wt_config_getones(session, value, "flush_time", &cval));

                /* If nothing has changed, there's nothing to do. */
                if (ckpt_time == 0 || (uint64_t)cval.val > ckpt_time) {
                    WT_STAT_CONN_INCR(session, flush_tier_skipped);
                    continue;
                }
            }
            /* Only instantiate the handle if we need to flush. */
            WT_ERR(__wt_session_get_dhandle(session, key, NULL, NULL, 0));
            /*
             * When we call wt_tiered_switch the session->dhandle points to the tiered: entry and
             * the arg is the config string that is currently in the metadata.
             */
            WT_ERR(__wt_tiered_switch(session, value));
            WT_STAT_CONN_INCR(session, flush_tier_switched);
            WT_ERR(__wt_session_release_dhandle(session));
        }
    }
    WT_ERR(__wt_metadata_cursor_release(session, &cursor));

    /* Clear the flag on success. */
    F_CLR(conn, WT_CONN_TIERED_FIRST_FLUSH);
    return (0);

err:
    WT_TRET(__wt_session_release_dhandle(session));
    WT_TRET(__wt_metadata_cursor_release(session, &cursor));
    return (ret);
}

/*
 * __tier_storage_remove_local --
 *     Perform one iteration of tiered storage local object removal.
 */
static int
__tier_storage_remove_local(WT_SESSION_IMPL *session)
{
    WT_DECL_RET;
    WT_TIERED_WORK_UNIT *entry;
    uint64_t now;
    const char *object;

    entry = NULL;
    for (;;) {
        /* Check if we're quitting or being reconfigured. */
        if (!__tiered_server_run_chk(session))
            break;

        __wt_seconds(session, &now);
        __wt_tiered_get_drop_local(session, now, &entry);
        if (entry == NULL)
            break;
        WT_ERR(__wt_tiered_name(
          session, &entry->tiered->iface, entry->id, WT_TIERED_NAME_OBJECT, &object));
        __wt_verbose(session, WT_VERB_TIERED, "REMOVE_LOCAL: %s at %" PRIu64, object, now);
        WT_PREFIX_SKIP_REQUIRED(session, object, "object:");
        /*
         * If the handle is still open, it could still be in use for reading. In that case put the
         * work unit back on the work queue and keep trying.
         */
        if (__wt_handle_is_open(session, object)) {
            __wt_verbose(session, WT_VERB_TIERED, "REMOVE_LOCAL: %s in USE, queue again", object);
            WT_STAT_CONN_INCR(session, local_objects_inuse);
            /*
             * FIXME-WT-7470: If the object we want to remove is in use this is the place to call
             * object sweep to clean up block->ofh file handles. Another alternative would be to try
             * to sweep and then try the remove call below rather than pushing it back on the work
             * queue. NOTE: Remove 'ofh' from s_string.ok when removing this comment.
             *
             * Update the time on the entry before pushing it back on the queue so that we don't get
             * into an infinite loop trying to drop an open file that may be in use a while.
             */
            WT_ASSERT(session, entry->tiered != NULL && entry->tiered->bstorage != NULL);
            entry->op_val = now + entry->tiered->bstorage->retain_secs;
            __wt_tiered_push_work(session, entry);
        } else {
            __wt_verbose(session, WT_VERB_TIERED, "REMOVE_LOCAL: actually remove %s", object);
            WT_STAT_CONN_INCR(session, local_objects_removed);
            WT_ERR(__wt_fs_remove(session, object, false));
            /*
             * We are responsible for freeing the work unit when we're done with it.
             */
            __wt_tiered_work_free(session, entry);
        }
        entry = NULL;
    }
err:
    if (entry != NULL)
        __wt_tiered_work_free(session, entry);
    return (ret);
}

/*
 * __tier_flush_meta --
 *     Perform one iteration of altering the metadata after a flush. This is in its own function so
 *     that we can hold the schema lock while doing the metadata tracking.
 */
static int
__tier_flush_meta(
  WT_SESSION_IMPL *session, WT_TIERED *tiered, const char *local_uri, const char *obj_uri)
{
    WT_CONNECTION_IMPL *conn;
    WT_DATA_HANDLE *dhandle;
    WT_DECL_ITEM(buf);
    WT_DECL_RET;
    uint64_t now;
    char hex_timestamp[WT_TS_HEX_STRING_SIZE];
    char *newconfig, *obj_value;
    const char *cfg[3] = {NULL, NULL, NULL};
    bool release, tracking;

    conn = S2C(session);
    release = tracking = false;
    WT_RET(__wt_scr_alloc(session, 512, &buf));
    dhandle = &tiered->iface;

    newconfig = obj_value = NULL;
    WT_ERR(__wt_meta_track_on(session));
    tracking = true;

    WT_ERR(__wt_session_get_dhandle(session, dhandle->name, NULL, NULL, 0));
    release = true;
    /*
     * Once the flush call succeeds we want to first remove the file: entry from the metadata and
     * then update the object: metadata to indicate the flush is complete. Record the flush
     * timestamp from the flush call. We know that no new flush_tier call can begin until all work
     * from the last call completes, so the connection field is correct.
     */
    __wt_timestamp_to_hex_string(conn->flush_ts, hex_timestamp);
    WT_ERR(__wt_metadata_remove(session, local_uri));
    WT_ERR(__wt_metadata_search(session, obj_uri, &obj_value));
    __wt_seconds(session, &now);
    WT_ERR(__wt_buf_fmt(
      session, buf, "flush_time=%" PRIu64 ",flush_timestamp=\"%s\"", now, hex_timestamp));
    cfg[0] = obj_value;
    cfg[1] = buf->mem;
    WT_ERR(__wt_config_collapse(session, cfg, &newconfig));
    WT_ERR(__wt_metadata_update(session, obj_uri, newconfig));
    WT_ERR(__wt_meta_track_off(session, true, ret != 0));
    tracking = false;

err:
    __wt_free(session, newconfig);
    __wt_free(session, obj_value);
    if (release)
        WT_TRET(__wt_session_release_dhandle(session));
    __wt_scr_free(session, &buf);
    if (tracking)
        WT_TRET(__wt_meta_track_off(session, true, ret != 0));
    if (ret == ENOENT)
        ret = 0;
    return (ret);
}

/*
 * __tier_do_operation --
 *     Perform one iteration of copying newly flushed objects to shared storage or post-flush
 *     processing.
 */
static int
__tier_do_operation(WT_SESSION_IMPL *session, WT_TIERED *tiered, uint32_t id, const char *local_uri,
  const char *obj_uri, uint32_t op)
{
    WT_CONFIG_ITEM pfx;
    WT_DECL_RET;
    WT_FILE_SYSTEM *bucket_fs;
    WT_STORAGE_SOURCE *storage_source;
    size_t len;
    char *tmp;
    const char *cfg[2], *local_name, *obj_name;

    WT_ASSERT(session, (op == WT_TIERED_WORK_FLUSH || op == WT_TIERED_WORK_FLUSH_FINISH));
    tmp = NULL;
    storage_source = tiered->bstorage->storage_source;
    bucket_fs = tiered->bstorage->file_system;

    local_name = local_uri;
    WT_PREFIX_SKIP_REQUIRED(session, local_name, "file:");
    obj_name = obj_uri;
    WT_PREFIX_SKIP_REQUIRED(session, obj_name, "object:");
    cfg[0] = tiered->obj_config;
    cfg[1] = NULL;
    WT_RET(__wt_config_gets(session, cfg, "tiered_storage.bucket_prefix", &pfx));
    WT_ASSERT(session, pfx.len != 0);
    len = strlen(obj_name) + pfx.len + 1;
    WT_RET(__wt_calloc_def(session, len, &tmp));
    WT_ERR(__wt_snprintf(tmp, len, "%.*s%s", (int)pfx.len, pfx.str, obj_name));

    if (op == WT_TIERED_WORK_FLUSH_FINISH)
        WT_ERR(storage_source->ss_flush_finish(
          storage_source, &session->iface, bucket_fs, local_name, tmp, NULL));
    else {
        /* WT_TIERED_WORK_FLUSH */
        /* This call make take a while, and may fail due to network timeout. */
        ret = storage_source->ss_flush(
          storage_source, &session->iface, bucket_fs, local_name, tmp, NULL);
        if (ret == 0)
            WT_WITH_CHECKPOINT_LOCK(session,
              WT_WITH_SCHEMA_LOCK(
                session, ret = __tier_flush_meta(session, tiered, local_uri, obj_uri)));
        /*
         * If a user did a flush_tier with sync off, it is possible that a drop happened before the
         * flush work unit was processed. Ignore non-existent errors from either previous call.
         */
        if (ret == ENOENT)
            ret = 0;
        else {
            WT_ERR(ret);

            /*
             * After successful flushing, push a work unit to perform whatever post-processing the
             * shared storage wants to do for this object. Note that this work unit is unrelated to
             * the drop local work unit below. They do not need to be in any order and do not
             * interfere with each other.
             */
            WT_ERR(__wt_tiered_put_flush_finish(session, tiered, id));
            /*
             * After successful flushing, push a work unit to drop the local object in the future.
             * The object will be removed locally after the local retention period expires.
             */
            WT_ERR(__wt_tiered_put_drop_local(session, tiered, id));
        }
    }

err:
    __wt_free(session, tmp);
    return (ret);
}

/*
 * __tier_operation --
 *     Given an ID generate the URI names and call the operation code to flush or finish.
 */
static int
__tier_operation(WT_SESSION_IMPL *session, WT_TIERED *tiered, uint32_t id, uint32_t op)
{
    WT_DECL_RET;
    const char *local_uri, *obj_uri;

    local_uri = obj_uri = NULL;
    WT_ERR(__wt_tiered_name(session, &tiered->iface, id, WT_TIERED_NAME_LOCAL, &local_uri));
    WT_ERR(__wt_tiered_name(session, &tiered->iface, id, WT_TIERED_NAME_OBJECT, &obj_uri));
    WT_ERR(__tier_do_operation(session, tiered, id, local_uri, obj_uri, op));

err:
    __wt_free(session, local_uri);
    __wt_free(session, obj_uri);
    return (ret);
}

/*
 * __tier_storage_finish --
 *     Perform one iteration of shared storage post-flush work. This is separated from copying the
 *     objects to shared storage to allow the flush_tier call to return after only the necessary
 *     work has completed.
 */
static int
__tier_storage_finish(WT_SESSION_IMPL *session)
{
    WT_DECL_RET;
    WT_TIERED_WORK_UNIT *entry;

    entry = NULL;
    /*
     * Sleep a known period of time so that tests using the timing stress flag can have an idea when
     * to check for the cache operation to complete. Sleep one second before processing the work
     * queue of cache work units.
     */
    if (FLD_ISSET(S2C(session)->timing_stress_flags, WT_TIMING_STRESS_TIERED_FLUSH_FINISH))
        __wt_sleep(1, 0);
    for (;;) {
        /* Check if we're quitting or being reconfigured. */
        if (!__tiered_server_run_chk(session))
            break;

        __wt_tiered_get_flush_finish(session, &entry);
        if (entry == NULL)
            break;
        WT_ERR(__tier_operation(session, entry->tiered, entry->id, WT_TIERED_WORK_FLUSH_FINISH));
        /*
         * We are responsible for freeing the work unit when we're done with it.
         */
        __wt_tiered_work_free(session, entry);
        entry = NULL;
    }

err:
    if (entry != NULL)
        __wt_tiered_work_free(session, entry);
    return (ret);
}

/*
 * __tier_storage_copy --
 *     Perform one iteration of copying newly flushed objects to the shared storage.
 */
static int
__tier_storage_copy(WT_SESSION_IMPL *session)
{
    WT_DECL_RET;
    WT_TIERED_WORK_UNIT *entry;

    /* There is nothing to do until the checkpoint after the flush completes. */
    if (!S2C(session)->flush_ckpt_complete)
        return (0);
    entry = NULL;
    for (;;) {
        /* Check if we're quitting or being reconfigured. */
        if (!__tiered_server_run_chk(session))
            break;

        /*
         * We probably need some kind of flush generation so that we don't process flush items for
         * tables that are added during an in-progress flush_tier. This thread could run due to a
         * condition timeout rather than a signal. Checking that generation number would be part of
         * calling __wt_tiered_get_flush so that we don't pull it off the queue until we're sure we
         * want to process it.
         */
        __wt_tiered_get_flush(session, &entry);
        if (entry == NULL)
            break;
        WT_ERR(__tier_operation(session, entry->tiered, entry->id, WT_TIERED_WORK_FLUSH));
        /*
         * We are responsible for freeing the work unit when we're done with it.
         */
        __wt_tiered_work_free(session, entry);
        entry = NULL;
    }

err:
    if (entry != NULL)
        __wt_tiered_work_free(session, entry);
    return (ret);
}

/*
 * __tier_storage_remove --
 *     Perform one iteration of tiered storage local tier removal.
 */
static int
__tier_storage_remove(WT_SESSION_IMPL *session, bool force)
{
    WT_UNUSED(session);
    WT_UNUSED(force);

    /*
     * We want to walk the metadata perhaps and for each tiered URI, call remove on its file:URI
     * version.
     */
    WT_RET(__tier_storage_remove_local(session));
    return (0);
}

/*
 * __wt_flush_tier --
 *     Entry function for flush_tier method.
 */
int
__wt_flush_tier(WT_SESSION_IMPL *session, const char *config)
{
    WT_CONFIG_ITEM cval;
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    uint32_t flags;
    const char *cfg[3];
    const char *checkpoint_cfg[] = {WT_CONFIG_BASE(session, WT_SESSION_checkpoint), NULL};
    bool locked, wait;

    conn = S2C(session);
    WT_STAT_CONN_INCR(session, flush_tier);
    flags = 0;
    cfg[0] = WT_CONFIG_BASE(session, WT_SESSION_flush_tier);
    cfg[1] = (char *)config;
    cfg[2] = NULL;
    WT_RET(__wt_config_gets(session, cfg, "force", &cval));
    if (cval.val)
        LF_SET(WT_FLUSH_TIER_FORCE);
    WT_RET(__wt_config_gets(session, cfg, "sync", &cval));
    if (WT_STRING_MATCH("off", cval.str, cval.len))
        LF_SET(WT_FLUSH_TIER_OFF);
    else if (WT_STRING_MATCH("on", cval.str, cval.len))
        LF_SET(WT_FLUSH_TIER_ON);

    WT_RET(__wt_config_gets(session, cfg, "lock_wait", &cval));
    if (cval.val)
        wait = true;
    else
        wait = false;

    /*
     * We have to hold the lock around both the wait call for a previous flush tier and the
     * execution of the current flush tier call.
     */
    if (wait)
        __wt_spin_lock(session, &conn->flush_tier_lock);
    else
        WT_RET(__wt_spin_trylock(session, &conn->flush_tier_lock));
    locked = true;

    /*
     * We cannot perform another flush tier until any earlier ones are done. Often threads will wait
     * after the flush tier based on the sync setting so this check will be fast. But if sync is
     * turned off then any following call must wait and will do so here. We have to wait while not
     * holding the schema lock.
     */
    WT_ERR(__flush_tier_wait(session, cfg));
    if (wait)
        WT_WITH_CHECKPOINT_LOCK(
          session, WT_WITH_SCHEMA_LOCK(session, ret = __flush_tier_once(session, flags)));
    else
        WT_WITH_CHECKPOINT_LOCK_NOWAIT(session, ret,
          WT_WITH_SCHEMA_LOCK_NOWAIT(session, ret, ret = __flush_tier_once(session, flags)));
    __wt_spin_unlock(session, &conn->flush_tier_lock);
    locked = false;
    if (FLD_ISSET(conn->debug_flags, WT_CONN_DEBUG_FLUSH_CKPT))
        WT_ERR(__wt_txn_checkpoint(session, checkpoint_cfg, true));

    if (ret == 0 && LF_ISSET(WT_FLUSH_TIER_ON))
        WT_ERR(__flush_tier_wait(session, cfg));

err:
    if (locked)
        __wt_spin_unlock(session, &conn->flush_tier_lock);
    return (ret);
}

/*
 * __tiered_server --
 *     The tiered storage server thread.
 */
static WT_THREAD_RET
__tiered_server(void *arg)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_ITEM path, tmp;
    WT_SESSION_IMPL *session;
    uint64_t cond_time, time_start, time_stop, timediff;
    bool signalled;

    session = arg;
    conn = S2C(session);

    WT_CLEAR(path);
    WT_CLEAR(tmp);

    /* Condition timeout is in microseconds. */
    cond_time = conn->tiered_interval * WT_MILLION;
    time_start = __wt_clock(session);
    signalled = false;
    for (;;) {
        /* Wait until the next event. */
        __wt_cond_wait_signal(
          session, conn->tiered_cond, cond_time, __tiered_server_run_chk, &signalled);

        /* Check if we're quitting or being reconfigured. */
        if (!__tiered_server_run_chk(session))
            break;

        time_stop = __wt_clock(session);
        timediff = WT_CLOCKDIFF_SEC(time_stop, time_start);
        /*
         * Here is where we do work. Work we expect to do:
         *  - Copy any files that need moving from a flush tier call.
         *  - Perform any shared storage processing after flushing.
         *  - Remove any cached objects that are aged out.
         */
        if (timediff >= conn->tiered_interval || signalled) {
            WT_ERR(__tier_storage_copy(session));
            WT_ERR(__tier_storage_finish(session));
            WT_ERR(__tier_storage_remove(session, false));
            time_start = time_stop;
        }
    }

    if (0) {
err:
        WT_IGNORE_RET(__wt_panic(session, ret, "storage server error"));
    }
    __wt_buf_free(session, &path);
    __wt_buf_free(session, &tmp);
    return (WT_THREAD_RET_VALUE);
}

/*
 * __wt_tiered_storage_create --
 *     Start the tiered storage subsystem.
 */
int
__wt_tiered_storage_create(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;

    conn = S2C(session);

    /* Start the internal thread. */
    WT_ERR(__wt_cond_alloc(session, "flush tier", &conn->flush_cond));
    WT_ERR(__wt_cond_alloc(session, "storage server", &conn->tiered_cond));
    FLD_SET(conn->server_flags, WT_CONN_SERVER_TIERED);

    WT_ERR(__wt_open_internal_session(conn, "tiered-server", true, 0, 0, &conn->tiered_session));
    session = conn->tiered_session;

    /*
     * Check for objects that are not flushed on the first flush_tier call. We cannot do that work
     * right now because it would entail opening and getting the dhandle for every table and that
     * work is already done in the flush_tier. So do it there and keep that code together.
     */
    F_SET(conn, WT_CONN_TIERED_FIRST_FLUSH);
    /* Start the thread. */
    WT_ERR(__wt_thread_create(session, &conn->tiered_tid, __tiered_server, session));
    conn->tiered_tid_set = true;

    if (0) {
err:
        FLD_CLR(conn->server_flags, WT_CONN_SERVER_TIERED);
        WT_TRET(__wt_tiered_storage_destroy(session, false));
    }
    return (ret);
}

/*
 * __wt_tiered_storage_destroy --
 *     Destroy the tiered storage server thread.
 */
int
__wt_tiered_storage_destroy(WT_SESSION_IMPL *session, bool final_flush)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_TIERED_WORK_UNIT *entry;

    conn = S2C(session);

    /*
     * Stop the internal server thread. If there is unfinished work, we will recover it on startup
     * just as if there had been a system failure.
     */
    if (conn->flush_cond != NULL)
        __wt_cond_signal(session, conn->flush_cond);
    if (final_flush && conn->tiered_cond != NULL) {
        __wt_cond_signal(session, conn->tiered_cond);
        WT_TRET(__wt_tiered_flush_work_wait(session, 30));
    }
    FLD_CLR(conn->server_flags, WT_CONN_SERVER_TIERED);
    if (conn->tiered_tid_set) {
        WT_ASSERT(session, conn->tiered_cond != NULL);
        __wt_cond_signal(session, conn->tiered_cond);
        WT_TRET(__wt_thread_join(session, &conn->tiered_tid));
        conn->tiered_tid_set = false;
        while ((entry = TAILQ_FIRST(&conn->tieredqh)) != NULL) {
            TAILQ_REMOVE(&conn->tieredqh, entry, q);
            __wt_tiered_work_free(session, entry);
        }
    }
    if (conn->tiered_session != NULL) {
        WT_TRET(__wt_session_close_internal(conn->tiered_session));
        conn->tiered_session = NULL;
    }

    /* Destroy all condition variables after threads have stopped. */
    __wt_cond_destroy(session, &conn->tiered_cond);
    /* The flush condition variable must be last because any internal thread could be using it. */
    __wt_cond_destroy(session, &conn->flush_cond);

    return (ret);
}
