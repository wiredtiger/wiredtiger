/*-
 * Copyright (c) 2014-2019 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * When an operation is accessing the lookaside table, it should ignore the cache size (since the
 * cache is already full), and the operation can't reenter reconciliation.
 */
#define WT_LAS_SESSION_FLAGS (WT_SESSION_IGNORE_CACHE_SIZE | WT_SESSION_NO_RECONCILE)

/*
 * __las_set_isolation --
 *     Switch to read-uncommitted.
 */
static void
__las_set_isolation(WT_SESSION_IMPL *session, WT_TXN_ISOLATION *saved_isolationp)
{
    *saved_isolationp = session->txn.isolation;
    session->txn.isolation = WT_ISO_READ_UNCOMMITTED;
}

/*
 * __las_restore_isolation --
 *     Restore isolation.
 */
static void
__las_restore_isolation(WT_SESSION_IMPL *session, WT_TXN_ISOLATION saved_isolation)
{
    session->txn.isolation = saved_isolation;
}

/*
 * __las_entry_count --
 *     Return when there are entries in the lookaside table.
 */
static uint64_t
__las_entry_count(WT_CACHE *cache)
{
    uint64_t insert_cnt, remove_cnt;

    insert_cnt = cache->las_insert_count;
    WT_ORDERED_READ(remove_cnt, cache->las_remove_count);

    return (insert_cnt > remove_cnt ? insert_cnt - remove_cnt : 0);
}

/*
 * __wt_las_config --
 *     Configure the lookaside table.
 */
int
__wt_las_config(WT_SESSION_IMPL *session, const char **cfg)
{
    WT_CONFIG_ITEM cval;
    WT_CURSOR_BTREE *las_cursor;
    WT_SESSION_IMPL *las_session;

    WT_RET(__wt_config_gets(session, cfg, "cache_overflow.file_max", &cval));

    if (cval.val != 0 && cval.val < WT_LAS_FILE_MIN)
        WT_RET_MSG(session, EINVAL, "max cache overflow size %" PRId64 " below minimum %d",
          cval.val, WT_LAS_FILE_MIN);

    /* This is expected for in-memory configurations. */
    las_session = S2C(session)->cache->las_session[0];
    WT_ASSERT(session, las_session != NULL || F_ISSET(S2C(session), WT_CONN_IN_MEMORY));

    if (las_session == NULL)
        return (0);

    /*
     * We need to set file_max on the btree associated with one of the lookaside sessions.
     */
    las_cursor = (WT_CURSOR_BTREE *)las_session->las_cursor;
    las_cursor->btree->file_max = (uint64_t)cval.val;

    WT_STAT_CONN_SET(session, cache_lookaside_ondisk_max, las_cursor->btree->file_max);

    return (0);
}

/*
 * __wt_las_empty --
 *     Return when there are entries in the lookaside table.
 */
bool
__wt_las_empty(WT_SESSION_IMPL *session)
{
    return (__las_entry_count(S2C(session)->cache) == 0);
}

/*
 * __wt_las_stats_update --
 *     Update the lookaside table statistics for return to the application.
 */
void
__wt_las_stats_update(WT_SESSION_IMPL *session)
{
    WT_CACHE *cache;
    WT_CONNECTION_IMPL *conn;
    WT_CONNECTION_STATS **cstats;
    WT_DSRC_STATS **dstats;
    int64_t v;

    conn = S2C(session);
    cache = conn->cache;

    /*
     * Lookaside table statistics are copied from the underlying lookaside table data-source
     * statistics. If there's no lookaside table, values remain 0.
     */
    if (!F_ISSET(conn, WT_CONN_LOOKASIDE_OPEN))
        return;

    /* Set the connection-wide statistics. */
    cstats = conn->stats;

    WT_STAT_SET(session, cstats, cache_lookaside_entries, __las_entry_count(cache));

    /*
     * We have a cursor, and we need the underlying data handle; we can get to it by way of the
     * underlying btree handle, but it's a little ugly.
     */
    dstats = ((WT_CURSOR_BTREE *)cache->las_session[0]->las_cursor)->btree->dhandle->stats;

    v = WT_STAT_READ(dstats, cursor_update);
    WT_STAT_SET(session, cstats, cache_lookaside_insert, v);
    v = WT_STAT_READ(dstats, cursor_remove);
    WT_STAT_SET(session, cstats, cache_lookaside_remove, v);

    /*
     * If we're clearing stats we need to clear the cursor values we just read. This does not clear
     * the rest of the statistics in the lookaside data source stat cursor, but we own that
     * namespace so we don't have to worry about users seeing inconsistent data source information.
     */
    if (FLD_ISSET(conn->stat_flags, WT_STAT_CLEAR)) {
        WT_STAT_SET(session, dstats, cursor_insert, 0);
        WT_STAT_SET(session, dstats, cursor_remove, 0);
    }
}

/*
 * __wt_las_create --
 *     Initialize the database's lookaside store.
 */
int
__wt_las_create(WT_SESSION_IMPL *session, const char **cfg)
{
    WT_CACHE *cache;
    WT_CONNECTION_IMPL *conn;
    int i;

    conn = S2C(session);
    cache = conn->cache;

    /* Read-only and in-memory configurations don't need the LAS table. */
    if (F_ISSET(conn, WT_CONN_IN_MEMORY | WT_CONN_READONLY))
        return (0);

    /* Re-create the table. */
    WT_RET(__wt_session_create(session, WT_LAS_URI, WT_LAS_CONFIG));

    /*
     * Open a shared internal session and cursor used for the lookaside table. This session should
     * never perform reconciliation.
     */
    for (i = 0; i < WT_LAS_NUM_SESSIONS; i++) {
        WT_RET(__wt_open_internal_session(
          conn, "lookaside table", true, WT_LAS_SESSION_FLAGS, &cache->las_session[i]));
        WT_RET(__wt_las_cursor_open(cache->las_session[i]));
    }

    WT_RET(__wt_las_config(session, cfg));

    /* The statistics server is already running, make sure we don't race. */
    WT_WRITE_BARRIER();
    F_SET(conn, WT_CONN_LOOKASIDE_OPEN);

    return (0);
}

/*
 * __wt_las_destroy --
 *     Destroy the database's lookaside store.
 */
int
__wt_las_destroy(WT_SESSION_IMPL *session)
{
    WT_CACHE *cache;
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_SESSION *wt_session;
    int i;

    conn = S2C(session);
    cache = conn->cache;

    F_CLR(conn, WT_CONN_LOOKASIDE_OPEN);
    if (cache == NULL)
        return (0);

    for (i = 0; i < WT_LAS_NUM_SESSIONS; i++) {
        if (cache->las_session[i] == NULL)
            continue;

        wt_session = &cache->las_session[i]->iface;
        WT_TRET(wt_session->close(wt_session, NULL));
        cache->las_session[i] = NULL;
    }

    __wt_buf_free(session, &cache->las_sweep_key);
    __wt_buf_free(session, &cache->las_max_key);
    __wt_free(session, cache->las_dropped);
    __wt_free(session, cache->las_sweep_dropmap);

    return (ret);
}

/*
 * __wt_las_cursor_open --
 *     Open a new lookaside table cursor.
 */
int
__wt_las_cursor_open(WT_SESSION_IMPL *session)
{
    WT_BTREE *btree;
    WT_CURSOR *cursor;
    WT_DECL_RET;
    const char *open_cursor_cfg[] = {WT_CONFIG_BASE(session, WT_SESSION_open_cursor), NULL};

    WT_WITHOUT_DHANDLE(
      session, ret = __wt_open_cursor(session, WT_LAS_URI, NULL, open_cursor_cfg, &cursor));
    WT_RET(ret);

    /*
     * Retrieve the btree from the cursor, rather than the session because we don't always switch
     * the LAS handle in to the session before entering this function.
     */
    btree = ((WT_CURSOR_BTREE *)cursor)->btree;

    /* Track the lookaside file ID. */
    if (S2C(session)->cache->las_fileid == 0)
        S2C(session)->cache->las_fileid = btree->id;

    /*
     * Set special flags for the lookaside table: the lookaside flag (used, for example, to avoid
     * writing records during reconciliation), also turn off checkpoints and logging.
     *
     * Test flags before setting them so updates can't race in subsequent opens (the first update is
     * safe because it's single-threaded from wiredtiger_open).
     */
    if (!F_ISSET(btree, WT_BTREE_LOOKASIDE))
        F_SET(btree, WT_BTREE_LOOKASIDE);
    if (!F_ISSET(btree, WT_BTREE_NO_LOGGING))
        F_SET(btree, WT_BTREE_NO_LOGGING);

    session->las_cursor = cursor;
    F_SET(session, WT_SESSION_LOOKASIDE_CURSOR);

    return (0);
}

/*
 * __wt_las_cursor --
 *     Return a lookaside cursor.
 */
void
__wt_las_cursor(WT_SESSION_IMPL *session, WT_CURSOR **cursorp, uint32_t *session_flags)
{
    WT_CACHE *cache;
    int i;

    *cursorp = NULL;

    /*
     * We don't want to get tapped for eviction after we start using the lookaside cursor; save a
     * copy of the current eviction state, we'll turn eviction off before we return.
     *
     * Don't cache lookaside table pages, we're here because of eviction problems and there's no
     * reason to believe lookaside pages will be useful more than once.
     */
    *session_flags = F_MASK(session, WT_LAS_SESSION_FLAGS);

    cache = S2C(session)->cache;

    /*
     * Some threads have their own lookaside table cursors, else lock the shared lookaside cursor.
     */
    if (F_ISSET(session, WT_SESSION_LOOKASIDE_CURSOR))
        *cursorp = session->las_cursor;
    else {
        for (;;) {
            __wt_spin_lock(session, &cache->las_lock);
            for (i = 0; i < WT_LAS_NUM_SESSIONS; i++) {
                if (!cache->las_session_inuse[i]) {
                    *cursorp = cache->las_session[i]->las_cursor;
                    cache->las_session_inuse[i] = true;
                    break;
                }
            }
            __wt_spin_unlock(session, &cache->las_lock);
            if (*cursorp != NULL)
                break;
            /*
             * If all the lookaside sessions are busy, stall.
             *
             * XXX better as a condition variable.
             */
            __wt_sleep(0, WT_THOUSAND);
            if (F_ISSET(session, WT_SESSION_INTERNAL))
                WT_STAT_CONN_INCRV(session, cache_lookaside_cursor_wait_internal, WT_THOUSAND);
            else
                WT_STAT_CONN_INCRV(session, cache_lookaside_cursor_wait_application, WT_THOUSAND);
        }
    }

    /* Configure session to access the lookaside table. */
    F_SET(session, WT_LAS_SESSION_FLAGS);
}

/*
 * __wt_las_cursor_close --
 *     Discard a lookaside cursor.
 */
int
__wt_las_cursor_close(WT_SESSION_IMPL *session, WT_CURSOR **cursorp, uint32_t session_flags)
{
    WT_CACHE *cache;
    WT_CURSOR *cursor;
    WT_DECL_RET;
    int i;

    cache = S2C(session)->cache;

    if ((cursor = *cursorp) == NULL)
        return (0);
    *cursorp = NULL;

    /* Reset the cursor. */
    ret = cursor->reset(cursor);

    /*
     * We turned off caching and eviction while the lookaside cursor was in use, restore the
     * session's flags.
     */
    F_CLR(session, WT_LAS_SESSION_FLAGS);
    F_SET(session, session_flags);

    /*
     * Some threads have their own lookaside table cursors, else unlock the shared lookaside cursor.
     */
    if (!F_ISSET(session, WT_SESSION_LOOKASIDE_CURSOR)) {
        __wt_spin_lock(session, &cache->las_lock);
        for (i = 0; i < WT_LAS_NUM_SESSIONS; i++)
            if (cursor->session == &cache->las_session[i]->iface) {
                cache->las_session_inuse[i] = false;
                break;
            }
        __wt_spin_unlock(session, &cache->las_lock);
        WT_ASSERT(session, i != WT_LAS_NUM_SESSIONS);
    }

    return (ret);
}

/*
 * __wt_las_page_skip_locked --
 *     Check if we can skip reading a page with lookaside entries, where the page is already locked.
 */
bool
__wt_las_page_skip_locked(WT_SESSION_IMPL *session, WT_REF *ref)
{
    WT_TXN *txn;

    txn = &session->txn;

    /*
     * Skip lookaside pages if reading without a timestamp and all the updates in lookaside are in
     * the past.
     *
     * Lookaside eviction preferentially chooses the newest updates when creating page images with
     * no stable timestamp. If a stable timestamp has been set, we have to visit the page because
     * eviction chooses old version of records in that case.
     *
     * One case where we may need to visit the page is if lookaside eviction is active in tree 2
     * when a checkpoint has started and is working its way through tree 1. In that case, lookaside
     * may have created a page image with updates in the future of the checkpoint.
     *
     * We also need to instantiate a lookaside page if this is an update operation in progress or
     * transaction is in prepared state.
     */
    if (F_ISSET(txn, WT_TXN_PREPARE | WT_TXN_UPDATE))
        return (false);

    if (!F_ISSET(txn, WT_TXN_HAS_SNAPSHOT))
        return (false);

    /*
     * If some of the page's history overlaps with the reader's snapshot then we have to read it.
     */
    if (WT_TXNID_LE(txn->snap_min, ref->page_las->max_txn))
        return (false);

    /*
     * Otherwise, if not reading at a timestamp, the page's history is in the past, so the page
     * image is correct if it contains the most recent versions of everything and nothing was
     * prepared.
     */
    if (!F_ISSET(txn, WT_TXN_HAS_TS_READ))
        return (!ref->page_las->has_prepares && ref->page_las->min_skipped_ts == WT_TS_MAX);

    /*
     * Skip lookaside history if reading as of a timestamp, we evicted new versions of data and all
     * the updates are in the past. This is not possible for prepared updates, because the commit
     * timestamp was not known when the page was evicted.
     *
     * Otherwise, skip reading lookaside history if everything on the page is older than the read
     * timestamp, and the oldest update in lookaside newer than the page is in the future of the
     * reader. This seems unlikely, but is exactly what eviction tries to do when a checkpoint is
     * running.
     */
    if (!ref->page_las->has_prepares && ref->page_las->min_skipped_ts == WT_TS_MAX &&
      txn->read_timestamp >= ref->page_las->max_ondisk_ts)
        return (true);

    if (txn->read_timestamp >= ref->page_las->max_ondisk_ts &&
      txn->read_timestamp < ref->page_las->min_skipped_ts)
        return (true);

    return (false);
}

/*
 * __wt_las_page_skip --
 *     Check if we can skip reading a page with lookaside entries, where the page needs to be locked
 *     before checking.
 */
bool
__wt_las_page_skip(WT_SESSION_IMPL *session, WT_REF *ref)
{
    uint32_t previous_state;
    bool skip;

    if ((previous_state = ref->state) != WT_REF_LOOKASIDE)
        return (false);

    if (!WT_REF_CAS_STATE(session, ref, previous_state, WT_REF_LOCKED))
        return (false);

    skip = __wt_las_page_skip_locked(session, ref);

    /* Restore the state and push the change. */
    WT_REF_SET_STATE(ref, previous_state);
    WT_FULL_BARRIER();

    return (skip);
}

/*
 * __las_insert_updates_verbose --
 *     Display a verbose message once per checkpoint with details about the cache state when
 *     performing a lookaside table write.
 */
static void
__las_insert_updates_verbose(WT_SESSION_IMPL *session, WT_BTREE *btree, WT_MULTI *multi)
{
    WT_CACHE *cache;
    WT_CONNECTION_IMPL *conn;
    double pct_dirty, pct_full;
    uint64_t ckpt_gen_current, ckpt_gen_last;
    uint32_t btree_id;
    char ts_string[2][WT_TS_INT_STRING_SIZE];

    btree_id = btree->id;

    if (!WT_VERBOSE_ISSET(session, WT_VERB_LOOKASIDE | WT_VERB_LOOKASIDE_ACTIVITY))
        return;

    conn = S2C(session);
    cache = conn->cache;
    ckpt_gen_current = __wt_gen(session, WT_GEN_CHECKPOINT);
    ckpt_gen_last = cache->las_verb_gen_write;

    /*
     * Print a message if verbose lookaside, or once per checkpoint if only reporting activity.
     * Avoid an expensive atomic operation as often as possible when the message rate is limited.
     */
    if (WT_VERBOSE_ISSET(session, WT_VERB_LOOKASIDE) ||
      (ckpt_gen_current > ckpt_gen_last &&
          __wt_atomic_casv64(&cache->las_verb_gen_write, ckpt_gen_last, ckpt_gen_current))) {
        WT_IGNORE_RET_BOOL(__wt_eviction_clean_needed(session, &pct_full));
        WT_IGNORE_RET_BOOL(__wt_eviction_dirty_needed(session, &pct_dirty));

        __wt_verbose(session, WT_VERB_LOOKASIDE | WT_VERB_LOOKASIDE_ACTIVITY,
          "Page reconciliation triggered lookaside write: file ID %" PRIu32
          ". "
          "Max txn ID %" PRIu64
          ", max ondisk timestamp %s, "
          "first skipped ts %s. "
          "Entries now in lookaside file: %" PRId64
          ", "
          "cache dirty: %2.3f%% , "
          "cache use: %2.3f%%",
          btree_id, multi->page_las.max_txn,
          __wt_timestamp_to_string(multi->page_las.max_ondisk_ts, ts_string[0]),
          __wt_timestamp_to_string(multi->page_las.min_skipped_ts, ts_string[1]),
          WT_STAT_READ(conn->stats, cache_lookaside_entries), pct_dirty, pct_full);
    }

    /* Never skip updating the tracked generation */
    if (WT_VERBOSE_ISSET(session, WT_VERB_LOOKASIDE))
        cache->las_verb_gen_write = ckpt_gen_current;
}

/*
 * __las_squash_modifies --
 *     Squash multiple modify operations for the same key and timestamp into a standard update and
 *     insert it into lookaside. This is necessary since multiple updates on the same key/timestamp
 *     and transaction will clobber each other in the lookaside which is problematic for modifies.
 */
static int
__las_squash_modifies(WT_SESSION_IMPL *session, WT_CURSOR *cursor, WT_UPDATE **updp)
{
    WT_DECL_RET;
    WT_ITEM las_value;
    WT_MODIFY_VECTOR modifies;
    WT_UPDATE *next_upd, *start_upd, *upd;

    WT_CLEAR(las_value);
    __wt_modify_vector_init(session, &modifies);
    start_upd = upd = *updp;
    next_upd = start_upd->next;

    while (upd->type == WT_UPDATE_MODIFY) {
        WT_ERR(__wt_modify_vector_push(&modifies, upd));
        upd = upd->next;
        /*
         * Our goal here is to squash and write one update in the case where there are multiple
         * modifies for a given timestamp and transaction id. So we want to resume insertions right
         * where a new timestamp and transaction id pairing begins.
         */
        if (start_upd->start_ts == upd->start_ts && start_upd->txnid == upd->txnid)
            next_upd = upd;
        /*
         * If we've spotted a modify, there should be a standard update later in the update list. If
         * we hit the end and still haven't found one, something is not right.
         */
        if (upd == NULL)
            WT_PANIC_ERR(session, WT_PANIC,
              "found modify update but no corresponding standard update in the update list");
    }
    WT_ERR(__wt_buf_set(session, &las_value, upd->data, upd->size));
    while (modifies.size > 0) {
        __wt_modify_vector_pop(&modifies, &upd);
        WT_ERR(__wt_modify_apply_item(session, &las_value, upd->data, false));
    }
    cursor->set_value(
      cursor, start_upd->durable_ts, start_upd->prepare_state, WT_UPDATE_STANDARD, &las_value);
    WT_ERR(cursor->insert(cursor));

err:
    __wt_modify_vector_free(&modifies);
    __wt_buf_free(session, &las_value);
    WT_STAT_CONN_INCR(session, cache_lookaside_write_squash);
    *updp = next_upd;
    return (ret);
}

/*
 * __wt_las_insert_updates --
 *     Copy one set of saved updates into the database's lookaside table.
 */
int
__wt_las_insert_updates(WT_CURSOR *cursor, WT_BTREE *btree, WT_PAGE *page, WT_MULTI *multi)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_ITEM(key);
    WT_DECL_ITEM(mementos);
    WT_DECL_RET;
    WT_ITEM las_value;
    WT_KEY_MEMENTO *mementop;
    WT_SAVE_UPD *list;
    WT_SESSION_IMPL *session;
    WT_TXN_ISOLATION saved_isolation;
    WT_UPDATE *first_upd, *upd;
    wt_off_t las_size;
    wt_timestamp_t stop_ts;
    uint64_t insert_cnt, max_las_size, prepared_insert_cnt, stop_txnid;
    uint32_t mementos_cnt, btree_id, i, slot;
    uint8_t *p;
    bool las_key_saved, local_txn;

    mementop = NULL;
    session = (WT_SESSION_IMPL *)cursor->session;
    conn = S2C(session);
    WT_CLEAR(las_value);
    saved_isolation = 0; /*[-Wconditional-uninitialized] */
    insert_cnt = prepared_insert_cnt = 0;
    mementos_cnt = 0;
    btree_id = btree->id;
    local_txn = false;

    if (!btree->lookaside_entries)
        btree->lookaside_entries = true;

    /* Wrap all the updates in a transaction. */
    WT_ERR(__wt_txn_begin(session, NULL));
    __las_set_isolation(session, &saved_isolation);
    local_txn = true;

    /* Ensure enough room for a column-store key without checking. */
    WT_ERR(__wt_scr_alloc(session, WT_INTPACK64_MAXSIZE, &key));

    WT_ERR(__wt_scr_alloc(session, 0, &mementos));

    /* Inserts should be on the same page absent a split, search any pinned leaf page. */
    F_SET(cursor, WT_CURSTD_UPDATE_LOCAL);

    /* Enter each update in the boundary's list into the lookaside store. */
    for (i = 0, list = multi->supd; i < multi->supd_entries; ++i, ++list) {
        /* Lookaside table key component: source key. */
        switch (page->type) {
        case WT_PAGE_COL_FIX:
        case WT_PAGE_COL_VAR:
            p = key->mem;
            WT_ERR(__wt_vpack_uint(&p, 0, WT_INSERT_RECNO(list->ins)));
            key->size = WT_PTRDIFF(p, key->data);
            break;
        case WT_PAGE_ROW_LEAF:
            if (list->ins == NULL) {
                WT_WITH_BTREE(
                  session, btree, ret = __wt_row_leaf_key(session, page, list->ripcip, key, false));
                WT_ERR(ret);
            } else {
                key->data = WT_INSERT_KEY(list->ins);
                key->size = WT_INSERT_KEY_SIZE(list->ins);
            }
            break;
        default:
            WT_ERR(__wt_illegal_value(session, page->type));
        }

        /*
         * Lookaside table value component: update reference. Updates come from the row-store insert
         * list (an inserted item), or update array (an update to an original on-page item), or from
         * a column-store insert list (column-store format has no update array, the insert list
         * contains both inserted items and updates to original on-page items). When rolling forward
         * a modify update from an original on-page item, we need an on-page slot so we can find the
         * original on-page item. When rolling forward from an inserted item, no on-page slot is
         * possible.
         */
        slot = UINT32_MAX; /* Impossible slot */
        if (list->ripcip != NULL)
            slot = page->type == WT_PAGE_ROW_LEAF ? WT_ROW_SLOT(page, list->ripcip) :
                                                    WT_COL_SLOT(page, list->ripcip);
        first_upd = list->ins == NULL ? page->modify->mod_row_update[slot] : list->ins->upd;

        /*
         * Trim any updates before writing to lookaside. This saves wasted work, but is also
         * necessary because the reconciliation only resolves existing birthmarks if they aren't
         * obsolete.
         */
        WT_WITH_BTREE(
          session, btree, upd = __wt_update_obsolete_check(session, page, first_upd, true));
        __wt_free_update_list(session, &upd);
        upd = first_upd;

        /*
         * It's not OK for the update list to contain a birthmark on entry - we will generate one
         * below if necessary.
         */
        WT_ASSERT(session, __wt_count_birthmarks(first_upd) == 0);

        las_key_saved = false;

        /*
         * If the page being lookaside evicted has previous lookaside content associated with it,
         * there is a possibility that the update written to the disk is external (i.e. it came from
         * the existing lookaside content and not from in-memory updates). Since this update won't
         * be a part of the saved update list, we need to write a birthmark for it, separate from
         * processing of the saved updates.
         */
        WT_ASSERT(session, list->onpage_upd.upd == NULL || list->onpage_upd.ext == 0);
        if (list->onpage_upd.ext != 0) {
            /* Extend the buffer if needed */
            WT_ERR(__wt_buf_extend(session, mementos, (mementos_cnt + 1) * sizeof(WT_KEY_MEMENTO)));
            mementop = (WT_KEY_MEMENTO *)mementos->mem + mementos_cnt;
            mementop->txnid = list->onpage_upd.txnid;
            mementop->durable_ts = list->onpage_upd.durable_ts;
            mementop->start_ts = list->onpage_upd.start_ts;
            mementop->prepare_state = list->onpage_upd.prepare_state;
            /* Copy the key as well for reference. */
            WT_CLEAR(mementop->key);
            WT_ERR(__wt_buf_set(session, &mementop->key, key->data, key->size));
            mementos_cnt++;

            las_key_saved = true;
        }

        /*
         * The most recent update in the list never make into WT history. Only the earlier updates
         * in the list will make into WT history. All the entries in the WT history must have a stop
         * timestamp.
         */
        stop_ts = WT_TS_MAX;
        stop_txnid = WT_TXN_MAX;

        /*
         * Walk the list of updates, storing each key/value pair into the lookaside table. Skip
         * aborted items (there's no point to restoring them), and assert we never see a reserved
         * item.
         */
        do {
            if (upd->txnid == WT_TXN_ABORTED)
                continue;

            /* We have at least one LAS record from this key, save a copy of the key */
            if (!las_key_saved) {
                /* Extend the buffer if needed */
                WT_ERR(
                  __wt_buf_extend(session, mementos, (mementos_cnt + 1) * sizeof(WT_KEY_MEMENTO)));
                mementop = (WT_KEY_MEMENTO *)mementos->mem + mementos_cnt;
                WT_CLEAR(mementop->key);
                WT_ERR(__wt_buf_set(session, &mementop->key, key->data, key->size));
                mementop->txnid = WT_TXN_ABORTED;
                las_key_saved = true;
                mementos_cnt++;
            }

            switch (upd->type) {
            case WT_UPDATE_MODIFY:
            case WT_UPDATE_STANDARD:
                las_value.data = upd->data;
                las_value.size = upd->size;
                break;
            case WT_UPDATE_TOMBSTONE:
                las_value.size = 0;
                break;
            default:
                /*
                 * It is never OK to see a birthmark here - it would be referring to the wrong page
                 * image.
                 */
                WT_ERR(__wt_illegal_value(session, upd->type));
            }

            cursor->set_key(cursor, btree_id, key, upd->start_ts, upd->txnid, stop_ts, stop_txnid);

            /*
             * If saving a non-zero length value on the page, save a birthmark instead of
             * duplicating it in the lookaside table. (We check the length because row-store doesn't
             * write zero-length data items.)
             */
            if (upd == list->onpage_upd.upd && upd->size > 0) {
                WT_ASSERT(
                  session, upd->type == WT_UPDATE_STANDARD || upd->type == WT_UPDATE_MODIFY);
                /* Make sure that we are generating a birthmark for an in-memory update. */
                mementop->txnid = upd->txnid;
                mementop->durable_ts = upd->durable_ts;
                mementop->start_ts = upd->start_ts;
                mementop->prepare_state = upd->prepare_state;

                /*
                 * Store the current update commit timestamp and transaction id, these are the stop
                 * timestamp and transaction id's for the next record in the update list.
                 */
                stop_ts = upd->start_ts;
                stop_txnid = upd->txnid;

                /* Do not put birthmarks into the lookaside. */
                continue;
            }

            if (upd->prepare_state == WT_PREPARE_INPROGRESS)
                ++prepared_insert_cnt;

            /*
             * If we encounter a modify with preceding updates that have the same timestamp and
             * transaction id, they need to be squashed into a single lookaside entry to avoid
             * displacing each other.
             */
            if (upd->type == WT_UPDATE_MODIFY && upd->next != NULL &&
              upd->start_ts == upd->next->start_ts && upd->txnid == upd->next->txnid) {
                WT_ERR(__las_squash_modifies(session, cursor, &upd));
                ++insert_cnt;
                continue;
            }

            cursor->set_value(cursor, upd->durable_ts, upd->prepare_state, upd->type, &las_value);

            /*
             * Using update instead of insert so the page stays pinned and can be searched before
             * the tree.
             */
            WT_ERR(cursor->update(cursor));
            ++insert_cnt;

            /*
             * Store the current update commit timestamp and transaction id, these are the stop
             * timestamp and transaction id's for the next record in the update list.
             */
            stop_ts = upd->start_ts;
            stop_txnid = upd->txnid;

            /*
             * If we encounter subsequent updates with the same timestamp, skip them to avoid
             * clobbering the entry that we just wrote to lookaside. Only the last thing in the
             * update list for that timestamp and transaction id matters anyway.
             */
            while (upd->next != NULL && upd->start_ts == upd->next->start_ts &&
              upd->txnid == upd->next->txnid)
                upd = upd->next;
        } while ((upd = upd->next) != NULL);
    }

    WT_ERR(__wt_block_manager_named_size(session, WT_LAS_FILE, &las_size));
    WT_STAT_CONN_SET(session, cache_lookaside_ondisk, las_size);
    max_las_size = ((WT_CURSOR_BTREE *)cursor)->btree->file_max;
    if (max_las_size != 0 && (uint64_t)las_size > max_las_size)
        WT_PANIC_ERR(session, WT_PANIC, "WiredTigerLAS: file size of %" PRIu64
                                        " exceeds maximum "
                                        "size %" PRIu64,
          (uint64_t)las_size, max_las_size);

err:
    /* Resolve the transaction. */
    if (local_txn) {
        if (ret == 0)
            ret = __wt_txn_commit(session, NULL);
        else
            WT_TRET(__wt_txn_rollback(session, NULL));
        __las_restore_isolation(session, saved_isolation);
        F_CLR(cursor, WT_CURSTD_UPDATE_LOCAL);

        /* Adjust the entry count. */
        if (ret == 0) {
            (void)__wt_atomic_add64(&conn->cache->las_insert_count, insert_cnt);
            WT_STAT_CONN_INCRV(
              session, txn_prepared_updates_lookaside_inserts, prepared_insert_cnt);
        }
    }

    __las_restore_isolation(session, saved_isolation);

    if (ret == 0 && mementos_cnt > 0)
        ret = __wt_calloc(session, mementos_cnt, sizeof(WT_KEY_MEMENTO), &multi->page_las.mementos);

    if (ret == 0 && (insert_cnt > 0 || mementos_cnt > 0)) {
        WT_ASSERT(session, multi->page_las.max_txn != WT_TXN_NONE);
        multi->has_las = true;
        multi->page_las.has_prepares |= prepared_insert_cnt > 0;
        if (mementos_cnt > 0) {
            memcpy(multi->page_las.mementos, mementos->mem, mementos_cnt * sizeof(WT_KEY_MEMENTO));
            multi->page_las.mementos_cnt = mementos_cnt;
        }
        __las_insert_updates_verbose(session, btree, multi);
    }

    __wt_scr_free(session, &key);
    /* Free all the key mementos if there was a failure */
    if (ret != 0)
        for (i = 0, mementop = mementos->mem; i < mementos_cnt; i++, mementop++)
            __wt_buf_free(session, &mementop->key);
    __wt_scr_free(session, &mementos);
    return (ret);
}

/*
 * __wt_las_cursor_position --
 *     Position a lookaside cursor at the end of a set of updates for a given btree id, record key
 *     and timestamp. There may be no lookaside entries for the given btree id and record key if
 *     they have been removed by WT_CONNECTION::rollback_to_stable.
 */
int
__wt_las_cursor_position(WT_SESSION_IMPL *session, WT_CURSOR *cursor, uint32_t btree_id,
  WT_ITEM *key, wt_timestamp_t timestamp)
{
    WT_ITEM las_key;
    WT_TIME_PAIR las_start, las_stop;
    uint32_t las_btree_id;
    int cmp, exact;

    /*
     * Because of the special visibility rules for lookaside, a new key can appear in between our
     * search and the set of updates that we're interested in. Keep trying until we find it.
     */
    for (;;) {
        cursor->set_key(cursor, btree_id, key, timestamp, WT_TXN_MAX, WT_TS_MAX, WT_TXN_MAX);
        WT_RET(cursor->search_near(cursor, &exact));
        if (exact > 0)
            WT_RET(cursor->prev(cursor));

        /*
         * Because of the special visibility rules for lookaside, a new key can appear in between
         * our search and the set of updates we're interested in. Keep trying while we have a key
         * lower than we expect.
         *
         * There may be no lookaside entries for the given btree id and record key if they have been
         * removed by WT_CONNECTION::rollback_to_stable.
         */
        WT_CLEAR(las_key);
        WT_RET(cursor->get_key(cursor, &las_btree_id, &las_key, &las_start.timestamp,
          &las_start.txnid, &las_stop.timestamp, &las_stop.txnid));
        if (las_btree_id < btree_id)
            return (0);
        else if (las_btree_id == btree_id) {
            WT_RET(__wt_compare(session, NULL, &las_key, key, &cmp));
            if (cmp < 0)
                return (0);
            if (cmp == 0 && las_start.timestamp <= timestamp)
                return (0);
        }
    }

    /* NOTREACHED */
}

/*
 * __wt_las_remove_dropped --
 *     Remove an opened btree ID if it is in the dropped table.
 */
void
__wt_las_remove_dropped(WT_SESSION_IMPL *session)
{
    WT_BTREE *btree;
    WT_CACHE *cache;
    u_int i, j;

    btree = S2BT(session);
    cache = S2C(session)->cache;

    __wt_spin_lock(session, &cache->las_sweep_lock);
    for (i = 0; i < cache->las_dropped_next && cache->las_dropped[i] != btree->id; i++)
        ;

    if (i < cache->las_dropped_next) {
        cache->las_dropped_next--;
        for (j = i; j < cache->las_dropped_next; j++)
            cache->las_dropped[j] = cache->las_dropped[j + 1];
    }
    __wt_spin_unlock(session, &cache->las_sweep_lock);
}

/*
 * __wt_las_save_dropped --
 *     Save a dropped btree ID to be swept from the lookaside table.
 */
int
__wt_las_save_dropped(WT_SESSION_IMPL *session)
{
    WT_BTREE *btree;
    WT_CACHE *cache;
    WT_DECL_RET;

    btree = S2BT(session);
    cache = S2C(session)->cache;

    __wt_spin_lock(session, &cache->las_sweep_lock);
    WT_ERR(__wt_realloc_def(
      session, &cache->las_dropped_alloc, cache->las_dropped_next + 1, &cache->las_dropped));
    cache->las_dropped[cache->las_dropped_next++] = btree->id;
err:
    __wt_spin_unlock(session, &cache->las_sweep_lock);
    return (ret);
}

/*
 * __las_sweep_count --
 *     Calculate how many records to examine per sweep step.
 */
static inline uint64_t
__las_sweep_count(WT_CACHE *cache)
{
    uint64_t las_entry_count;

    /*
     * The sweep server is a slow moving thread. Try to review the entire lookaside table once every
     * 5 minutes.
     *
     * The reason is because the lookaside table exists because we're seeing cache/eviction pressure
     * (it allows us to trade performance and disk space for cache space), and it's likely lookaside
     * blocks are being evicted, and reading them back in doesn't help things. A trickier, but
     * possibly better, alternative might be to review all lookaside blocks in the cache in order to
     * get rid of them, and slowly review lookaside blocks that have already been evicted.
     *
     * Put upper and lower bounds on the calculation: since reads of pages with lookaside entries
     * are blocked during sweep, make sure we do some work but don't block reads for too long.
     */
    las_entry_count = __las_entry_count(cache);
    return (
      (uint64_t)WT_MAX(WT_LAS_SWEEP_ENTRIES, las_entry_count / (5 * WT_MINUTE / WT_LAS_SWEEP_SEC)));
}

/*
 * __las_sweep_init --
 *     Prepare to start a lookaside sweep.
 */
static int
__las_sweep_init(WT_SESSION_IMPL *session, WT_CURSOR *las_cursor)
{
    WT_CACHE *cache;
    WT_DECL_RET;
    WT_ITEM max_key;
    WT_TIME_PAIR max_start, max_stop;
    uint32_t max_btree_id;
    u_int i;

    cache = S2C(session)->cache;
    WT_CLEAR(max_key);

    __wt_spin_lock(session, &cache->las_sweep_lock);

    /*
     * If no files have been dropped and the lookaside file is empty, there's nothing to do.
     */
    if (cache->las_dropped_next == 0 && __wt_las_empty(session))
        WT_ERR(WT_NOTFOUND);

    /* Find the max key for the current sweep. */
    WT_ERR(las_cursor->reset(las_cursor));
    WT_ERR(las_cursor->prev(las_cursor));
    WT_ERR(las_cursor->get_key(las_cursor, &max_btree_id, &max_key, &max_start.timestamp,
      &max_start.txnid, &max_stop.timestamp, &max_stop.txnid));
    WT_ERR(__wt_buf_set(session, &cache->las_max_key, max_key.data, max_key.size));
    cache->las_max_btree_id = max_btree_id;
    cache->las_max_timestamp = max_start.timestamp;
    cache->las_max_txnid = max_start.txnid;

    /* Scan the btree IDs to find min/max. */
    cache->las_sweep_dropmin = UINT32_MAX;
    cache->las_sweep_dropmax = 0;
    for (i = 0; i < cache->las_dropped_next; i++) {
        cache->las_sweep_dropmin = WT_MIN(cache->las_sweep_dropmin, cache->las_dropped[i]);
        cache->las_sweep_dropmax = WT_MAX(cache->las_sweep_dropmax, cache->las_dropped[i]);
    }

    /* Initialize the bitmap. */
    __wt_free(session, cache->las_sweep_dropmap);
    WT_ERR(__bit_alloc(
      session, 1 + cache->las_sweep_dropmax - cache->las_sweep_dropmin, &cache->las_sweep_dropmap));
    for (i = 0; i < cache->las_dropped_next; i++)
        __bit_set(cache->las_sweep_dropmap, cache->las_dropped[i] - cache->las_sweep_dropmin);

    /* Clear the list of btree IDs. */
    cache->las_dropped_next = 0;

err:
    __wt_spin_unlock(session, &cache->las_sweep_lock);
    return (ret);
}

/*
 * __las_sweep_remove --
 *     Remove a record for the sweep cursor.
 */
static int
__las_sweep_remove(WT_CURSOR *cursor)
{
    WT_ITEM remove_key, remove_value;
    WT_SESSION_IMPL *session;
    wt_timestamp_t remove_durable_timestamp, remove_timestamp;
    uint64_t remove_txnid;
    uint32_t remove_btree_id;
    uint8_t remove_prepare_state, remove_upd_type;

    session = (WT_SESSION_IMPL *)cursor->session;

    /* Produce the verbose output of a remove record if configured. */
    if (WT_VERBOSE_ISSET(session, WT_VERB_LOOKASIDE_ACTIVITY)) {
        WT_RET(
          cursor->get_key(cursor, &remove_btree_id, &remove_key, &remove_timestamp, &remove_txnid));
        WT_RET(cursor->get_value(cursor, &remove_durable_timestamp, &remove_prepare_state,
          &remove_upd_type, &remove_value));

        __wt_verbose(session, WT_VERB_LOOKASIDE_ACTIVITY,
          "Sweep removing lookaside entry with "
          "btree ID: %" PRIu32 " key size: %" WT_SIZET_FMT " prepared state: %" PRIu8
          " record type: %" PRIu8 " durable timestamp: %" PRIu64 " transaction ID: %" PRIu64,
          remove_btree_id, remove_key.size, remove_prepare_state, remove_upd_type,
          remove_durable_timestamp, remove_txnid);
    }
    return (cursor->remove(cursor));
}

/*
 * __wt_las_sweep --
 *     Sweep the lookaside table.
 */
int
__wt_las_sweep(WT_SESSION_IMPL *session)
{
    WT_CACHE *cache;
    WT_CURSOR *cursor;
    WT_DECL_ITEM(saved_key);
    WT_DECL_RET;
    WT_ITEM las_key, las_value, remove_key;
    WT_ITEM *sweep_key;
    WT_TIME_PAIR las_start, las_stop, remove_start, remove_stop, saved;
    wt_timestamp_t durable_timestamp;
    uint64_t cnt, remove_cnt, pending_remove_cnt, visit_cnt;
    uint32_t las_btree_id, remove_btree_id, saved_btree_id, session_flags;
    uint8_t prepare_state, upd_type;
    int cmp, notused;
    bool globally_visible_ondisk_value, key_change, local_txn, locked, prev_rec_verified;

    cache = S2C(session)->cache;
    cursor = NULL;
    sweep_key = &cache->las_sweep_key;
    remove_cnt = 0;
    session_flags = 0; /* [-Werror=maybe-uninitialized] */
    local_txn = locked = false;

    WT_RET(__wt_scr_alloc(session, 0, &saved_key));

    /*
     * Prevent other threads removing entries from underneath the sweep.
     */
    __wt_writelock(session, &cache->las_sweepwalk_lock);
    locked = true;

    /*
     * Allocate a cursor and wrap all the updates in a transaction. We should have our own lookaside
     * cursor.
     */
    __wt_las_cursor(session, &cursor, &session_flags);
    WT_ASSERT(session, cursor->session == &session->iface);
    WT_ERR(__wt_txn_begin(session, NULL));
    local_txn = true;

    /* Encourage a race */
    __wt_timing_stress(session, WT_TIMING_STRESS_LOOKASIDE_SWEEP);

    /*
     * When continuing a sweep, position the cursor using the key from the last call (we don't care
     * if we're before or after the key, either side is fine).
     *
     * Otherwise, we're starting a new sweep, gather the list of trees to sweep.
     */
    if (sweep_key->size != 0) {
        __wt_cursor_set_raw_key(cursor, sweep_key);
        ret = cursor->search_near(cursor, &notused);

        /*
         * Don't search for the same key twice; if we don't set a new key below, it's because we've
         * reached the end of the table and we want the next pass to start at the beginning of the
         * table. Searching for the same key could leave us stuck at the end of the table,
         * repeatedly checking the same rows.
         */
        __wt_buf_free(session, sweep_key);
    } else {
        ret = __las_sweep_init(session, cursor);

        /* Reset the cursor to start from first record */
        cursor->reset(cursor);
    }
    if (ret != 0)
        goto srch_notfound;

    cnt = __las_sweep_count(cache);
    pending_remove_cnt = 0;
    visit_cnt = 0;
    saved_btree_id = 0;
    saved.timestamp = WT_TS_NONE;
    saved.txnid = WT_TXN_NONE;
    globally_visible_ondisk_value = false;

    /* Walk the file. */
    while ((ret = cursor->next(cursor)) == 0) {
        WT_ERR(cursor->get_key(cursor, &las_btree_id, &las_key, &las_start.timestamp,
          &las_start.txnid, &las_stop.timestamp, &las_stop.txnid));

        /*
         * Check for the max range for the current sweep is reached.
         *
         * The LAS key is in the order of btree id, key, timestamp and transaction id. The current
         * LAS record key is compared against the max key that is stored to find out whether it
         * reached the max range for the current sweep.
         */
        WT_ERR(__wt_compare(session, NULL, &las_key, &cache->las_max_key, &cmp));
        if (las_btree_id > cache->las_max_btree_id ||
          (las_btree_id == cache->las_max_btree_id && cmp > 0) ||
          (las_btree_id == cache->las_max_btree_id && cmp == 0 &&
              las_start.timestamp > cache->las_max_timestamp) ||
          (las_btree_id == cache->las_max_btree_id && cmp == 0 &&
              las_start.timestamp == cache->las_max_timestamp &&
              las_start.txnid > cache->las_max_txnid))
            /* It is a success scenario, don't return error */
            goto err;

        /*
         * Signal to stop if the cache is stuck: we are ignoring the cache size while scanning the
         * lookaside table, so we're making things worse.
         */
        if (__wt_cache_stuck(session))
            cnt = 0;

        /*
         * We only want to break between key blocks. Stop if we've processed enough entries either
         * all we wanted or enough and there is a reader waiting and we're on a key boundary.
         */
        ++visit_cnt;
        if (cnt == 0 || (visit_cnt > WT_LAS_SWEEP_ENTRIES && cache->las_reader))
            break;
        if (cnt > 0)
            --cnt;

        /*
         * If the entry belongs to a dropped tree, discard it.
         *
         * Cursor opened overwrite=true: won't return WT_NOTFOUND should another thread remove the
         * record before we do (not expected for dropped trees), and the cursor remains positioned
         * in that case.
         */
        if (las_btree_id >= cache->las_sweep_dropmin && las_btree_id <= cache->las_sweep_dropmax &&
          __bit_test(cache->las_sweep_dropmap, las_btree_id - cache->las_sweep_dropmin)) {
            WT_ERR(__las_sweep_remove(cursor));
            ++remove_cnt;
            saved_key->size = 0;
            continue;
        }

        /* The remaining tests require the value. */
        WT_ERR(
          cursor->get_value(cursor, &durable_timestamp, &prepare_state, &upd_type, &las_value));

        /*
         * Expect an update entry with:
         *  1. Not in a prepare locked state
         *  2. Durable timestamp not max timestamp
         *  3. For an in-progress prepared update, durable timestamp should be zero and no
         *     restriction on durable timestamp value for other updates.
         */
        WT_ERR_ASSERT(session, prepare_state != WT_PREPARE_LOCKED, EINVAL,
          "LAS prepared record is in locked state");
        WT_ERR_ASSERT(session, durable_timestamp != WT_TS_MAX, EINVAL,
          "LAS record durable timestamp is set as MAX timestamp");
        WT_ERR_ASSERT(
          session,
          ((prepare_state != WT_PREPARE_INPROGRESS || durable_timestamp == 0) &&
            (prepare_state == WT_PREPARE_INPROGRESS || durable_timestamp >= las_start.timestamp)),
          EINVAL,
          "Either LAS record is a in-progress prepared update with wrong durable timestamp or not"
          " a prepared update with wrong durable timestamp (prepared state: %" PRIu8
          " durable timestamp: %" PRIu64 ")",
          prepare_state, durable_timestamp);

        /*
         * Check to see if the page or key has changed this iteration, and if they have, setup
         * context for safely removing obsolete updates.
         */
        key_change = false;
        if (las_btree_id != saved_btree_id || saved_key->size != las_key.size ||
          memcmp(saved_key->data, las_key.data, las_key.size) != 0)
            key_change = true;

        /*
         * Remove the previous obsolete entries whenever the scan moves into next key or the current
         * LAS record is not a modify record.
         *
         * The special ondisk value i.e both timestamp and transaction id values as 0 and is the
         * last record of the key that is globally visible is present in the LAS can only be removed
         * whenever a next record of the same key gets removed. This is to protect the cases where
         * we may still need the older ondisk value for some scenarios. The draw back is that this
         * record never be removed until more updates happens on that key, but these scenarios are
         * minimal.
         *
         * TODO: Better key/value pair design that let you know that on disk image of the key is
         * globally visible will simplify the logic of removing the entire LAS records for that key.
         */
        if ((globally_visible_ondisk_value ? pending_remove_cnt > 1 : pending_remove_cnt > 0) &&
          (key_change || upd_type != WT_UPDATE_MODIFY)) {
            prev_rec_verified = false;
            globally_visible_ondisk_value = false;
            while (pending_remove_cnt > 0) {
                WT_ERR(cursor->prev(cursor));

                /*
                 * New inserts of any existing LAS record keys or higher keys are only possible
                 * at the end of the key boundary. There may be a case that sweep server can see the
                 * newly inserted LAS record while scanning backwards to remove the obsolete records
                 * (when it switches to the next key in the LAS file). To avoid such problems, the
                 * first previous key that is getting removed must be verified the saved key.
                 */
                if (key_change && !prev_rec_verified) {
                    WT_ERR(cursor->get_key(cursor, &remove_btree_id, &remove_key,
                      &remove_start.timestamp, &remove_start.txnid, &remove_stop.timestamp,
                      &remove_stop.txnid));
                    if (remove_btree_id != saved_btree_id || saved_key->size != remove_key.size ||
                      memcmp(saved_key->data, remove_key.data, remove_key.size) != 0 ||
                      saved.timestamp != remove_start.timestamp ||
                      saved.txnid != remove_start.txnid) {
                        pending_remove_cnt = 0;
                        break;
                    }
                    prev_rec_verified = true;
                }

                WT_ERR(__las_sweep_remove(cursor));
                ++remove_cnt;
                --pending_remove_cnt;
            }

            WT_ERR(cursor->next(cursor));
        }

        /*
         * There are several conditions that need to be met before we choose to remove a lookaside
         * entry:
         *  1. If there exists a last checkpoint timestamp, then the lookaside record timestamp must
         *     be less than last checkpoint timestamp.
         *  2. The entry is globally visible.
         *  3. The entry wasn't from a prepared transaction.
         */
        if (((S2C(session)->txn_global.last_ckpt_timestamp != WT_TS_NONE) &&
              (las_start.timestamp > S2C(session)->txn_global.last_ckpt_timestamp)) ||
          !__wt_txn_visible_all(session, las_start.txnid, durable_timestamp) ||
          prepare_state == WT_PREPARE_INPROGRESS) {
            /*
             * TODO: In case if there exists any pending remove count records that can be removed,
             * it is better to squash them to frame a UPDATE record and remove the obsolete records.
             */
            pending_remove_cnt = 0;
            continue;
        }

        /* Save the key whenever we get a new key. */
        if (key_change) {
            saved_btree_id = las_btree_id;
            WT_ERR(__wt_buf_set(session, saved_key, las_key.data, las_key.size));
            pending_remove_cnt = 0;
            globally_visible_ondisk_value = false;
        }
        saved.timestamp = las_start.timestamp;
        saved.txnid = las_start.txnid;

        /* Mark the flag if the LAS record is a previous ondisk image value */
        if (las_start.timestamp == WT_TS_NONE && las_start.txnid == WT_TXN_NONE)
            globally_visible_ondisk_value = true;

        pending_remove_cnt++;
    }

    /*
     * If the loop terminates after completing a work unit, we will continue the table sweep next
     * time. Get a local copy of the sweep key to initialize the cursor position.
     */
    if (ret == 0) {
        WT_ERR(__wt_cursor_get_raw_key(cursor, sweep_key));
        if (!WT_DATA_IN_ITEM(sweep_key))
            WT_ERR(__wt_buf_set(session, sweep_key, sweep_key->data, sweep_key->size));
    }

srch_notfound:
    WT_ERR_NOTFOUND_OK(ret);

    if (0) {
err:
        __wt_buf_free(session, sweep_key);
    }
    if (local_txn) {
        if (ret == 0)
            ret = __wt_txn_commit(session, NULL);
        else
            WT_TRET(__wt_txn_rollback(session, NULL));
        if (ret == 0)
            (void)__wt_atomic_add64(&cache->las_remove_count, remove_cnt);
    }

    WT_TRET(__wt_las_cursor_close(session, &cursor, session_flags));

    if (locked)
        __wt_writeunlock(session, &cache->las_sweepwalk_lock);

    __wt_scr_free(session, &saved_key);

    return (ret);
}

/*
 * __find_birthmark_update --
 *     A helper function to find a birthmark update for a given key. If there is no birthmark record
 *     (either no lookaside content or the most recent value was instantiated) the update pointer
 *     will be set to null.
 */
static void
__find_birthmark_update(WT_CURSOR_BTREE *cbt, WT_UPDATE **updp)
{
    WT_PAGE *page;
    WT_UPDATE *upd;

    page = cbt->ref->page;

    upd = NULL;
    if (cbt->ins != NULL)
        upd = cbt->ins->upd;
    else if (cbt->btree->type == BTREE_ROW && page->modify != NULL &&
      page->modify->mod_row_update != NULL)
        upd = page->modify->mod_row_update[cbt->slot];

    for (; upd != NULL && upd->type != WT_UPDATE_BIRTHMARK; upd = upd->next)
        ;

    *updp = upd;
}

/*
 * __wt_find_lookaside_upd --
 *     Scan the lookaside for a record the btree cursor wants to position on. Create an update for
 *     the record and return to the caller. The caller may choose to optionally allow prepared
 *     updates to be returned regardless of whether prepare is being ignored globally. Otherwise, a
 *     prepare conflict will be returned upon reading a prepared update.
 */
int
__wt_find_lookaside_upd(
  WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt, WT_UPDATE **updp, bool allow_prepare)
{
    WT_CACHE *cache;
    WT_CURSOR *las_cursor;
    WT_DECL_ITEM(las_key);
    WT_DECL_ITEM(las_value);
    WT_DECL_RET;
    WT_ITEM *key, _key;
    WT_MODIFY_VECTOR modifies;
    WT_TIME_PAIR las_start, las_start_tmp, las_stop, las_stop_tmp;
    WT_TXN *txn;
    WT_UPDATE *birthmark_upd, *mod_upd, *upd;
    wt_timestamp_t durable_timestamp, durable_timestamp_tmp, read_timestamp;
    size_t notused, size;
    uint64_t recno;
    uint32_t las_btree_id, session_flags;
    uint8_t prepare_state, prepare_state_tmp, *p, recno_key[WT_INTPACK64_MAXSIZE], upd_type;
    const uint8_t *recnop;
    int cmp;
    bool modify, sweep_locked;

    *updp = NULL;

    cache = S2C(session)->cache;
    las_cursor = NULL;
    key = NULL;
    mod_upd = upd = NULL;
    __wt_modify_vector_init(session, &modifies);
    txn = &session->txn;
    notused = size = 0;
    las_btree_id = S2BT(session)->id;
    session_flags = 0; /* [-Werror=maybe-uninitialized] */
    WT_NOT_READ(modify, false);
    sweep_locked = false;

    /* Row-store has the key available, create the column-store key on demand. */
    switch (cbt->btree->type) {
    case BTREE_ROW:
        key = &cbt->iface.key;
        break;
    case BTREE_COL_FIX:
    case BTREE_COL_VAR:
        p = recno_key;
        WT_RET(__wt_vpack_uint(&p, 0, cbt->recno));
        WT_CLEAR(_key);
        _key.data = recno_key;
        _key.size = WT_PTRDIFF(p, recno_key);
        key = &_key;
    }

    /* Allocate buffers for the lookaside key/value. */
    WT_ERR(__wt_scr_alloc(session, 0, &las_key));
    WT_ERR(__wt_scr_alloc(session, 0, &las_value));

    /* Open a lookaside table cursor. */
    __wt_las_cursor(session, &las_cursor, &session_flags);

    /*
     * The lookaside records are in key and update order, that is, there will be a set of in-order
     * updates for a key, then another set of in-order updates for a subsequent key. We scan through
     * all of the updates till we locate the one we want.
     */
    cache->las_reader = true;
    __wt_readlock(session, &cache->las_sweepwalk_lock);
    cache->las_reader = false;
    sweep_locked = true;

    /*
     * After positioning our cursor, we're stepping backwards to find the correct update. Since the
     * timestamp is part of the key, our cursor needs to go from the newest record (further in the
     * las) to the oldest (earlier in the las) for a given key.
     */
    read_timestamp = allow_prepare ? txn->prepare_timestamp : txn->read_timestamp;
    ret = __wt_las_cursor_position(session, las_cursor, las_btree_id, key, read_timestamp);
    for (; ret == 0; ret = las_cursor->prev(las_cursor)) {
        WT_ERR(las_cursor->get_key(las_cursor, &las_btree_id, las_key, &las_start.timestamp,
          &las_start.txnid, &las_stop.timestamp, &las_stop.txnid));

        /* Stop before crossing over to the next btree */
        if (las_btree_id != S2BT(session)->id)
            break;

        /*
         * Keys are sorted in an order, skip the ones before the desired key, and bail out if we
         * have crossed over the desired key and not found the record we are looking for.
         */
        WT_ERR(__wt_compare(session, NULL, las_key, key, &cmp));
        if (cmp != 0)
            break;

        /*
         * It is safe to assume that we're reading the updates newest to the oldest. We can quit
         * searching after finding the newest visible record.
         */
        if (!__wt_txn_visible(session, las_start.txnid, las_start.timestamp))
            continue;

        WT_ERR(las_cursor->get_value(
          las_cursor, &durable_timestamp, &prepare_state, &upd_type, las_value));

        /*
         * Found a visible record, return success unless it is prepared and we are not ignoring
         * prepared.
         *
         * It's necessary to explicitly signal a prepare conflict so that the callers don't fallback
         * to using something from the update list.
         */
        if (prepare_state == WT_PREPARE_INPROGRESS &&
          !F_ISSET(&session->txn, WT_TXN_IGNORE_PREPARE) && !allow_prepare) {
            ret = WT_PREPARE_CONFLICT;
            break;
        }

        /* We do not have birthmarks in the lookaside anymore. */
        WT_ASSERT(session, upd_type != WT_UPDATE_BIRTHMARK);

        /*
         * Keep walking until we get a non-modify update. Once we get to that point, squash the
         * updates together.
         */
        if (upd_type == WT_UPDATE_MODIFY) {
            WT_NOT_READ(modify, true);
            __find_birthmark_update(cbt, &birthmark_upd);
            while (upd_type == WT_UPDATE_MODIFY) {
                WT_ERR(__wt_update_alloc(session, las_value, &mod_upd, &notused, upd_type));
                WT_ERR(__wt_modify_vector_push(&modifies, mod_upd));
                mod_upd = NULL;

                /*
                 * This loop can exit on three conditions:
                 * 1: We find a standard update in the lookaside table to apply the deltas to.
                 * 2: We find that the birthmark record for the given key is more recent than the
                 * current lookaside record.
                 * 3: We cross the key boundary meaning that the base update MUST be the on-disk
                 * value for that key. Note that if we hit the beginning of the lookaside table when
                 * backtracking, this equates to crossing a key boundary.
                 */
                WT_ERR_NOTFOUND_OK(las_cursor->prev(las_cursor));
                las_start_tmp.timestamp = WT_TS_NONE;
                las_start_tmp.txnid = WT_TXN_NONE;
                if (ret != WT_NOTFOUND) {
                    /*
                     * Make sure we use the temporary variants of these variables. We need to retain
                     * the timestamps of the original modify we saw.
                     *
                     * The regular case (1) is where we keep looking back into lookaside until we
                     * find a base update to apply the deltas on top of.
                     */
                    WT_ERR(las_cursor->get_key(las_cursor, &las_btree_id, las_key,
                      &las_start_tmp.timestamp, &las_start_tmp.txnid, &las_stop_tmp.timestamp,
                      &las_stop_tmp.txnid));

                    /*
                     * Another possibility (2) is where the birthmark that we instantiated the
                     * lookaside page with IS the base update that we should be applying the deltas
                     * to. If the cursor positions itself on a modify, we immediately traverse the
                     * update list to look for the birthmark update and compare its timestamp/txnid
                     * with the lookaside contents.
                     */
                    if (birthmark_upd != NULL && birthmark_upd->start_ts < read_timestamp &&
                      ((birthmark_upd->start_ts > las_start_tmp.timestamp) ||
                          (birthmark_upd->start_ts == las_start_tmp.timestamp &&
                            birthmark_upd->txnid > las_start_tmp.txnid))) {
                        upd_type = WT_UPDATE_STANDARD;
                        WT_ERR(__wt_buf_set(
                          session, las_value, birthmark_upd->data, birthmark_upd->size));
                        break;
                    }
                    WT_ERR(__wt_compare(session, NULL, las_key, key, &cmp));
                }

                /*
                 * The last possibility (3) is where the on-disk value is the base update but is not
                 * a birthmark record. This can happen if reconciliation occurred when ONLY that
                 * value existed. It will be not be a birthmark since we won't go through the
                 * lookaside eviction path in the case of a single value.
                 */
                if (ret == WT_NOTFOUND || las_btree_id != S2BT(session)->id || cmp != 0) {
                    upd_type = WT_UPDATE_STANDARD;
                    WT_ERR(__wt_value_return_buf(cbt, cbt->ref, las_value));
                    break;
                } else
                    WT_ASSERT(session,
                      __wt_txn_visible(session, las_start_tmp.txnid, las_start_tmp.timestamp));
                WT_ERR(las_cursor->get_value(
                  las_cursor, &durable_timestamp_tmp, &prepare_state_tmp, &upd_type, las_value));
            }
            WT_ASSERT(session, upd_type == WT_UPDATE_STANDARD);
            while (modifies.size > 0) {
                __wt_modify_vector_pop(&modifies, &mod_upd);
                WT_ERR(__wt_modify_apply_item(session, las_value, mod_upd->data, false));
                __wt_free_update_list(session, &mod_upd);
                mod_upd = NULL;
            }
            WT_STAT_CONN_INCR(session, cache_lookaside_read_squash);
        }

        /* Allocate an update structure for the record found. */
        WT_ERR(__wt_update_alloc(session, las_value, &upd, &size, upd_type));
        upd->txnid = las_start.txnid;
        upd->durable_ts = durable_timestamp;
        upd->start_ts = las_start.timestamp;
        upd->prepare_state = prepare_state;

        /*
         * When we find a prepared update in lookaside, we should add it to our update list and
         * subsequently delete the corresponding lookaside entry. If it gets committed, the
         * timestamp in the las key may differ so it's easier if we get rid of it now and rewrite
         * the entry on eviction/commit/rollback.
         */
        if (prepare_state == WT_PREPARE_INPROGRESS) {
            WT_ASSERT(session, !modify);
            switch (cbt->ref->page->type) {
            case WT_PAGE_COL_FIX:
            case WT_PAGE_COL_VAR:
                recnop = las_key->data;
                WT_ERR(__wt_vunpack_uint(&recnop, 0, &recno));
                WT_ERR(__wt_col_modify(cbt, recno, NULL, upd, WT_UPDATE_STANDARD, false));
                break;
            case WT_PAGE_ROW_LEAF:
                WT_ERR(__wt_row_modify(cbt, las_key, NULL, upd, WT_UPDATE_STANDARD, false));
                break;
            }

            ret = las_cursor->remove(las_cursor);
            if (ret != 0)
                WT_PANIC_ERR(session, ret,
                  "initialised prepared update but was unable to remove the corresponding entry "
                  "from lookaside");

            /* This is going in our update list so it should be accounted for in cache usage. */
            __wt_cache_page_inmem_incr(session, cbt->ref->page, size);
        } else
            /*
             * We're not keeping this in our update list as we want to get rid of it after the read
             * has been dealt with. Mark this update as external and to be discarded when not
             * needed.
             */
            upd->ext = 1;
        *updp = upd;

        /* We are done, we found the record we were searching for */
        break;
    }
    WT_ERR_NOTFOUND_OK(ret);

err:
    if (sweep_locked)
        __wt_readunlock(session, &cache->las_sweepwalk_lock);

    __wt_scr_free(session, &las_key);
    __wt_scr_free(session, &las_value);

    WT_TRET(__wt_las_cursor_close(session, &las_cursor, session_flags));
    __wt_free_update_list(session, &mod_upd);
    while (modifies.size > 0) {
        __wt_modify_vector_pop(&modifies, &upd);
        __wt_free_update_list(session, &upd);
    }
    __wt_modify_vector_free(&modifies);

    if (ret == 0) {
        /* Couldn't find a record. */
        if (upd == NULL) {
            ret = WT_NOTFOUND;
            WT_STAT_CONN_INCR(session, cache_lookaside_read_miss);
        } else {
            WT_STAT_CONN_INCR(session, cache_lookaside_read);
            WT_STAT_DATA_INCR(session, cache_lookaside_read);
        }
    }

    WT_ASSERT(session, upd != NULL || ret != 0);

    return (ret);
}
