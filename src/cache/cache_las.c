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
 * __las_store_time_pair --
 *     Store the time pair to use for the lookaside inserts.
 */
static void
__las_store_time_pair(WT_SESSION_IMPL *session, wt_timestamp_t timestamp, uint64_t txnid)
{
    session->orig_timestamp_to_las = timestamp;
    session->orig_txnid_to_las = txnid;
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

    WT_STAT_CONN_SET(session, cache_hs_ondisk_max, las_cursor->btree->file_max);

    return (0);
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

    /*
     * We have a cursor, and we need the underlying data handle; we can get to it by way of the
     * underlying btree handle, but it's a little ugly.
     */
    dstats = ((WT_CURSOR_BTREE *)cache->las_session[0]->las_cursor)->btree->dhandle->stats;

    v = WT_STAT_READ(dstats, cursor_update);
    WT_STAT_SET(session, cstats, cache_hs_insert, v);

    /*
     * If we're clearing stats we need to clear the cursor values we just read. This does not clear
     * the rest of the statistics in the lookaside data source stat cursor, but we own that
     * namespace so we don't have to worry about users seeing inconsistent data source information.
     */
    if (FLD_ISSET(conn->stat_flags, WT_STAT_CLEAR))
        WT_STAT_SET(session, dstats, cursor_update, 0);
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
                WT_STAT_CONN_INCRV(session, cache_hs_cursor_wait_internal, WT_THOUSAND);
            else
                WT_STAT_CONN_INCRV(session, cache_hs_cursor_wait_application, WT_THOUSAND);
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
          "Current history store file size: %" PRId64
          ", "
          "cache dirty: %2.3f%% , "
          "cache use: %2.3f%%",
          btree_id, multi->page_las.max_txn,
          __wt_timestamp_to_string(multi->page_las.max_ondisk_ts, ts_string[0]),
          __wt_timestamp_to_string(multi->page_las.min_skipped_ts, ts_string[1]),
          WT_STAT_READ(conn->stats, cache_hs_ondisk), pct_dirty, pct_full);
    }

    /* Never skip updating the tracked generation */
    if (WT_VERBOSE_ISSET(session, WT_VERB_LOOKASIDE))
        cache->las_verb_gen_write = ckpt_gen_current;
}

/*
 * __las_insert_record --
 *     A helper function to insert the record into the lookaside including stop time pair.
 */
static int
__las_insert_record(WT_SESSION_IMPL *session, WT_CURSOR *cursor, const uint32_t btree_id,
  const WT_ITEM *key, const WT_UPDATE *upd, const uint8_t type, const WT_ITEM *las_value,
  WT_TIME_PAIR stop_ts_pair)
{
    cursor->set_key(
      cursor, btree_id, key, upd->start_ts, upd->txnid, stop_ts_pair.timestamp, stop_ts_pair.txnid);

    /* Set the current update start time pair as the commit time pair to the lookaside record. */
    __las_store_time_pair(session, upd->start_ts, upd->txnid);

    cursor->set_value(cursor, upd->durable_ts, upd->prepare_state, type, las_value);

    /*
     * Using update instead of insert so the page stays pinned and can be searched before the tree.
     */
    WT_RET(cursor->update(cursor));

    /* Append a delete record to represent stop time pair for the above insert record */
    cursor->set_key(
      cursor, btree_id, key, upd->start_ts, upd->txnid, stop_ts_pair.timestamp, stop_ts_pair.txnid);

    /* Set the stop time pair as the commit time pair of the lookaside delete record. */
    __las_store_time_pair(session, stop_ts_pair.timestamp, stop_ts_pair.txnid);

    /* Remove the inserted record with stop timestamp. */
    WT_RET(cursor->remove(cursor));

    return (0);
}

/*
 * __wt_las_insert_updates --
 *     Copy one set of saved updates into the database's lookaside table.
 */
int
__wt_las_insert_updates(WT_CURSOR *cursor, WT_BTREE *btree, WT_PAGE *page, WT_MULTI *multi)
{
    WT_DECL_ITEM(full_value);
    WT_DECL_ITEM(key);
    WT_DECL_ITEM(mementos);
    WT_DECL_ITEM(modify_value);
    WT_DECL_ITEM(prev_full_value);
    WT_DECL_ITEM(tmp);
    WT_DECL_RET;
    WT_KEY_MEMENTO *mementop;
/* If the limit is exceeded, we will insert a full update to lookaside */
#define MAX_REVERSE_MODIFY_NUM 16
    WT_MODIFY entries[MAX_REVERSE_MODIFY_NUM];
    WT_MODIFY_VECTOR modifies;
    WT_SAVE_UPD *list;
    WT_SESSION_IMPL *session;
    WT_TXN *txn;
    WT_TXN_ISOLATION saved_isolation;
    WT_UPDATE *prev_upd, *upd;
    WT_TIME_PAIR stop_ts_pair;
    wt_off_t las_size;
    wt_timestamp_t durable_timestamp;
    uint64_t insert_cnt, max_las_size;
    uint32_t mementos_cnt, btree_id, i;
    uint8_t *p, prepare_state, upd_type;
    int nentries;
    bool las_key_saved, local_txn, retrieve_modify, squashed;

    mementop = NULL;
    session = (WT_SESSION_IMPL *)cursor->session;
    txn = &session->txn;
    saved_isolation = 0; /*[-Wconditional-uninitialized] */
    insert_cnt = 0;
    mementos_cnt = 0;
    btree_id = btree->id;
    local_txn = retrieve_modify = false;
    __wt_modify_vector_init(session, &modifies);

    if (!btree->lookaside_entries)
        btree->lookaside_entries = true;

    /* Wrap all the updates in a transaction. */
    WT_ERR(__wt_txn_begin(session, NULL));
    __las_set_isolation(session, &saved_isolation);

    local_txn = true;

    /* Ensure enough room for a column-store key without checking. */
    WT_ERR(__wt_scr_alloc(session, WT_INTPACK64_MAXSIZE, &key));

    WT_ERR(__wt_scr_alloc(session, 0, &mementos));

    WT_ERR(__wt_scr_alloc(session, 0, &full_value));

    WT_ERR(__wt_scr_alloc(session, 0, &prev_full_value));

    /* Inserts should be on the same page absent a split, search any pinned leaf page. */
    F_SET(cursor, WT_CURSTD_UPDATE_LOCAL);

    /* Enter each update in the boundary's list into the lookaside store. */
    for (i = 0, list = multi->supd; i < multi->supd_entries; ++i, ++list) {
        /* If no onpage_upd is selected, we don't need to insert anything to lookaside */
        if (list->onpage_upd == NULL)
            continue;

        /* onpage_upd now is always from the update chain */
        WT_ASSERT(session, !F_ISSET(list->onpage_upd, WT_UPDATE_RESTORED_FROM_DISK));

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
         * Trim any updates before writing to lookaside. This saves wasted work, but is also
         * necessary because the reconciliation only resolves existing birthmarks if they aren't
         * obsolete.
         */
        WT_WITH_BTREE(
          session, btree, upd = __wt_update_obsolete_check(session, page, list->onpage_upd, true));
        __wt_free_update_list(session, &upd);
        upd = list->onpage_upd;

        /*
         * It's not OK for the update list to contain a birthmark on entry - we will generate one
         * below if necessary.
         */
        WT_ASSERT(session, __wt_count_birthmarks(upd) == 0);

        las_key_saved = false;

        /*
         * The algorithm assumes the oldest update on the update chain in memory is either a full
         * update or a tombstone.
         *
         * This is guaranteed by __wt_rec_upd_select appends the original onpage value at the end of
         * the chain. It also assumes the onpage_upd selected cannot be a TOMBSTONE and the update
         * newer to a TOMBSTONE must be a full update.
         *
         * The algorithm walks from the oldest update to the newest update and build full updates
         * along the way. It sets the stop time pair of the update to the start time pair of the
         * next update, squashes the updates that are from the same transaction and of the same
         * start timestamp, calculates reverse modification if prev_upd is a MODIFY, and inserts the
         * update to lookaside.
         *
         * It deals with the following scenarios:
         * 1) We only have full updates on the chain and we only insert full updates to lookaside.
         * 2) We have modifies on the chain, i.e., U (selected onpage value) -> M -> M ->U. We
         * reverse the modifies and insert the reversed modifies to lookaside if it is not the
         * newest update written to lookaside and the reverse operation is successful.
         * With regard to the example, we insert U -> RM -> U to lookaside.
         * 3) We have tombstones in the middle of the chain, i.e.,
         * U (selected onpage value) -> U -> T -> M -> U.
         * We write the stop time pair of M with the start time pair of the tombstone and skip the
         * tombstone.
         * 4) We have a tombstone at the end of the chain with transaction id WT_TXN_NONE
         * and start timestamp WT_TS_NONE, it is simply ignored.
         */
        for (; upd != NULL; upd = upd->next) {
            if (upd->txnid == WT_TXN_ABORTED)
                continue;

            /* We have at least one LAS record from this key, save a copy of the key */
            if (!las_key_saved) {
                /* Extend the buffer if needed. */
                WT_ERR(
                  __wt_buf_extend(session, mementos, (mementos_cnt + 1) * sizeof(WT_KEY_MEMENTO)));
                mementop = (WT_KEY_MEMENTO *)mementos->mem + mementos_cnt;
                WT_CLEAR(mementop->key);
                WT_ERR(__wt_buf_set(session, &mementop->key, key->data, key->size));
                mementop->txnid = WT_TXN_ABORTED;
                las_key_saved = true;
                mementos_cnt++;
            }

            WT_ERR(__wt_modify_vector_push(&modifies, upd));
        }

        upd = NULL;

        /*
         * Get the oldest full update on chain. It is either the oldest update or the second oldest
         * update if the oldest update is a TOMBSTONE.
         */
        WT_ASSERT(session, modifies.size > 0);
        __wt_modify_vector_pop(&modifies, &upd);

        /* If we popped a modify then it should be flagged as in the history store. */
        if (upd->type == WT_UPDATE_MODIFY) {
            WT_ASSERT(session, F_ISSET(upd, WT_UPDATE_HISTORY_STORE));
            retrieve_modify = true;
        }

        /* The key didn't exist back then, which is globally visible. */
        WT_ASSERT(session,
          retrieve_modify || upd->type == WT_UPDATE_STANDARD || upd->type == WT_UPDATE_TOMBSTONE);

        /* Skip TOMBSTONE at the end of the update chain. */
        if (upd->type == WT_UPDATE_TOMBSTONE) {
            if (modifies.size > 0) {
                __wt_modify_vector_pop(&modifies, &upd);
                WT_ASSERT(session, upd->type == WT_UPDATE_STANDARD);
            } else
                continue;
        }
        full_value->data = upd->data;
        full_value->size = upd->size;

        prev_upd = NULL;
        squashed = false;

        /* Flush the updates on stack. */
        for (; modifies.size > 0; tmp = full_value, full_value = prev_full_value,
                                  prev_full_value = tmp, upd = prev_upd) {
            /* Should not see BIRTHMARK or TOMBSTONE. */
            WT_ASSERT(session, upd->type == WT_UPDATE_STANDARD || upd->type == WT_UPDATE_MODIFY);

            __wt_modify_vector_pop(&modifies, &prev_upd);
            stop_ts_pair.timestamp = prev_upd->start_ts;
            stop_ts_pair.txnid = prev_upd->txnid;

            /* Skip TOMBSTONE. */
            if (prev_upd->type == WT_UPDATE_TOMBSTONE) {
                WT_ASSERT(session, modifies.size > 0);
                __wt_modify_vector_pop(&modifies, &prev_upd);

                /* The update newer to a TOMBSTONE must be a full update. */
                WT_ASSERT(session, prev_upd->type == WT_UPDATE_STANDARD);
            }

            /*
             * Retrieve the full value of the modify from the history store. This avoid us having to
             * iterate the full update list associated with the modify and recalculating the reverse
             * deltas.
             */
            if (retrieve_modify) {
                /*
                 * Here we need to set the read timestamp of the transaction to be the start
                 * timestamp of the update, otherwise when we search we will see the tombstone value
                 * associated with the update and return not found.
                 */
                txn->read_timestamp = upd->start_ts;
                F_SET(txn, WT_TXN_HAS_TS_READ);
                cursor->set_key(cursor, btree_id, key, upd->start_ts, upd->txnid,
                  stop_ts_pair.timestamp, stop_ts_pair.txnid);
                WT_ERR(cursor->search(cursor));
                WT_ERR(cursor->get_value(
                  cursor, &durable_timestamp, &prepare_state, &upd_type, full_value));
                txn->read_timestamp = 0;
                F_CLR(txn, WT_TXN_HAS_TS_READ);
                retrieve_modify = false;
            }

            if (prev_upd->type == WT_UPDATE_MODIFY) {
                WT_ERR(__wt_buf_set(session, prev_full_value, full_value->data, full_value->size));
                WT_ERR(__wt_modify_apply_item(session, prev_full_value, prev_upd->data, false));
            } else {
                WT_ASSERT(session, prev_upd->type == WT_UPDATE_STANDARD);

                prev_full_value->data = prev_upd->data;
                prev_full_value->size = prev_upd->size;
            }

            /*
             * Skip the updates have the same start timestamp and transaction id
             *
             * The update older than onpage_upd can be squashed away. Insert a full update anyway to
             * simplify the code. It will take some extra space but such case should be rare.
             */
            if (upd->start_ts != prev_upd->start_ts || upd->txnid != prev_upd->txnid ||
              modifies.size == 0) {
                /*
                 * Calculate reverse delta. Insert full update for the newest historical record even
                 * it's a MODIFY.
                 *
                 * It is not correct to check prev_upd == list->onpage_upd as we may have aborted
                 * updates in the middle.
                 */
                nentries = MAX_REVERSE_MODIFY_NUM;
                if (!F_ISSET(upd, WT_UPDATE_HISTORY_STORE)) {
                    if (upd->type == WT_UPDATE_MODIFY && modifies.size > 0 &&
                      __wt_calc_modify(session, prev_full_value, full_value,
                        prev_full_value->size / 10, entries, &nentries) == 0) {
                        WT_ERR(__wt_modify_pack(cursor, entries, nentries, &modify_value));
                        WT_ERR(__las_insert_record(session, cursor, btree_id, key, upd,
                          WT_UPDATE_MODIFY, modify_value, stop_ts_pair));
                        __wt_scr_free(session, &modify_value);
                        modify_value = NULL;
                    } else
                        WT_ERR(__las_insert_record(session, cursor, btree_id, key, upd,
                          WT_UPDATE_STANDARD, full_value, stop_ts_pair));

                    /* Flag the update as now in the lookaside file. */
                    F_SET(upd, WT_UPDATE_HISTORY_STORE);
                    ++insert_cnt;
                }
                if (squashed) {
                    WT_STAT_CONN_INCR(session, cache_hs_write_squash);
                    squashed = false;
                }
            } else
                squashed = true;
        }

        /*
         * The last element on the stack must be the onpage_upd.
         *
         * If saving a non-zero length value on the page, save a birthmark instead of duplicating it
         * in the lookaside table. (We check the length because row-store doesn't write zero-length
         * data items.)
         */
        if (upd->size > 0) {
            /* Make sure that we are generating a birthmark for an in-memory update. */
            WT_ASSERT(session, !F_ISSET(upd, WT_UPDATE_RESTORED_FROM_DISK) &&
                (upd->type == WT_UPDATE_STANDARD || upd->type == WT_UPDATE_MODIFY) &&
                upd == list->onpage_upd);

            mementop->txnid = upd->txnid;
            mementop->durable_ts = upd->durable_ts;
            mementop->start_ts = upd->start_ts;
            mementop->prepare_state = upd->prepare_state;
        }
    }

    WT_ERR(__wt_block_manager_named_size(session, WT_LAS_FILE, &las_size));
    WT_STAT_CONN_SET(session, cache_hs_ondisk, las_size);
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
    }

    __las_restore_isolation(session, saved_isolation);

    if (ret == 0 && mementos_cnt > 0)
        ret = __wt_calloc(session, mementos_cnt, sizeof(WT_KEY_MEMENTO), &multi->page_las.mementos);

    if (ret == 0 && (insert_cnt > 0 || mementos_cnt > 0)) {
        WT_ASSERT(session, multi->page_las.max_txn != WT_TXN_NONE);
        multi->has_las = true;
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
    /* modify_value is allocated in __wt_modify_pack. Free it if it is allocated. */
    if (modify_value != NULL)
        __wt_scr_free(session, &modify_value);
    __wt_scr_free(session, &mementos);
    __wt_modify_vector_free(&modifies);
    __wt_scr_free(session, &full_value);
    __wt_scr_free(session, &prev_full_value);
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
    WT_CURSOR *las_cursor;
    WT_DECL_ITEM(las_key);
    WT_DECL_ITEM(las_value);
    WT_DECL_RET;
    WT_ITEM *key, _key;
    WT_MODIFY_VECTOR modifies;
    WT_TIME_PAIR las_start, las_start_tmp, las_stop, las_stop_tmp;
    WT_TXN *txn;
    WT_UPDATE *mod_upd, *upd;
    wt_timestamp_t durable_timestamp, durable_timestamp_tmp, read_timestamp;
    size_t notused, size;
    uint64_t recno;
    uint32_t las_btree_id, session_flags;
    uint8_t prepare_state, prepare_state_tmp, *p, recno_key[WT_INTPACK64_MAXSIZE], upd_type;
    const uint8_t *recnop;
    int cmp;
    bool modify;

    *updp = NULL;

    las_cursor = NULL;
    key = NULL;
    mod_upd = upd = NULL;
    __wt_modify_vector_init(session, &modifies);
    txn = &session->txn;
    notused = size = 0;
    las_btree_id = S2BT(session)->id;
    session_flags = 0; /* [-Werror=maybe-uninitialized] */
    WT_NOT_READ(modify, false);

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

        /* We do not have prepared updates in the lookaside anymore */
        WT_ASSERT(session, prepare_state != WT_PREPARE_INPROGRESS);

        /*
         * Found a visible record, return success unless it is prepared and we are not ignoring
         * prepared.
         *
         * It's necessary to explicitly signal a prepare conflict so that the callers don't fallback
         * to using something from the update list.
         *
         * FIXME-PM-1521: review the code in future
         */
        if (prepare_state == WT_PREPARE_INPROGRESS &&
          !F_ISSET(&session->txn, WT_TXN_IGNORE_PREPARE) && !allow_prepare) {
            ret = WT_PREPARE_CONFLICT;
            break;
        }

        /* We do not have birthmarks and tombstones in the lookaside anymore. */
        WT_ASSERT(session, upd_type != WT_UPDATE_BIRTHMARK && upd_type != WT_UPDATE_TOMBSTONE);

        /*
         * Keep walking until we get a non-modify update. Once we get to that point, squash the
         * updates together.
         */
        if (upd_type == WT_UPDATE_MODIFY) {
            WT_NOT_READ(modify, true);
            while (upd_type == WT_UPDATE_MODIFY) {
                WT_ERR(__wt_update_alloc(session, las_value, &mod_upd, &notused, upd_type));
                WT_ERR(__wt_modify_vector_push(&modifies, mod_upd));
                mod_upd = NULL;

                /*
                 * Find the base update to apply the reverse deltas
                 */
                WT_ERR_NOTFOUND_OK(las_cursor->next(las_cursor));
                las_start_tmp.timestamp = WT_TS_NONE;
                las_start_tmp.txnid = WT_TXN_NONE;

                /*
                 * Make sure we use the temporary variants of these variables. We need to retain the
                 * timestamps of the original modify we saw.
                 *
                 * We keep looking back into lookaside until we find a base update to apply the
                 * reverse deltas on top of.
                 */
                WT_ERR(
                  las_cursor->get_key(las_cursor, &las_btree_id, las_key, &las_start_tmp.timestamp,
                    &las_start_tmp.txnid, &las_stop_tmp.timestamp, &las_stop_tmp.txnid));

                WT_ERR(__wt_compare(session, NULL, las_key, key, &cmp));

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
            WT_STAT_CONN_INCR(session, cache_hs_read_squash);
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
         *
         * FIXME-PM-1521: review the code in future
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
            F_SET(upd, WT_UPDATE_RESTORED_FROM_DISK);
        *updp = upd;

        /* We are done, we found the record we were searching for */
        break;
    }
    WT_ERR_NOTFOUND_OK(ret);

err:
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
            WT_STAT_CONN_INCR(session, cache_hs_read_miss);
        } else {
            WT_STAT_CONN_INCR(session, cache_hs_read);
            WT_STAT_DATA_INCR(session, cache_hs_read);
        }
    }

    WT_ASSERT(session, upd != NULL || ret != 0);

    return (ret);
}
