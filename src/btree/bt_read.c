/*-
 * Copyright (c) 2014-2019 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __col_instantiate --
 *     Update a column-store page entry based on a lookaside table update list.
 */
static int
__col_instantiate(
  WT_SESSION_IMPL *session, uint64_t recno, WT_REF *ref, WT_CURSOR_BTREE *cbt, WT_UPDATE *updlist)
{
    WT_PAGE *page;
    WT_UPDATE *upd;

    page = ref->page;

    /*
     * Discard any of the updates we don't need.
     *
     * Just free the memory: it hasn't been accounted for on the page yet.
     */
    if (updlist->next != NULL &&
      (upd = __wt_update_obsolete_check(session, page, updlist, false)) != NULL)
        __wt_free_update_list(session, &upd);

    /* Search the page and add updates. */
    WT_RET(__wt_col_search(cbt, recno, ref, true, NULL));
    WT_RET(__wt_col_modify(cbt, recno, NULL, updlist, WT_UPDATE_INVALID, false));
    return (0);
}

/*
 * __row_instantiate --
 *     Update a row-store page entry based on a lookaside table update list.
 */
static int
__row_instantiate(
  WT_SESSION_IMPL *session, WT_ITEM *key, WT_REF *ref, WT_CURSOR_BTREE *cbt, WT_UPDATE *updlist)
{
    WT_PAGE *page;
    WT_UPDATE *upd;

    page = ref->page;

    /*
     * Discard any of the updates we don't need.
     *
     * Just free the memory: it hasn't been accounted for on the page yet.
     */
    if (updlist->next != NULL &&
      (upd = __wt_update_obsolete_check(session, page, updlist, false)) != NULL)
        __wt_free_update_list(session, &upd);

    /* Search the page and add updates. */
    WT_RET(__wt_row_search(cbt, key, true, ref, true, NULL));
    WT_RET(__wt_row_modify(cbt, key, NULL, updlist, WT_UPDATE_INVALID, false));
    return (0);
}

/*
 * __create_birthmark_upd --
 *     Create a birthmark update to be put on the page.
 */
static int
__create_birthmark_upd(
  WT_SESSION_IMPL *session, WT_BIRTHMARK_DETAILS *birthmarkp, size_t *sizep, WT_UPDATE **updp)
{
    WT_UPDATE *upd;

    *updp = NULL;

    WT_RET(__wt_update_alloc(session, NULL, &upd, sizep, WT_UPDATE_BIRTHMARK));
    upd->txnid = birthmarkp->txnid;
    upd->durable_ts = birthmarkp->durable_ts;
    upd->start_ts = birthmarkp->start_ts;
    upd->prepare_state = birthmarkp->prepare_state;
    *updp = upd;

    return (0);
}

/*
 * __instantiate_birthmarks --
 *     Instantiate birthmark records in a recently read page.
 */
static int
__instantiate_birthmarks(WT_SESSION_IMPL *session, WT_REF *ref)
{
    WT_BIRTHMARK_DETAILS *birthmarkp;
    WT_CURSOR_BTREE cbt;
    WT_DECL_RET;
    WT_PAGE_LOOKASIDE *page_las;
    WT_UPDATE *upd;
    size_t incr, total_incr;
    uint64_t recno;
    uint32_t i;
    const uint8_t *p;

    page_las = ref->page_las;
    upd = NULL;
    total_incr = 0;

    if (page_las->birthmarks_cnt == 0)
        return (0);

    __wt_btcur_init(session, &cbt);
    __wt_btcur_open(&cbt);

    for (i = 0, birthmarkp = page_las->birthmarks; i < page_las->birthmarks_cnt;
         i++, birthmarkp++) {
        if (birthmarkp->txnid == WT_TXN_ABORTED)
            continue;

        WT_ERR(__create_birthmark_upd(session, birthmarkp, &incr, &upd));
        total_incr += incr;

        switch (ref->page->type) {
        case WT_PAGE_COL_FIX:
        case WT_PAGE_COL_VAR:
            p = birthmarkp->key.data;
            WT_ERR(__wt_vunpack_uint(&p, 0, &recno));
            WT_ERR(__col_instantiate(session, recno, ref, &cbt, upd));
            upd = NULL;
            break;
        case WT_PAGE_ROW_LEAF:
            WT_ERR(__row_instantiate(session, &birthmarkp->key, ref, &cbt, upd));
            upd = NULL;
            break;
        }
    }

    /* We do not need the birthmark information in the lookaside structure anymore. */
    for (i = 0, birthmarkp = page_las->birthmarks; i < page_las->birthmarks_cnt; i++, birthmarkp++)
        __wt_buf_free(session, &birthmarkp->key);

    page_las->birthmarks_cnt = 0;
    __wt_free(session, page_las->birthmarks);
    __wt_cache_page_inmem_incr(session, ref->page, total_incr);

err:
    WT_TRET(__wt_btcur_close(&cbt, true));
    __wt_free(session, upd);

    return (ret);
}

/*
 * __instantiate_lookaside --
 *     Instantiate lookaside update records that are not on disk image in a recently read page.
 */
static int
__instantiate_lookaside(WT_SESSION_IMPL *session, WT_REF *ref)
{
    struct las_page_prepared_updates {
        WT_ITEM key;
        wt_timestamp_t timestamp;
        uint64_t txnid;
    } * las_preparep;
    WT_CACHE *cache;
    WT_CURSOR *las_cursor;
    WT_CURSOR_BTREE cbt;
    WT_DECL_ITEM(error_buf);
    WT_DECL_ITEM(las_prepares);
    WT_DECL_RET;
    WT_ITEM las_key, las_key_tmp, las_value;
    WT_MODIFY_VECTOR modifies;
    WT_PAGE *page;
    WT_PAGE_LOOKASIDE *page_las;
    WT_UPDATE *mod_upd, *upd;
    wt_timestamp_t durable_timestamp, durable_timestamp_tmp, las_timestamp, las_timestamp_tmp;
    size_t notused, size, total_incr;
    uint64_t instantiated_cnt, las_txnid, las_txnid_tmp, recno;
    uint32_t i, las_btree_id, las_btree_id_tmp, las_prepare_cnt, mod_counter, session_flags;
    uint8_t prepare_state, upd_type;
    const uint8_t *p;
    int cmp;
    bool birthmark_record, locked;

    cache = S2C(session)->cache;
    las_cursor = NULL;
    WT_CLEAR(las_key);
    WT_CLEAR(las_key_tmp);
    WT_CLEAR(las_value);
    __wt_modify_vector_init(session, &modifies);
    page = ref->page;
    page_las = ref->page_las;
    mod_upd = upd = NULL;
    las_timestamp = WT_TS_NONE;
    las_txnid = WT_TXN_NONE;
    recno = WT_RECNO_OOB;
    las_btree_id = S2BT(session)->id;
    las_prepare_cnt = mod_counter = 0;
    session_flags = 0; /* [-Werror=maybe-uninitialized] */
    instantiated_cnt = 0;
    cmp = 0;
    birthmark_record = locked = false;

    /*
     * Check whether the disk image contains all the newest versions of the page. If the lookaside
     * contains prepared updates for this page, we need to check it regardless.
     */
    if (page_las->min_skipped_ts == WT_TS_MAX && !page_las->has_prepares) {
        WT_RET(__instantiate_birthmarks(session, ref));

        if (page->modify != NULL) {
            /*
             * Checkpoint may specify an older timestamp than the timestamp used to write the page,
             * it must be included in the next checkpoint.
             */
            page->modify->first_dirty_txn = WT_TXN_FIRST;
            FLD_SET(page->modify->restore_state, WT_PAGE_RS_LOOKASIDE);

            /*
             * The page image contained the newest versions of data so the updates in lookaside are
             * all older and we could consider marking it clean (i.e., the next checkpoint can use
             * the version already on disk).
             */
            if (!S2C(session)->txn_global.has_stable_timestamp &&
              __wt_txn_visible_all(session, page_las->max_txn, page_las->max_ondisk_ts)) {
                page->modify->rec_max_txn = page_las->max_txn;
                page->modify->rec_max_timestamp = page_las->max_ondisk_ts;
                __wt_page_modify_clear(session, page);
            }
        }

        return (0);
    }

    __wt_btcur_init(session, &cbt);
    __wt_btcur_open(&cbt);

    WT_ERR(__wt_scr_alloc(session, 0, &las_prepares));
    WT_STAT_CONN_INCR(session, cache_page_instantiate_read_lookaside);
    WT_STAT_DATA_INCR(session, cache_page_instantiate_read_lookaside);
    if (WT_SESSION_IS_CHECKPOINT(session)) {
        WT_STAT_CONN_INCR(session, cache_page_instantiate_read_lookaside_checkpoint);
        WT_STAT_DATA_INCR(session, cache_page_instantiate_read_lookaside_checkpoint);
    }

    /* Open a lookaside table cursor. */
    __wt_las_cursor(session, &las_cursor, &session_flags);

    /*
     * The lookaside records are in update order for a given key, that is, there will be a set of
     * in-order updates for a key, then another set of in-order updates for a subsequent key. We
     * find the most recent of the updates for a key and then insert that update into the page, then
     * all the updates for the next key, and so on. If a birthmark record exists for that key, then
     * insert birthmark record into the page.
     *
     * An important point to note is that the keys for a given page are NOT necessarily next to each
     * other in the lookaside table since we can specify our own ordering for a given table with a
     * custom collator. Therefore, we need to make use of the keys that we have stored in-memory
     * last time we evicted to instantiate each key.
     *
     * During instantiation, we iterate over our set of keys from eviction. If the key memento has a
     * specific txn id that isn't "aborted" then it indicates that birthmark update should be
     * instantiated for that key. Otherwise it is just an indicator that we need to search the
     * lookaside for that particular key.
     */
    cache->las_reader = true;
    __wt_readlock(session, &cache->las_sweepwalk_lock);
    cache->las_reader = false;
    notused = size = total_incr = 0;
    locked = true;
    for (i = 0; i < page_las->birthmarks_cnt; ++i) {
        /*
         * An "aborted transaction id means that this is a birthmark update as opposed to just
         * keeping the key in memory so we can search lookaside.
         */
        if (page_las->birthmarks[i].txnid != WT_TXN_ABORTED) {
            WT_ERR(__create_birthmark_upd(session, &page_las->birthmarks[i], &size, &upd));
            WT_ERR(__wt_buf_set(session, &las_key, page_las->birthmarks[i].key.data,
              page_las->birthmarks[i].key.size));
            WT_ASSERT(session, las_key.data != page_las->birthmarks[i].key.data);
            birthmark_record = true;
        } else {
            WT_ERR(__wt_las_cursor_position(
              session, las_cursor, las_btree_id, &page_las->birthmarks[i].key, WT_TS_MAX));
            WT_ERR(
                las_cursor->get_key(las_cursor, &las_btree_id_tmp, &las_key, &las_timestamp, &las_txnid));
            WT_ERR(__wt_compare(session, NULL, &las_key, &page_las->birthmarks[i].key, &cmp));
            if (las_btree_id != las_btree_id_tmp || cmp != 0) {
                WT_ERR(__wt_scr_alloc(session, 1024, &error_buf));
                WT_PANIC_ERR(
                  session, WT_NOTFOUND, "Could not find any lookaside records for key: %.1024s",
                  __wt_buf_set_printable_format(session, page_las->birthmarks[i].key.data,
                    page_las->birthmarks[i].key.size, S2BT(session)->key_format, error_buf));
            }

            /* Allocate the WT_UPDATE structure. */
            WT_ERR(las_cursor->get_value(
              las_cursor, &durable_timestamp, &prepare_state, &upd_type, &las_value));

            /*
             * If our update is a modify then rewrite it as a standard update. It's a problem if we
             * need to read backwards into lookaside just to make sense of what we have in our
             * update list.
             *
             * The update we're constructing will have the same visibility as the modify that we're
             * replacing it with.
             */
            while (upd_type == WT_UPDATE_MODIFY) {
                WT_ERR(__wt_update_alloc(session, &las_value, &mod_upd, &notused, upd_type));
                WT_ERR(__wt_modify_vector_push(&modifies, mod_upd));
                mod_upd = NULL;

                /*
                 * Check that we haven't crossed over to another btree/key. If we've crossed a
                 * boundary then the base update that we're applying the modifies to should be the
                 * on-disk value which won't be in the lookaside. If we hit the beginning while
                 * we're walking backwards, that also means we've hit a key boundary.
                 *
                 * The on-disk value cannot be a modify or a prepare so we can confidently assign
                 * the update type and prepare state to the resulting update.
                 */
                WT_ERR_NOTFOUND_OK(las_cursor->prev(las_cursor));
                las_timestamp_tmp = WT_TS_NONE;
                las_txnid_tmp = WT_TXN_NONE;
                if (ret != WT_NOTFOUND) {
                    WT_ERR(las_cursor->get_key(
                      las_cursor, &las_btree_id, &las_key_tmp, &las_timestamp_tmp, &las_txnid_tmp));
                    WT_ERR(__wt_compare(session, NULL, &las_key, &las_key_tmp, &cmp));
                }
                if (ret == WT_NOTFOUND || las_btree_id != S2BT(session)->id || cmp != 0) {
                    upd_type = WT_UPDATE_STANDARD;
                    prepare_state = WT_PREPARE_INIT;
                    WT_ERR(__wt_value_return_buf(&cbt, ref, &las_value));
                    break;
                } else
                    WT_ASSERT(session, __wt_txn_visible(session, las_txnid_tmp, las_timestamp_tmp));
                WT_ERR(las_cursor->get_value(
                  las_cursor, &durable_timestamp_tmp, &prepare_state, &upd_type, &las_value));
            }
            WT_ASSERT(session, upd_type == WT_UPDATE_STANDARD || upd_type == WT_UPDATE_TOMBSTONE);
            while (modifies.size > 0) {
                __wt_modify_vector_pop(&modifies, &mod_upd);
                WT_ERR(__wt_modify_apply_item(session, &las_value, mod_upd->data, false));
                __wt_free_update_list(session, &mod_upd);
                mod_upd = NULL;
                /*
                 * We had to do some backtracking to construct this update. Unwind back to where we
                 * were before.
                 */
                WT_ERR(las_cursor->next(las_cursor));
            }

            WT_ERR(__wt_update_alloc(session, &las_value, &upd, &size, upd_type));
            upd->txnid = las_txnid;
            upd->durable_ts = durable_timestamp;
            upd->start_ts = las_timestamp;
            upd->prepare_state = prepare_state;
        }

        ++instantiated_cnt;
        total_incr += size;

        switch (page->type) {
        case WT_PAGE_COL_FIX:
        case WT_PAGE_COL_VAR:
            p = las_key.data;
            WT_ERR(__wt_vunpack_uint(&p, 0, &recno));
            WT_ERR(__col_instantiate(session, recno, ref, &cbt, upd));
            break;
        case WT_PAGE_ROW_LEAF:
            WT_ERR(__row_instantiate(session, &las_key, ref, &cbt, upd));
            break;
        default:
            WT_ERR(__wt_illegal_value(session, page->type));
        }

        /* Remove the prepared record from LAS once the page is instantiated successfully. */
        if (!birthmark_record && upd->prepare_state == WT_PREPARE_INPROGRESS) {
            /* Extend the buffer if needed. */
            WT_ERR(__wt_buf_extend(session, las_prepares,
              (las_prepare_cnt + 1) * sizeof(struct las_page_prepared_updates)));
            las_preparep = (struct las_page_prepared_updates *)las_prepares->mem + las_prepare_cnt;
            WT_CLEAR(las_preparep->key);
            WT_ERR(__wt_buf_set(session, &las_preparep->key, las_key.data, las_key.size));
            las_preparep->timestamp = las_timestamp;
            las_preparep->txnid = las_txnid;
            las_prepare_cnt++;
        }

        birthmark_record = false;
        upd = NULL;
    }

    __wt_readunlock(session, &cache->las_sweepwalk_lock);
    locked = false;
    WT_ERR_NOTFOUND_OK(ret);

    __wt_cache_page_inmem_incr(session, page, total_incr);

    /*
     * If the updates in lookaside are newer than the versions on the page, it must be included in
     * the next checkpoint.
     */
    if (page->modify != NULL) {
        page->modify->first_dirty_txn = WT_TXN_FIRST;
        FLD_SET(page->modify->restore_state, WT_PAGE_RS_LOOKASIDE);
    }

    /*
     * Now the page is successfully instantiated. Remove the prepared updates from LAS that are
     * instantiated.
     */
    for (i = 0, las_preparep = las_prepares->mem; i < las_prepare_cnt; i++, las_preparep++) {
        las_cursor->set_key(las_cursor, las_btree_id, &las_preparep->key, las_preparep->timestamp,
          las_preparep->txnid);
        WT_ERR(las_cursor->remove(las_cursor));
    }

    __wt_verbose(session, WT_VERB_LOOKASIDE_ACTIVITY,
      "btree ID %" PRIu32 " page instantiated with %" PRIu64 " lookaside items", las_btree_id,
      instantiated_cnt);

err:
    __wt_free_update_list(session, &mod_upd);
    while (modifies.size > 0) {
        __wt_modify_vector_pop(&modifies, &mod_upd);
        __wt_free_update_list(session, &mod_upd);
    }
    __wt_modify_vector_free(&modifies);
    if (las_prepare_cnt != 0)
        for (i = 0, las_preparep = las_prepares->mem; i < las_prepare_cnt; i++, las_preparep++)
            __wt_buf_free(session, &las_preparep->key);

    __wt_scr_free(session, &las_prepares);
    if (locked)
        __wt_readunlock(session, &cache->las_sweepwalk_lock);
    WT_TRET(__wt_las_cursor_close(session, &las_cursor, session_flags));
    WT_TRET(__wt_btcur_close(&cbt, true));
    __wt_free(session, upd);

    return (ret);
}

/*
 * __evict_force_check --
 *     Check if a page matches the criteria for forced eviction.
 */
static bool
__evict_force_check(WT_SESSION_IMPL *session, WT_REF *ref)
{
    WT_BTREE *btree;
    WT_PAGE *page;
    size_t footprint;

    btree = S2BT(session);
    page = ref->page;

    /* Leaf pages only. */
    if (WT_PAGE_IS_INTERNAL(page))
        return (false);

    /*
     * It's hard to imagine a page with a huge memory footprint that has never been modified, but
     * check to be sure.
     */
    if (__wt_page_evict_clean(page))
        return (false);

    /*
     * Exclude the disk image size from the footprint checks.  Usually the
     * disk image size is small compared with the in-memory limit (e.g.
     * 16KB vs 5MB), so this doesn't make a big difference.  Where it is
     * important is for pages with a small number of large values, where
     * the disk image size takes into account large values that have
     * already been written and should not trigger forced eviction.
     */
    footprint = page->memory_footprint;
    if (page->dsk != NULL)
        footprint -= page->dsk->mem_size;

    /* Pages are usually small enough, check that first. */
    if (footprint < btree->splitmempage)
        return (false);

    /*
     * If this session has more than one hazard pointer, eviction will fail and there is no point
     * trying.
     */
    if (__wt_hazard_count(session, ref) > 1)
        return (false);

    /* If we can do an in-memory split, do it. */
    if (__wt_leaf_page_can_split(session, page))
        return (true);
    if (footprint < btree->maxmempage)
        return (false);

    /* Bump the oldest ID, we're about to do some visibility checks. */
    WT_IGNORE_RET(__wt_txn_update_oldest(session, 0));

    /*
     * Allow some leeway if the transaction ID isn't moving forward since it is unlikely eviction
     * will be able to evict the page. Don't keep skipping the page indefinitely or large records
     * can lead to extremely large memory footprints.
     */
    if (!__wt_page_evict_retry(session, page))
        return (false);

    /* Trigger eviction on the next page release. */
    __wt_page_evict_soon(session, ref);

    /* If eviction cannot succeed, don't try. */
    return (__wt_page_can_evict(session, ref, NULL));
}

/*
 * __page_read --
 *     Read a page from the file.
 */
static int
__page_read(WT_SESSION_IMPL *session, WT_REF *ref, uint32_t flags)
{
    WT_DECL_RET;
    WT_ITEM tmp;
    WT_PAGE *notused;
    size_t addr_size;
    uint64_t time_diff, time_start, time_stop;
    uint32_t page_flags, final_state, new_state, previous_state;
    const uint8_t *addr;
    bool timer;

    time_start = time_stop = 0;

    /*
     * Don't pass an allocated buffer to the underlying block read function, force allocation of new
     * memory of the appropriate size.
     */
    WT_CLEAR(tmp);

    /*
     * Attempt to set the state to WT_REF_READING for normal reads, or WT_REF_LOCKED, for deleted
     * pages or pages with lookaside entries. The difference is that checkpoints can skip over clean
     * pages that are being read into cache, but need to wait for deletes or lookaside updates to be
     * resolved (in order for checkpoint to write the correct version of the page).
     *
     * If successful, we've won the race, read the page.
     */
    switch (previous_state = ref->state) {
    case WT_REF_DISK:
        new_state = WT_REF_READING;
        break;
    case WT_REF_DELETED:
    case WT_REF_LOOKASIDE:
        new_state = WT_REF_LOCKED;
        break;
    default:
        return (0);
    }
    if (!WT_REF_CAS_STATE(session, ref, previous_state, new_state))
        return (0);

    final_state = WT_REF_MEM;

    /*
     * Get the address: if there is no address, the page was deleted or had only lookaside entries,
     * and a subsequent search or insert is forcing re-creation of the name space.
     */
    __wt_ref_info(session, ref, &addr, &addr_size, NULL);
    if (addr == NULL) {
        WT_ASSERT(session, previous_state != WT_REF_DISK);

        WT_ERR(__wt_btree_new_leaf_page(session, &ref->page));
        goto skip_read;
    }

    /*
     * There's an address, read or map the backing disk page and build an in-memory version of the
     * page.
     */
    timer = !F_ISSET(session, WT_SESSION_INTERNAL);
    if (timer)
        time_start = __wt_clock(session);
    WT_ERR(__wt_bt_read(session, &tmp, addr, addr_size));
    if (timer) {
        time_stop = __wt_clock(session);
        time_diff = WT_CLOCKDIFF_US(time_stop, time_start);
        WT_STAT_CONN_INCR(session, cache_read_app_count);
        WT_STAT_CONN_INCRV(session, cache_read_app_time, time_diff);
        WT_STAT_SESSION_INCRV(session, read_time, time_diff);
    }

    /*
     * Build the in-memory version of the page. Clear our local reference to the allocated copy of
     * the disk image on return, the in-memory object steals it.
     *
     * If a page is read with eviction disabled, we don't count evicting it as progress. Since
     * disabling eviction allows pages to be read even when the cache is full, we want to avoid
     * workloads repeatedly reading a page with eviction disabled (e.g., a metadata page), then
     * evicting that page and deciding that is a sign that eviction is unstuck.
     */
    page_flags = WT_DATA_IN_ITEM(&tmp) ? WT_PAGE_DISK_ALLOC : WT_PAGE_DISK_MAPPED;
    if (LF_ISSET(WT_READ_IGNORE_CACHE_SIZE))
        FLD_SET(page_flags, WT_PAGE_EVICT_NO_PROGRESS);
    WT_ERR(__wt_page_inmem(session, ref, tmp.data, page_flags, true, &notused));
    tmp.mem = NULL;

    /*
     * The WT_REF lookaside state should match the page-header state of any page we read.
     */
    WT_ASSERT(session, previous_state != WT_REF_LOOKASIDE || ref->page->dsk == NULL ||
        F_ISSET(ref->page->dsk, WT_PAGE_LAS_UPDATE));

skip_read:
    switch (previous_state) {
    case WT_REF_DELETED:
        /*
         * A truncated page may also have lookaside information. The delete happened after page
         * eviction (writing the lookaside information), first update based on the lookaside table
         * and then apply the delete.
         */
        if (ref->page_las != NULL)
            WT_ERR(__instantiate_lookaside(session, ref));

        /* Move all records to a deleted state. */
        WT_ERR(__wt_delete_page_instantiate(session, ref));
        break;
    case WT_REF_LOOKASIDE:
        WT_ERR(__instantiate_lookaside(session, ref));
        break;
    }

    WT_REF_SET_STATE(ref, final_state);

    WT_ASSERT(session, ret == 0);
    return (0);

err:
    /*
     * If the function building an in-memory version of the page failed, it discarded the page, but
     * not the disk image. Discard the page and separately discard the disk image in all cases.
     */
    if (ref->page != NULL)
        __wt_ref_out(session, ref);
    WT_REF_SET_STATE(ref, previous_state);

    __wt_buf_free(session, &tmp);

    return (ret);
}

/*
 * __wt_page_in_func --
 *     Acquire a hazard pointer to a page; if the page is not in-memory, read it from the disk and
 *     build an in-memory version.
 */
int
__wt_page_in_func(WT_SESSION_IMPL *session, WT_REF *ref, uint32_t flags
#ifdef HAVE_DIAGNOSTIC
  ,
  const char *func, int line
#endif
  )
{
    WT_BTREE *btree;
    WT_DECL_RET;
    WT_PAGE *page;
    uint64_t sleep_usecs, yield_cnt;
    uint32_t current_state;
    int force_attempts;
    bool busy, cache_work, evict_skip, stalled, wont_need;

    btree = S2BT(session);

    if (F_ISSET(session, WT_SESSION_IGNORE_CACHE_SIZE))
        LF_SET(WT_READ_IGNORE_CACHE_SIZE);

    /* Sanity check flag combinations. */
    WT_ASSERT(session, !LF_ISSET(WT_READ_DELETED_SKIP | WT_READ_NO_WAIT | WT_READ_LOOKASIDE) ||
        LF_ISSET(WT_READ_CACHE));
    WT_ASSERT(session, !LF_ISSET(WT_READ_DELETED_CHECK) || !LF_ISSET(WT_READ_DELETED_SKIP));

    /*
     * Ignore reads of pages already known to be in cache, otherwise the eviction server can
     * dominate these statistics.
     */
    if (!LF_ISSET(WT_READ_CACHE)) {
        WT_STAT_CONN_INCR(session, cache_pages_requested);
        WT_STAT_DATA_INCR(session, cache_pages_requested);
    }

    for (evict_skip = stalled = wont_need = false, force_attempts = 0, sleep_usecs = yield_cnt = 0;
         ;) {
        switch (current_state = ref->state) {
        case WT_REF_DELETED:
            if (LF_ISSET(WT_READ_DELETED_SKIP | WT_READ_NO_WAIT))
                return (WT_NOTFOUND);
            if (LF_ISSET(WT_READ_DELETED_CHECK) && __wt_delete_page_skip(session, ref, false))
                return (WT_NOTFOUND);
            goto read;
        case WT_REF_LOOKASIDE:
            if (LF_ISSET(WT_READ_CACHE)) {
                if (!LF_ISSET(WT_READ_LOOKASIDE))
                    return (WT_NOTFOUND);
                /*
                 * If we skip a lookaside page, the tree cannot be left clean: lookaside entries
                 * must be resolved before the tree can be discarded.
                 */
                if (__wt_las_page_skip(session, ref)) {
                    __wt_tree_modify_set(session);
                    return (WT_NOTFOUND);
                }
            }
            goto read;
        case WT_REF_DISK:
            if (LF_ISSET(WT_READ_CACHE))
                return (WT_NOTFOUND);

read:
            /*
             * The page isn't in memory, read it. If this thread respects the cache size, check for
             * space in the cache.
             */
            if (!LF_ISSET(WT_READ_IGNORE_CACHE_SIZE))
                WT_RET(__wt_cache_eviction_check(
                  session, true, !F_ISSET(&session->txn, WT_TXN_HAS_ID), NULL));
            WT_RET(__page_read(session, ref, flags));

            /*
             * We just read a page, don't evict it before we have a chance to use it.
             */
            evict_skip = true;

            /*
             * If configured to not trash the cache, leave the page generation unset, we'll set it
             * before returning to the oldest read generation, so the page is forcibly evicted as
             * soon as possible. We don't do that set here because we don't want to evict the page
             * before we "acquire" it.
             */
            wont_need = LF_ISSET(WT_READ_WONT_NEED) ||
              F_ISSET(session, WT_SESSION_READ_WONT_NEED) ||
              F_ISSET(S2C(session)->cache, WT_CACHE_EVICT_NOKEEP);
            continue;
        case WT_REF_READING:
            if (LF_ISSET(WT_READ_CACHE))
                return (WT_NOTFOUND);
            if (LF_ISSET(WT_READ_NO_WAIT))
                return (WT_NOTFOUND);

            /* Waiting on another thread's read, stall. */
            WT_STAT_CONN_INCR(session, page_read_blocked);
            stalled = true;
            break;
        case WT_REF_LOCKED:
            if (LF_ISSET(WT_READ_NO_WAIT))
                return (WT_NOTFOUND);

            /* Waiting on eviction, stall. */
            WT_STAT_CONN_INCR(session, page_locked_blocked);
            stalled = true;
            break;
        case WT_REF_SPLIT:
            return (WT_RESTART);
        case WT_REF_MEM:
            /*
             * The page is in memory.
             *
             * Get a hazard pointer if one is required. We cannot be evicting if no hazard pointer
             * is required, we're done.
             */
            if (F_ISSET(btree, WT_BTREE_IN_MEMORY))
                goto skip_evict;

/*
 * The expected reason we can't get a hazard pointer is because the page is being evicted, yield,
 * try again.
 */
#ifdef HAVE_DIAGNOSTIC
            WT_RET(__wt_hazard_set(session, ref, &busy, func, line));
#else
            WT_RET(__wt_hazard_set(session, ref, &busy));
#endif
            if (busy) {
                WT_STAT_CONN_INCR(session, page_busy_blocked);
                break;
            }

            /*
             * Check if the page requires forced eviction.
             */
            if (evict_skip || LF_ISSET(WT_READ_NO_SPLIT) || btree->evict_disabled > 0 ||
              btree->lsm_primary)
                goto skip_evict;

            /*
             * If reconciliation is disabled (e.g., when inserting into the lookaside table), skip
             * forced eviction if the page can't split.
             */
            if (F_ISSET(session, WT_SESSION_NO_RECONCILE) &&
              !__wt_leaf_page_can_split(session, ref->page))
                goto skip_evict;

            /*
             * Forcibly evict pages that are too big.
             */
            if (force_attempts < 10 && __evict_force_check(session, ref)) {
                ++force_attempts;
                ret = __wt_page_release_evict(session, ref, 0);
                /*
                 * If forced eviction succeeded, don't retry. If it failed, stall.
                 */
                if (ret == 0)
                    evict_skip = true;
                else if (ret == EBUSY) {
                    WT_NOT_READ(ret, 0);
                    WT_STAT_CONN_INCR(session, page_forcible_evict_blocked);
                    stalled = true;
                    break;
                }
                WT_RET(ret);

                /*
                 * The result of a successful forced eviction is a page-state transition
                 * (potentially to an in-memory page we can use, or a restart return for our
                 * caller), continue the outer page-acquisition loop.
                 */
                continue;
            }

        skip_evict:
            /*
             * If we read the page and are configured to not trash the cache, and no other thread
             * has already used the page, set the read generation so the page is evicted soon.
             *
             * Otherwise, if we read the page, or, if configured to update the page's read
             * generation and the page isn't already flagged for forced eviction, update the page
             * read generation.
             */
            page = ref->page;
            if (page->read_gen == WT_READGEN_NOTSET) {
                if (wont_need)
                    page->read_gen = WT_READGEN_WONT_NEED;
                else
                    __wt_cache_read_gen_new(session, page);
            } else if (!LF_ISSET(WT_READ_NO_GEN))
                __wt_cache_read_gen_bump(session, page);

            /*
             * Check if we need an autocommit transaction. Starting a transaction can trigger
             * eviction, so skip it if eviction isn't permitted.
             *
             * The logic here is a little weird: some code paths do a blanket ban on checking the
             * cache size in sessions, but still require a transaction (e.g., when updating metadata
             * or lookaside). If WT_READ_IGNORE_CACHE_SIZE was passed in explicitly, we're done. If
             * we set WT_READ_IGNORE_CACHE_SIZE because it was set in the session then make sure we
             * start a transaction.
             */
            return (LF_ISSET(WT_READ_IGNORE_CACHE_SIZE) &&
                  !F_ISSET(session, WT_SESSION_IGNORE_CACHE_SIZE) ?
                0 :
                __wt_txn_autocommit_check(session));
        default:
            return (__wt_illegal_value(session, current_state));
        }

        /*
         * We failed to get the page -- yield before retrying, and if we've yielded enough times,
         * start sleeping so we don't burn CPU to no purpose.
         */
        if (yield_cnt < WT_THOUSAND) {
            if (!stalled) {
                ++yield_cnt;
                __wt_yield();
                continue;
            }
            yield_cnt = WT_THOUSAND;
        }

        /*
         * If stalling and this thread is allowed to do eviction work, check if the cache needs help
         * evicting clean pages (don't force a read to do dirty eviction). If we do work for the
         * cache, substitute that for a sleep.
         */
        if (!LF_ISSET(WT_READ_IGNORE_CACHE_SIZE)) {
            WT_RET(__wt_cache_eviction_check(session, true, true, &cache_work));
            if (cache_work)
                continue;
        }
        __wt_spin_backoff(&yield_cnt, &sleep_usecs);
        WT_STAT_CONN_INCRV(session, page_sleep, sleep_usecs);
    }
}
