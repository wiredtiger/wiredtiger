/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

static int __cursor_prepared_txn_list_append(WT_SESSION_IMPL *, WT_CURSOR_PREPARE_TXN *, uint64_t);
static int __cursor_prepared_txn_setup(WT_SESSION_IMPL *, WT_CURSOR_PREPARE_TXN *);

/*
 * __cursor_prepared_txn_next --
 *     WT_CURSOR->next method for the prepared transaction cursor type.
 */
static int
__cursor_prepared_txn_next(WT_CURSOR *cursor)
{
    WT_CURSOR_PREPARE_TXN *cursor_prepare;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    cursor_prepare = (WT_CURSOR_PREPARE_TXN *)cursor;
    CURSOR_API_CALL(cursor, session, ret, next, NULL);

    if (cursor_prepare->list == NULL || cursor_prepare->list[cursor_prepare->next] == 0) {
        F_CLR(cursor, WT_CURSTD_KEY_SET);
        WT_ERR(WT_NOTFOUND);
    }

    /* TODO: Is this the right way to set a uint64_t in a cursor item structure? */
    cursor_prepare->iface.key.data = &cursor_prepare->list[cursor_prepare->next];
    cursor_prepare->iface.key.size = sizeof(uint64_t);
    ++cursor_prepare->next;

    F_SET(cursor, WT_CURSTD_KEY_INT);

err:
    API_END_RET(session, ret);
}

/*
 * __cursor_prepared_txn_reset --
 *     WT_CURSOR->reset method for the prepared transaction cursor type.
 */
static int
__cursor_prepared_txn_reset(WT_CURSOR *cursor)
{
    WT_CURSOR_PREPARE_TXN *cursor_prepare;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    cursor_prepare = (WT_CURSOR_PREPARE_TXN *)cursor;
    CURSOR_API_CALL_PREPARE_ALLOWED(cursor, session, reset, NULL);

    cursor_prepare->next = 0;
    F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);

err:
    API_END_RET(session, ret);
}

/*
 * __cursor_prepared_txn_close --
 *     WT_CURSOR->close method for the prepared transaction cursor type.
 */
static int
__cursor_prepared_txn_close(WT_CURSOR *cursor)
{
    WT_CURSOR_PREPARE_TXN *cursor_prepare;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    cursor_prepare = (WT_CURSOR_PREPARE_TXN *)cursor;
    CURSOR_API_CALL_PREPARE_ALLOWED(cursor, session, close, NULL);
err:

    __wt_free(session, cursor_prepare->list);
    __wt_cursor_close(cursor);

    API_END_RET(session, ret);
}

/*
 * __wt_cursor_prepared_txn_open --
 *     WT_SESSION->open_cursor method for the prepared transaction cursor type.
 */
int
__wt_cursor_prepared_txn_open(WT_SESSION_IMPL *session, const char *uri, WT_CURSOR *other,
  const char *cfg[], WT_CURSOR **cursorp)
{
    WT_CURSOR_STATIC_INIT(iface, __wt_cursor_get_key, /* get-key */
      __wti_cursor_get_value_notsup,                  /* get-value */
      __wti_cursor_get_raw_key_value_notsup,          /* get-raw-key-value */
      __wti_cursor_set_key_notsup,                    /* set-key */
      __wti_cursor_set_value_notsup,                  /* set-value */
      __wti_cursor_compare_notsup,                    /* compare */
      __wti_cursor_equals_notsup,                     /* equals */
      __cursor_prepared_txn_next,                     /* next */
      __wt_cursor_notsup,                             /* prev */
      __cursor_prepared_txn_reset,                    /* reset */
      __wt_cursor_notsup,                             /* search */
      __wt_cursor_search_near_notsup,                 /* search-near */
      __wt_cursor_notsup,                             /* insert */
      __wt_cursor_modify_notsup,                      /* modify */
      __wt_cursor_notsup,                             /* update */
      __wt_cursor_notsup,                             /* remove */
      __wt_cursor_notsup,                             /* reserve */
      __wt_cursor_config_notsup,                      /* reconfigure */
      __wt_cursor_notsup,                             /* largest_key */
      __wt_cursor_config_notsup,                      /* bound */
      __wt_cursor_notsup,                             /* cache */
      __wt_cursor_reopen_notsup,                      /* reopen */
      __wt_cursor_checkpoint_id,                      /* checkpoint ID */
      __cursor_prepared_txn_close);                   /* close */
    WT_CURSOR *cursor;
    WT_CURSOR_PREPARE_TXN *cursor_prepare;
    WT_DECL_RET;

    WT_VERIFY_OPAQUE_POINTER(WT_CURSOR_PREPARE_TXN);

    WT_UNUSED(other);
    WT_UNUSED(cfg);

    /*
     * TODO: This should acquire a prepared transaction discovery lock RW lock in write mode.
     * Any thread wanting to commit a prepared transaction should acquire that lock in read mode
     * (or return an error).
     * If the write lock is already held, this should exit immediately.
     */
    WT_RET(__wt_calloc_one(session, &cursor_prepare));
    cursor = (WT_CURSOR *)cursor_prepare;
    *cursor = iface;
    cursor->session = (WT_SESSION *)session;
    cursor->key_format = "Q";  /* The key is an unsigned 64 bit number. */
    cursor->value_format = ""; /* Empty for now, will probably have something eventually */

    /*
     * Start the prepared transaction cursor which will fill in the cursor's list. Acquire the
     * schema lock, we need a consistent view of the metadata when scanning for prepared artifacts.
     */
    WT_WITH_CHECKPOINT_LOCK(session,
      WT_WITH_SCHEMA_LOCK(
        session, ret = __cursor_prepared_txn_setup(session, cursor_prepare)));
    WT_ERR(ret);
    WT_ERR(__wt_cursor_init(cursor, uri, NULL, cfg, cursorp));

    if (0) {
err:
        WT_TRET(__cursor_prepared_txn_close(cursor));
        *cursorp = NULL;
    }

    return (ret);
}

/*
 * __cursor_prepared_txn_setup --
 *     Setup a prepared transaction cursor on open. This will populate the data structures for the
 *     cursor to traverse. Some data structures live in this cursor, others live in the connection
 *     handle, since they can be claimed by other sessions while the cursor is open.
 */
static int
__cursor_prepared_txn_setup(
  WT_SESSION_IMPL *session, WT_CURSOR_PREPARE_TXN *cursor_prepare)
{
    /*
     * TODO: Replace this with code that traverses the metadata looking for tables with prepared
     * content, generating a WiredTiger transaction structure for each prepared transaction found,
     * and appending an entry to the cursors list for each unique transaction.
     */
    WT_RET(__cursor_prepared_txn_list_append(session, cursor_prepare, 123));
    return (0);
}

/*
 * __cursor_prepared_txn_list_append --
 *     Append a new prepared transaction identifier to the cursors list.
 */
static int
__cursor_prepared_txn_list_append(
  WT_SESSION_IMPL *session, WT_CURSOR_PREPARE_TXN *cursor_prepare, uint64_t prepared_id)
{
    uint64_t *p;

    /* Leave a NULL at the end to mark the end of the list. */
    WT_RET(__wt_realloc_def(session, &cursor_prepare->list_allocated, cursor_prepare->list_next + 2,
      &cursor_prepare->list));
    p = &cursor_prepare->list[cursor_prepare->list_next];
    p[0] = prepared_id;
    p[1] = 0;

    ++cursor_prepare->list_next;

    return (0);
}


/*
 * __prepared_discover_btree_has_prepare --
 *     Check the metadata entry for a btree to see whether it included prepared updates.
 */
static int
__prepared_discover_btree_has_prepare(WT_SESSION_IMPL *session, const char *config, bool *has_prepp)
{
    WT_CONFIG ckptconf;
    WT_CONFIG_ITEM cval, key, value;
    bool prepared_updates;

    *has_prepp = false;

    /* This configuration parsing is copied out of the rollback to stable implementation */
    WT_RET(__wt_config_getones(session, config, "checkpoint", &cval));
    __wt_config_subinit(session, &ckptconf, &cval);
    for (; __wt_config_next(&ckptconf, &key, &cval) == 0;) {
        ret = __wt_config_subgets(session, &cval, "prepare", &value);
        if (ret == 0) {
            if (value.val)
                *has_prepp = true;
        }
        WT_RET_NOTFOUND_OK(ret);
    }
    return (0);
}

/*
 * __prepared_discover_process_update --
 *     We found a prepared update, add it into a prepared transaction context.
 */
static int
__prepared_discover_process_update(WT_SESSION_IMPL *session, WT_UPDATE *first_upd)
{
    WT_ASSERT(session, first_upd->prepare_state != WT_PREPARE_INIT);

    prepare_timestamp = prepare_txn_id = first_upd->start_ts;
    return (0);
}

/*
 * __prepared_discover_process_update_list --
 *     Review an update list looking for prepared updates. If a prepared update is found, insert
 *     it into a pending prepared transaction structure.
 */
static int
__prepared_discover_process_update_list(WT_SESSION_IMPL *session, WT_ITEM *key, WT_UPDATE *first_upd)
{
    WT_UPDATE *upd;

    for (upd = first_upd; upd != NULL; upd = upd->next) {
        /*
         * Prepared updates must be at the start of the chain, so the first time an update that
         * hasn't been prepared is seen, it's safe to terminate the update chain traversal
         */
        if (upd->prepare_state == WT_PREPARE_INIT) {
            break;
        }
        WT_RET(__prepared_discover_process_prepared_update(session, upd));
    }

    return (0);
}

/*
 * __prepared_discover_process_insert_list --
 *     Review an insert list looking for prepared entries that need to be added to 
 *     had stable updates.
 */
static int
__prepared_discover_process_insert_list(WT_SESSION_IMPL *session, WT_PAGE *page, WT_INSERT_HEAD *head)
{
    WT_DECL_ITEM(key);
    WT_DECL_ITEM(key_string);
    WT_DECL_RET;
    WT_INSERT *ins;
    uint64_t recno;
    uint8_t *memp;

    WT_ERR(
      __wt_scr_alloc(session, page->type == WT_PAGE_ROW_LEAF ? 0 : WT_INTPACK64_MAXSIZE, &key));

    WT_ERR(__wt_scr_alloc(session, 0, &key_string));
    WT_SKIP_FOREACH (ins, head)
        if (ins->upd != NULL) {
            if (page->type == WT_PAGE_ROW_LEAF) {
                key->data = WT_INSERT_KEY(ins);
                key->size = WT_INSERT_KEY_SIZE(ins);
            } else {
                recno = WT_INSERT_RECNO(ins);
                memp = key->mem;
                WT_ERR(__wt_vpack_uint(&memp, 0, recno));
                key->size = WT_PTRDIFF(memp, key->data);
            }

            WT_ERR(__prepared_discover_process_update_list(session, key, ins->upd));
        }

err:
    __wt_scr_free(session, &key);
    __wt_scr_free(session, &key_string);
    return (ret);
}

/*
 * __prepared_discover_process_row_store_leaf_page --
 *     Find and process any prepared artifacts on this row store leaf page. It could either be clean
 *     or have been modified. So handle the full possible page structure, not just a clean image.
 */
static int
__rts_btree_abort_row_leaf(WT_SESSION_IMPL *session, WT_REF *ref)
{
    WT_CELL_UNPACK_KV *vpack, _vpack;
    WT_DECL_ITEM(key);
    WT_DECL_RET;
    WT_INSERT_HEAD *insert;
    WT_PAGE *page;
    WT_ROW *rip;
    WT_UPDATE *upd;
    uint32_t i;
    char ts_string[WT_TS_INT_STRING_SIZE];

    page = ref->page;

    WT_RET(__wt_scr_alloc(session, 0, &key));

    /*
     * Review the insert list for keys before the first entry on the disk page.
     */
    if ((insert = WT_ROW_INSERT_SMALLEST(page)) != NULL)
        WT_ERR(__prepared_discover_process_isnert_list(session, page, insert, NULL));

    /*
     * Review updates that belong to keys that are on the disk image, as well as for keys inserted
     * since the page was read from disk.
     */
    WT_ROW_FOREACH (page, rip, i) {

        /* Process either the update list or disk image cell for the next entry on the page */
        if ((upd = WT_ROW_UPDATE(page, rip)) != NULL) {
            WT_ERR(__wt_row_leaf_key(session, page, rip, key, false));
            WT_ERR(__prepared_discover_process_update_list(session, key, upd));
        } else {
            /* If there was no update list, check the disk image cell for a prepared update */
            vpack = &_vpack;
            __wt_row_leaf_value_cell(session, page, rip, vpack);

            WT_ERR(__rts_btree_abort_ondisk_kv(
              session, ref, rip, 0, NULL, vpack, NULL));
        }

        /* Walk through any intermediate insert list. */
        if ((insert = WT_ROW_INSERT(page, rip)) != NULL) {
            WT_ERR(__prepared_discover_process_insert_list(session, page, insert, NULL));
        }
    }

err:
    __wt_scr_free(session, &key);
    return (ret);
}

/*
 * __prepared_discover_process_leaf_page --
 *     Review the content of a leaf page discovering and processing prepared updates.
 */
int
__prepared_discover_process_leaf_page(WT_SESSION_IMPL *session, WT_REF *ref)
{
    WT_PAGE *page;

    page = ref->page;
    modified = __wt_page_is_modified(page);

    switch (page->type) {
    case WT_PAGE_ROW_LEAF:
        WT_RET(__prepared_discover_process_row_store_leaf_page(session, ref));
        break;
    case WT_PAGE_COL_FIX:
    case WT_PAGE_COL_VAR:
        WT_ASSERT_ALWAYS(session, false, "Prepared discovery does not support column stores");
        /* Fall through. */
    case WT_PAGE_COL_INT:
    case WT_PAGE_ROW_INT:
        /* This function is not called for internal pages. */
        WT_ASSERT(session, false);
        /* Fall through. */
    default:
        WT_RET(__wt_illegal_value(session, page->type));
    }

    /* Mark the page as dirty to reconcile the page. */
    if (!dryrun && page->modify)
        __wt_page_modify_set(session, page);
    return (0);
}

/*
 * __prepared_discover_tree_walk_skip --
 *     Skip if rollback to stable doesn't require reading this page.
 */
static int
prepared_discover_tree_walk_skip(
  WT_SESSION_IMPL *session, WT_REF *ref, void *context, bool visible_all, bool *skipp)
{
    WT_PAGE_DELETED *page_del;
    char time_string[3][WT_TIME_STRING_SIZE];

    WT_UNUSED(context);
    WT_UNUSED(visible_all);

    *skipp = false; /* Default to reading */

    /*
     * Fast deleted pages can be ignored here, unless they were generated by a prepared transaction.
     * This code doesn't currently support truncate within a prepared transaction - trigger a fatal
     * error if we encounter that.
     */
    if (WT_REF_GET_STATE(ref) == WT_REF_DELETED &&
      WT_REF_CAS_STATE(session, ref, WT_REF_DELETED, WT_REF_LOCKED)) {
        page_del = ref->page_del;
        WT_ASSERT_ALWAYS(session, page_del->prepare_state == WT_PREPARE_INIT,
                "Prepared transaction discovery does not support truncate operations");
    }
    /*
     * Any other deleted page can't have prepared content that needs to be discovered, so it is
     * safe to skip it.
     */
    if (WT_REF_GET_STATE(ref) == WT_REF_DELETED) {
        *skipp = true;
        return (0);
    }

    /*
     * Otherwise, if the page state is other than on disk, it is probably in-memory and we can't
     * relly on the address or cell information from the disk image to decide if it has prepared
     * content to discover.
     */
    if (WT_REF_GET_STATE(ref) != WT_REF_DISK)
        return (0);

    /*
     * Check whether this on-disk page or it's children has any prepared content.
     */
    if (!__wt_off_page(ref->home, ref->addr)) {
        /* Check if the page is obsolete using the page disk address. */
        __wt_cell_unpack_addr(session, ref->home->dsk, (WT_CELL *)addr, &vpack);
        /* Retrieve the time aggregate from the unpacked address cell. */
        __wt_cell_get_ta(&vpack, &ta);
        if (!ta->prepare)
            *skipp = true;
    } else if (ref->addr != NULL) {
        if (!addr->ta.prepare)
            *skipp = true;
    } else
        __wt_abort(session, "Prepared discovery walk encountered a page wihout a valid address");

    return (0);
}

/*
 * __prepared_discover_walk_one_tree --
 *     Walk a btree handle that is known to have prepared artifacts, attaching them to transaction
 *     handles as they are discovered.
 */
static int
__prepared_discover_walk_one_tree(WT_SESSION_IMPL *session, const char *uri)
{
    WT_DECL_RET;
    uint32_t flags;

    /* Open a handle for processing. */
    ret = __wt_session_get_dhandle(session, uri, NULL, NULL, 0);
    if (ret != 0)
        WT_RET_MSG(session, ret, "%s: unable to open handle%s", uri,
          ret == EBUSY ? ", error indicates handle is unavailable due to concurrent use" : "");
    /* There is nothing to do on an empty tree. */
    if (btree->root.page != NULL) {
        flags = WT_READ_NO_EVICT | WT_READ_VISIBLE_ALL | WT_READ_WONT_NEED | WT_READ_SEE_DELETED;
        while ((ret = __wt_tree_walk_custom_skip(
                  session, &ref, __prepared_discover_tree_walk_skip, NULL, flags)) == 0 &&
          ref != NULL) {

            if (F_ISSET(ref, WT_REF_FLAG_LEAF))
                WT_ERR(__prepared_discover_process_leaf_page(session, ref, rollback_timestamp));
        }
err:
    WT_TRET(__wt_session_release_dhandle(session));
    return (ret);
}

/*
 * __prepared_discover_filter_apply_handles --
 *     Review the metadata and identify btrees that have prepared content that needs to be
 *     discovered
 */
static int
__prepared_discover_filter_apply_handles(WT_SESSION_IMPL *session)
{
    WT_CURSOR *cursor;
    WT_DECL_RET;
    int t_ret;
    const char *uri, *config;
    bool has_prepare;

    /*
     * TODO: how careful does this need to be about concurrent schema operations? If this step
     * needs to be exclusive in some way it should probably accumulate a set of relevant handles
     * before releasing that access and doing the processing after generating the list.
     */
    WT_RET(__wt_metadata_cursor(session, &cursor));

    while ((ret = cursor->next(cursor)) == 0) {
        WT_ERR(cursor->get_key(cursor, &uri));
        /* Only intersted in btree handles that aren't the metadata */
       if (!WT_BTREE_PREFIX(uri) || strcmp(uri, WT_METAFILE_URI) == 0)
            continue;

        WT_ERR(cursor->get_value(cursor, &config));
        if (t_ret != 0)
            config = NULL;
        /* Check to see if there is any prepared content in the handle */
        WT_TRET(__wt_prepared_discover_btree_has_prepare(session, config, &has_prepare));
        if (!has_prepare)
            continue;
        }
    }
err:
    WT_TRET(__wt_metadata_cursor_release(session, &cursor));

    return (ret);
}
