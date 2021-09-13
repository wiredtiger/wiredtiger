/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

typedef struct {
    uint8_t *key; /* Key/value pair */
    size_t ksize;
    uint8_t *value;
    size_t vsize;
    bool isleaf; /* Page type */

    WT_SESSION_IMPL *session; /* Enclosing session */
    int qsort_ret;
} WT_BULK_ROOT_KEY;

typedef struct {
    WT_SPINLOCK lock; /* Single-thread bulk-loading cursors */
    uint32_t count;   /* Bulk-load open cursor count */
    uint32_t fid;     /* Next bulk-load file ID */

    WT_BULK_ROOT_KEY *rootkeys; /* Root keys */
    size_t rootkey_allocated;   /* Bytes allocated */
    uint32_t rootkey_count;     /* Root key count */
} WT_BULK_LOAD;

/*
 * __bulk_open_fs --
 *     Open a file stream.
 */
static int
__bulk_open_fs(WT_SESSION_IMPL *session, uint32_t id, bool readonly, WT_FSTREAM **fsp)
{
    WT_BTREE *btree;
    char buf[128];

    btree = S2BT(session);

    /*
     * KEITH: Need a mechanism to remove these files after a crash.
     */
    WT_RET(__wt_snprintf(buf, sizeof(buf), "bulkload.%" PRIu32 ".%" PRIu32, btree->id, id));
    return (__wt_fopen(session, buf, WT_FS_OPEN_CREATE | WT_FS_OPEN_ACCESS_SEQ,
      readonly ? WT_STREAM_READ : WT_STREAM_WRITE, fsp));
}

/*
 * __bulk_rec_init --
 *     Initialize reconciliation for a bulk-load pass.
 */
static int
__bulk_rec_init(WT_CURSOR_BULK *cbulk, uint32_t id, bool leafpage)
{
    WT_BTREE *btree;
    WT_RECONCILE *r;
    WT_REF *ref;
    WT_SESSION_IMPL *session;
    uint64_t recno;

    btree = CUR2BT(cbulk);
    session = CUR2S(cbulk);

    /* Reconciliation requires a page, create a fake one. */
    ref = &cbulk->ref;
    memset(ref, 0, sizeof(WT_REF));
    WT_RET(__wt_btree_new_page(session, leafpage, ref));
    WT_REF_SET_STATE(ref, WT_REF_MEM);
    WT_RET(__wt_page_modify_init(session, ref->page));
    __wt_page_only_modify_set(session, ref->page);

    /* Initialize reconciliation. */
    WT_RET(__wt_rec_init(session, ref, 0, NULL, &cbulk->reconcile));

    r = cbulk->reconcile;
    r->is_bulk_load = true;
    recno = btree->type == BTREE_ROW ? WT_RECNO_OOB : 1;

    WT_RET(__wt_rec_split_init(session, r, ref->page, recno, btree->maxleafpage_precomp));

    /* Open a backing file. */
    return (__bulk_open_fs(session, id, false, &r->bulk_fs));
}

/*
 * __bulk_rec_destroy --
 *     Close down reconciliation for a bulk-load pass.
 */
static int
__bulk_rec_destroy(WT_CURSOR_BULK *cbulk)
{
    WT_BTREE *btree;
    WT_DECL_RET;
    WT_RECONCILE *r;
    WT_SESSION_IMPL *session;

    btree = CUR2BT(cbulk);
    session = CUR2S(cbulk);

    if ((r = cbulk->reconcile) != NULL) {
        switch (btree->type) {
        case BTREE_COL_FIX:
            if (cbulk->entry != 0)
                __wt_rec_incr(
                  session, r, cbulk->entry, __bitstr_size((size_t)cbulk->entry * btree->bitcnt));
            break;
        case BTREE_COL_VAR:
            if (cbulk->rle != 0)
                WT_TRET(__wt_bulk_insert_var(session, cbulk, false));
            break;
        case BTREE_ROW:
            break;
        }

        WT_TRET(__wt_rec_split_finish(session, r));
    }

    WT_TRET(__wt_fclose(session, &r->bulk_fs));

    __wt_page_modify_clear(session, cbulk->ref.page);
    __wt_ref_out(session, &cbulk->ref);

    /* Save the number of pages this cursor wrote for the next stage. */
    cbulk->written = r->bulk_fs_pages;

    __wt_rec_cleanup(session, r);
    __wt_rec_destroy(session, &cbulk->reconcile);

    return (0);
}

/*
 * __bulk_row_internal --
 *     Build a new level of row-store internal pages.
 */
static int
__bulk_row_internal(WT_CURSOR_BULK *cbulk, bool isleaf)
{
    WT_ADDR addr;
    WT_BTREE *btree;
    WT_BULK_LOAD *bulk_load;
    WT_DECL_ITEM(hex);
    WT_DECL_ITEM(tmp);
    WT_DECL_RET;
    WT_FSTREAM *input;
    WT_RECONCILE *r;
    WT_REC_CHUNK *chunk;
    WT_REC_KV *key, *val;
    WT_SESSION_IMPL *session;
    bool ovfl_key;

    btree = CUR2BT(cbulk);
    session = CUR2S(cbulk);
    bulk_load = btree->bulk_load;
    input = NULL;

    WT_TIME_AGGREGATE_INIT(&addr.ta);
    addr.reuse = 0;

    /* Open the previous backing file. */
    WT_RET(__bulk_open_fs(session, cbulk->fid, true, &input));

    /* Initialize reconciliation. */
    __wt_spin_lock(session, &btree->bulk_load_lock);
    cbulk->fid = ++bulk_load->fid;
    __wt_spin_unlock(session, &btree->bulk_load_lock);
    WT_ERR(__bulk_rec_init(cbulk, cbulk->fid, false));
    r = cbulk->reconcile;
    cbulk->key_set = false;
    key = &r->k;
    val = &r->v;

    WT_ERR(__wt_scr_alloc(session, 0, &hex));
    WT_ERR(__wt_scr_alloc(session, 0, &tmp));
    for (;;) {
        WT_ERR(__wt_getline(session, input, hex));
        if (hex->size == 0)
            break;
        WT_ERR(__wt_hex_to_raw(session, hex->mem, tmp));
        /*
         * KEITH: this is wrong. The underlying split-init routine normally sets the key, but sets
         * it from the page that we're reconciling. We don't have a page we're reconciling here...
         */
        if (!cbulk->key_set) {
            chunk = r->cur_ptr;
            WT_ERR(__wt_buf_set(session, &chunk->key, tmp->data, tmp->size));
            cbulk->key_set = true;
        }
        WT_ERR(__wt_rec_cell_build_int_key(session, r, tmp->mem, tmp->size, &ovfl_key));

        WT_ERR(__wt_getline(session, input, hex));
        WT_ERR(__wt_hex_to_raw(session, hex->mem, tmp));
        addr.addr = tmp->mem;
        addr.size = tmp->size;
        addr.type = isleaf ? WT_ADDR_LEAF : WT_ADDR_INT;
        __wt_rec_cell_build_addr(session, r, &addr, NULL, false, WT_RECNO_OOB);

        /* Boundary: split or write the page. */
        if (__wt_rec_need_split(r, key->len + val->len))
            WT_ERR(__wt_rec_split_crossing_bnd(session, r, key->len + val->len, false));

        /* Copy the key and value onto the page. */
        __wt_rec_image_copy(session, r, key);
        __wt_rec_image_copy(session, r, val);
        /*
         * KEITH We don't really need to aggregate the timestamp information, it's always empty, but
         * this code looks like other row-store internal page builders...
         */
        WT_TIME_AGGREGATE_MERGE(session, &r->cur_ptr->ta, &addr.ta);

        /* Update compression state. */
        __wt_rec_key_state_update(r, ovfl_key);
    }

err:
    WT_TRET(__bulk_rec_destroy(cbulk));
    if (input != NULL)
        WT_TRET(__wt_fclose(session, &input));

    __wt_scr_free(session, &hex);
    __wt_scr_free(session, &tmp);
    return (ret);
}

/*
 * __bulk_root_prep --
 *     Build per-cursor structures for the root page.
 */
static int
__bulk_root_prep(WT_CURSOR_BULK *cbulk, bool isleaf)
{
    WT_BTREE *btree;
    WT_BULK_LOAD *bulk_load;
    WT_BULK_ROOT_KEY *rootkeys;
    WT_DECL_ITEM(hex);
    WT_DECL_ITEM(tmp);
    WT_DECL_RET;
    WT_FSTREAM *input;
    WT_SESSION_IMPL *session;
    size_t rootkey_allocated;
    uint32_t i;

    btree = CUR2BT(cbulk);
    session = CUR2S(cbulk);
    bulk_load = btree->bulk_load;
    rootkeys = NULL;
    input = NULL;

    /* Open the previous backing file. */
    WT_RET(__bulk_open_fs(session, cbulk->fid, true, &input));

    /* Read the key/value pairs and store them in memory. */
    WT_ERR(__wt_scr_alloc(session, 0, &hex));
    WT_ERR(__wt_scr_alloc(session, 0, &tmp));
    for (i = 0, rootkey_allocated = 0;; ++i) {
        WT_ERR(__wt_getline(session, input, hex));
        if (hex->size == 0)
            break;
        WT_ERR(__wt_realloc_def(session, &rootkey_allocated, i + 1, &rootkeys));
        WT_ERR(__wt_hex_to_raw(session, hex->mem, tmp));
        WT_ERR(__wt_memdup(session, tmp->data, tmp->size, &rootkeys[i].key));
        rootkeys[i].ksize = tmp->size;

        WT_ERR(__wt_getline(session, input, hex));
        WT_ERR(__wt_hex_to_raw(session, hex->mem, tmp));
        WT_ERR(__wt_memdup(session, tmp->data, tmp->size, &rootkeys[i].value));
        rootkeys[i].vsize = tmp->size;

        rootkeys[i].isleaf = isleaf;
    }
    WT_ASSERT(session, i > 0);

    /* Aggregate the key/value pairs into the underlying btree. */
    __wt_spin_lock(session, &btree->bulk_load_lock);
    WT_ERR(__wt_realloc_def(
      session, &bulk_load->rootkey_allocated, bulk_load->rootkey_count + i, &bulk_load->rootkeys));
    do {
        --i;
        bulk_load->rootkeys[bulk_load->rootkey_count] = rootkeys[i];
        ++bulk_load->rootkey_count;
    } while (i > 0);
    __wt_spin_unlock(session, &btree->bulk_load_lock);

err:
    if (input != NULL)
        WT_TRET(__wt_fclose(session, &input));
    if (rootkeys != NULL)
        __wt_free(session, rootkeys);

    __wt_scr_free(session, &hex);
    __wt_scr_free(session, &tmp);
    return (ret);
}

/*
 * __bulk_rootkey_compare --
 *     Qsort comparison routine for rootkeys.
 */
static int WT_CDECL
__bulk_rootkey_compare(const void *a, const void *b)
{
    WT_BULK_ROOT_KEY *a_key, *b_key;
    WT_COLLATOR *collator;
    WT_DECL_RET;
    WT_ITEM a_item, b_item;
    WT_SESSION_IMPL *session;
    int cmp;

    a_key = (WT_BULK_ROOT_KEY *)a;
    b_key = (WT_BULK_ROOT_KEY *)b;
    session = a_key->session;
    collator = S2BT(session)->collator;

    a_item.data = a_key->key;
    a_item.size = a_key->ksize;
    b_item.data = b_key->key;
    b_item.size = b_key->ksize;

    if ((ret = __wt_compare(session, collator, &a_item, &b_item, &cmp)) != 0)
        a_key->qsort_ret = ret;
    return (cmp);
}

/*
 * __bulk_root --
 *     Resolve the bulk load, building the tree's root page.
 */
static int
__bulk_root(WT_SESSION_IMPL *session)
{
    WT_ADDR *addr;
    WT_BTREE *btree;
    WT_BULK_LOAD *bulk_load;
    WT_DECL_RET;
    WT_PAGE *root;
    WT_PAGE_INDEX *pindex;
    WT_REF *ref, **refp;
    uint32_t i;

    addr = NULL;
    btree = S2BT(session);
    bulk_load = btree->bulk_load;

    /* Add the session to the root key structures so sort can call the underlying collator. */
    for (i = 0; i < bulk_load->rootkey_count; ++i)
        bulk_load->rootkeys[i].session = session;

    /* Sort the root keys. */
    __wt_qsort(bulk_load->rootkeys, bulk_load->rootkey_count, sizeof(bulk_load->rootkeys[0]),
      __bulk_rootkey_compare);

    /* Check for failures. */
    for (i = 0; i < bulk_load->rootkey_count; ++i)
        WT_TRET(bulk_load->rootkeys[i].qsort_ret);
    WT_ERR(ret);

    /* Allocate a root (internal) page and fill it in. */
    WT_ERR(__wt_page_alloc(session, btree->type == BTREE_ROW ? WT_PAGE_ROW_INT : WT_PAGE_COL_INT,
      bulk_load->rootkey_count, true, &root));
    WT_ERR(__wt_page_modify_init(session, root));
    __wt_page_modify_set(session, root);

    pindex = WT_INTL_INDEX_GET_SAFE(root);
    for (refp = pindex->index, i = 0; i < bulk_load->rootkey_count; ++i) {
        ref = *refp++;
        ref->home = root;
        ref->page = NULL;

        /*
         * KEITH: Don't set WT_ADDR_LEAF_NO here, there's the possibility of an overflow record on
         * the child pages. It's safe (because WT_ADDR_LEAF is only advisory), but it's not quite
         * right either. To fix this would need to track overflow records as we built internal
         * pages.
         */
        WT_ERR(__wt_calloc_one(session, &addr));
        WT_TIME_AGGREGATE_INIT(&addr->ta);
        WT_ERR(__wt_memdup(
          session, bulk_load->rootkeys[i].value, bulk_load->rootkeys[i].vsize, &addr->addr));
        addr->size = bulk_load->rootkeys[i].vsize;
        addr->type = bulk_load->rootkeys[i].isleaf ? WT_ADDR_LEAF : WT_ADDR_INT;
        ref->addr = addr;
        addr = NULL;

        __wt_ref_key_clear(ref);
        F_SET(ref, bulk_load->rootkeys[i].isleaf ? WT_REF_FLAG_LEAF : WT_REF_FLAG_INTERNAL);
        WT_REF_SET_STATE(ref, WT_REF_DISK);

        WT_ERR(
          __wt_row_ikey(session, 0, bulk_load->rootkeys[i].key, bulk_load->rootkeys[i].ksize, ref));
        ++ref;
    }

    /* Swap the page into the btree root structure. */
    __wt_ref_out(session, &btree->root);
    root->pg_intl_parent_ref = &btree->root;
    __wt_root_ref_init(session, &btree->root, root, btree->type != BTREE_ROW);

    if (0) {
err:
        __wt_free(session, addr);
        __wt_page_out(session, &root);
    }

    /* Free root key allocated memory. */
    do {
        --bulk_load->rootkey_count;
        __wt_free(session, bulk_load->rootkeys[bulk_load->rootkey_count].key);
        __wt_free(session, bulk_load->rootkeys[bulk_load->rootkey_count].value);
    } while (bulk_load->rootkey_count > 0);
    __wt_free(session, bulk_load->rootkeys);
    __wt_free(session, btree->bulk_load);

    return (ret);
}

/*
 * __wt_bulk_init --
 *     Bulk insert initialization.
 */
int
__wt_bulk_init(WT_SESSION_IMPL *session, WT_CURSOR_BULK *cbulk)
{
    WT_BTREE *btree;
    WT_BULK_LOAD *bulk_load;
    WT_DECL_RET;

    btree = CUR2BT(cbulk);

    /*
     * Bulk-load is only permitted on newly created files, not any empty file -- see the checkpoint
     * code for a discussion.
     *
     * KEITH: doesn't support VLCS/FLCS, yet and that gets tricky unless we require there be no gaps
     * in the record name space (which is not a requirement of the previous bulk load code). If we
     * allow gaps in the name space, then there are potentially implicit records between leaf pages
     * we've created. I think we'd have to have each thread write out all of its pages but the last
     * one, and at that point figure out if it needs to insert a record to handle any record gap. I
     * am not sure though -- both the "fast split on append" and original bulk load code have magic
     * to handle how column-store loads/splits/whatever. Anyway, this will require some thought.
     *
     * KEITH: This locking is insufficient. If we're no longer acquiring an exclusive handle on the
     * bulk-load cursor, other cursors can be opened. Bulk cursors need to block that path, having
     * both bulk and non-bulk cursors on an object doesn't make any sense. I think we should go with
     * whatever is opened first: if you open a non-bulk-load cursor, turn off the ability to open a
     * bulk-load cursor. Applications where the operations can race don't make a lot of sense. For
     * now, this just single-threads access to the bulk-load itself.
     */
    bulk_load = btree->bulk_load;
    ;
    __wt_spin_lock(session, &btree->bulk_load_lock);
    if (btree->original) {
        if (btree->bulk_load == NULL && (ret = __wt_calloc_one(session, &bulk_load)) == 0)
            btree->bulk_load = bulk_load;
        if (ret == 0) {
            ++bulk_load->count;
            cbulk->fid = ++bulk_load->fid;
        }
    } else
        ret = EINVAL;
    __wt_spin_unlock(session, &btree->bulk_load_lock);

    if (ret != 0)
        WT_RET_MSG(session, ret, "bulk-load is only possible for newly created trees");

    return (__bulk_rec_init(cbulk, cbulk->fid, true));
}

/*
 * __wt_bulk_wrapup --
 *     Bulk insert cleanup.
 */
int
__wt_bulk_wrapup(WT_SESSION_IMPL *session, WT_CURSOR_BULK *cbulk)
{
    WT_BTREE *btree;
    WT_BULK_LOAD *bulk_load;
    WT_DECL_RET;
    bool isleaf;

    btree = CUR2BT(cbulk);
    bulk_load = btree->bulk_load;

    WT_ERR(__bulk_rec_destroy(cbulk));

    /*
     * Create internal page levels until we're down to a manageable level. I'd expect 50 threads to
     * be large number of threads loading, and if each has 50 blocks to merge, that's 2500 objects
     * in the final root page, which is a manageable number to sort. The alternative is to increase
     * the depth of the tree, which isn't desirable,
     */
    for (isleaf = true; cbulk->written > 50; isleaf = false)
        WT_ERR(__bulk_row_internal(cbulk, isleaf));

    /* Copy this cursor's key/address pairs to the underlying tree structure */
    WT_ERR(__bulk_root_prep(cbulk, isleaf));

    /* When the last open cursor closes, build the root page. */
    __wt_spin_lock(session, &btree->bulk_load_lock);
    if (--bulk_load->count == 0) {
        ret = __bulk_root(session);
    }
    __wt_cursor_disable_bulk(session);
    __wt_spin_unlock(session, &btree->bulk_load_lock);

    return (0);

err:
    WT_RET_MSG(session, ret, "%s failed", __func__);
}
