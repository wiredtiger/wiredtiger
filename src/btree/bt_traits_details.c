#include "wt_internal.h"
#include "bt_traits_details.h"

/*
 * __bt_col_fix_huffman --
 *     col fix doesn't support huffman.
 */
int
__bt_col_fix_huffman(WT_SESSION_IMPL *session, size_t len)
{
    WT_UNUSED(len);
    WT_RET_MSG(session, EINVAL, "fixed-size column-store files may not be Huffman encoded");
}

/*
 * __bt_col_var_huffman --
 *     Check whether col var supports huffman.
 */
int
__bt_col_var_huffman(WT_SESSION_IMPL *session, size_t len)
{
    if (len != 0)
        WT_RET_MSG(session, EINVAL,
          "the keys of variable-length column-store files "
          "may not be Huffman encoded");
    return (0);
}

/*
 * __bt_row_huffman --
 *     row always supports huffman.
 */
int
__bt_row_huffman(WT_SESSION_IMPL *session, size_t len)
{
    WT_UNUSED(session);
    WT_UNUSED(len);
    return (0);
}

/*
 * __bt_col_fix_cursor_valid --
 *     Check cursor validity for col fix.
 */
int
__bt_col_fix_cursor_valid(WT_CURSOR_BTREE *cbt, WT_UPDATE **updp, bool *valid)
{
    WT_UNUSED(updp);

    /*
     * If search returned an insert object, there may or may not be a matching on-page object, we
     * have to check. Fixed-length column-store pages don't have slots, but map one-to-one to keys,
     * check for retrieval past the end of the page.
     */
    if (cbt->recno >= cbt->ref->ref_recno + cbt->ref->page->entries)
        return (0);
    *valid = true;
    return (0);
}

/*
 * __bt_col_var_cursor_valid --
 *     Check cursor validity for col var.
 */
int
__bt_col_var_cursor_valid(WT_CURSOR_BTREE *cbt, WT_UPDATE **updp, bool *valid)
{
    WT_CELL *cell;
    WT_COL *cip;
    WT_PAGE *page;
    WT_SESSION_IMPL *session;

    WT_UNUSED(updp);
    session = (WT_SESSION_IMPL *)cbt->iface.session;
    page = cbt->ref->page;

    /* The search function doesn't check for empty pages. */
    if (page->entries == 0)
        return (0);
    /*
     * In case of prepare conflict, the slot might not have a valid value, if the update in the
     * insert list of a new page scanned is in prepared state.
     */
    WT_ASSERT(session, cbt->slot == UINT32_MAX || cbt->slot < page->entries);

    /*
     * Column-store updates are stored as "insert" objects. If search returned an insert object we
     * can't return, the returned on-page object must be checked for a match.
     */
    if (cbt->ins != NULL && !F_ISSET(cbt, WT_CBT_VAR_ONPAGE_MATCH))
        return (0);

    /*
     * Although updates would have appeared as an "insert" objects, variable-length column store
     * deletes are written into the backing store; check the cell for a record already deleted when
     * read.
     */
    cip = &page->pg_var[cbt->slot];
    cell = WT_COL_PTR(page, cip);
    if (__wt_cell_type(cell) == WT_CELL_DEL)
        return (0);
    *valid = true;
    return (0);
}

/*
 * __bt_row_cursor_valid --
 *     Check cursor validity for row.
 */
int
__bt_row_cursor_valid(WT_CURSOR_BTREE *cbt, WT_UPDATE **updp, bool *valid)
{
    WT_PAGE *page;
    WT_SESSION_IMPL *session;
    WT_UPDATE *upd;

    session = (WT_SESSION_IMPL *)cbt->iface.session;
    page = cbt->ref->page;
    /* The search function doesn't check for empty pages. */
    if (page->entries == 0)
        return (0);
    /*
     * In case of prepare conflict, the slot might not have a valid value, if the update in the
     * insert list of a new page scanned is in prepared state.
     */
    WT_ASSERT(session, cbt->slot == UINT32_MAX || cbt->slot < page->entries);

    /*
     * See above: for row-store, no insert object can have the same key as an on-page object, we're
     * done.
     */
    if (cbt->ins != NULL)
        return (0);

    /* Check for an update. */
    if (page->modify != NULL && page->modify->mod_row_update != NULL) {
        WT_RET(__wt_txn_read(session, page->modify->mod_row_update[cbt->slot], &upd));
        if (upd != NULL) {
            if (upd->type == WT_UPDATE_TOMBSTONE)
                return (0);
            if (updp != NULL)
                *updp = upd;
        }
    }
    *valid = true;
    return (0);
}

/*
 * __bt_col_cursor_key_order_check --
 *     Check key ordering for column-store cursor movements.
 */
int
__bt_col_cursor_key_order_check(WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt, bool next)
{
    int cmp;

    cmp = 0; /* -Werror=maybe-uninitialized */

    if (cbt->lastrecno != WT_RECNO_OOB) {
        if (cbt->lastrecno < cbt->recno)
            cmp = -1;
        if (cbt->lastrecno > cbt->recno)
            cmp = 1;
    }

    if (cbt->lastrecno == WT_RECNO_OOB || (next && cmp < 0) || (!next && cmp > 0)) {
        cbt->lastrecno = cbt->recno;
        return (0);
    }

    WT_PANIC_RET(session, EINVAL, "WT_CURSOR.%s out-of-order returns: returned key %" PRIu64
                                  " then "
                                  "key %" PRIu64,
      next ? "next" : "prev", cbt->lastrecno, cbt->recno);
}

/*
 * __bt_row_cursor_key_order_check --
 *     Check key ordering for row-store cursor movements.
 */
int
__bt_row_cursor_key_order_check(WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt, bool next)
{
    WT_BTREE *btree;
    WT_DECL_ITEM(a);
    WT_DECL_ITEM(b);
    WT_DECL_RET;
    WT_ITEM *key;
    int cmp;

    btree = S2BT(session);
    key = &cbt->iface.key;
    cmp = 0; /* -Werror=maybe-uninitialized */

    if (cbt->lastkey->size != 0)
        WT_RET(__wt_compare(session, btree->collator, cbt->lastkey, key, &cmp));

    if (cbt->lastkey->size == 0 || (next && cmp < 0) || (!next && cmp > 0))
        return (__wt_buf_set(session, cbt->lastkey, cbt->iface.key.data, cbt->iface.key.size));

    WT_ERR(__wt_scr_alloc(session, 512, &a));
    WT_ERR(__wt_scr_alloc(session, 512, &b));

    WT_PANIC_ERR(session, EINVAL,
      "WT_CURSOR.%s out-of-order returns: returned key %.1024s then "
      "key %.1024s",
      next ? "next" : "prev", __wt_buf_set_printable_format(session, cbt->lastkey->data,
                                cbt->lastkey->size, btree->key_format, a),
      __wt_buf_set_printable_format(session, key->data, key->size, btree->key_format, b));

err:
    __wt_scr_free(session, &a);
    __wt_scr_free(session, &b);

    return (ret);
}
