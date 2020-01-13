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
