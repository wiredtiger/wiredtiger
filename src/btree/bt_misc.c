/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_addr_string --
 *     Load a buffer with a printable, nul-terminated representation of an address.
 */
const char *
__wt_addr_string(WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size, WT_ITEM *buf)
{
    WT_BM *bm;
    WT_BTREE *btree;

    btree = S2BT_SAFE(session);

    WT_ASSERT(session, buf != NULL);

    if (addr == NULL || addr_size == 0) {
        buf->data = WT_NO_ADDR_STRING;
        buf->size = strlen(WT_NO_ADDR_STRING);
    } else if (btree == NULL || (bm = btree->bm) == NULL ||
      bm->addr_string(bm, session, buf, addr, addr_size) != 0) {
        buf->data = WT_ERR_STRING;
        buf->size = strlen(WT_ERR_STRING);
    }
    return (buf->data);
}

/*
 * __wt_cell_type_string --
 *     Return a string representing the cell type.
 */
const char *
__wt_cell_type_string(uint8_t type)
{
    switch (type) {
    case WT_CELL_ADDR_DEL:
        return ("addr_del");
    case WT_CELL_ADDR_INT:
        return ("addr_int");
    case WT_CELL_ADDR_LEAF:
        return ("addr_leaf");
    case WT_CELL_ADDR_LEAF_NO:
        return ("addr_leaf_no_ovfl");
    case WT_CELL_DEL:
        return ("deleted");
    case WT_CELL_KEY:
        return ("key");
    case WT_CELL_KEY_PFX:
        return ("key_pfx");
    case WT_CELL_KEY_OVFL:
        return ("key_ovfl");
    case WT_CELL_KEY_SHORT:
        return ("key_short");
    case WT_CELL_KEY_SHORT_PFX:
        return ("key_short_pfx");
    case WT_CELL_KEY_OVFL_RM:
        return ("key_ovfl_rm");
    case WT_CELL_VALUE:
        return ("value");
    case WT_CELL_VALUE_COPY:
        return ("value_copy");
    case WT_CELL_VALUE_OVFL:
        return ("value_ovfl");
    case WT_CELL_VALUE_OVFL_RM:
        return ("value_ovfl_rm");
    case WT_CELL_VALUE_SHORT:
        return ("value_short");
    default:
        return ("unknown");
    }
    /* NOTREACHED */
}

/*
 * __wt_key_string --
 *     Load a buffer with a printable, nul-terminated representation of a key.
 */
const char *
__wt_key_string(
  WT_SESSION_IMPL *session, const void *data_arg, size_t size, const char *key_format, WT_ITEM *buf)
{
    WT_ITEM tmp;

#ifdef HAVE_DIAGNOSTIC
    if (session->dump_raw)
        return (__wt_buf_set_printable(session, data_arg, size, false, buf));
#endif

    /*
     * If the format is 'S', it's a string and our version of it may not yet be nul-terminated.
     */
    if (WT_STREQ(key_format, "S") && ((char *)data_arg)[size - 1] != '\0') {
        WT_CLEAR(tmp);
        if (__wt_buf_fmt(session, &tmp, "%.*s", (int)size, (char *)data_arg) == 0) {
            data_arg = tmp.data;
            size = tmp.size + 1;
        } else {
            data_arg = WT_ERR_STRING;
            size = sizeof(WT_ERR_STRING);
        }
    }
    return (__wt_buf_set_printable_format(session, data_arg, size, key_format, false, buf));
}

/*
 * __wt_page_type_string --
 *     Return a string representing the page type.
 */
const char *
__wt_page_type_string(u_int type) WT_GCC_FUNC_ATTRIBUTE((visibility("default")))
{
    switch (type) {
    case WT_PAGE_INVALID:
        return ("invalid");
    case WT_PAGE_BLOCK_MANAGER:
        return ("block manager");
    case WT_PAGE_COL_FIX:
        return ("column-store fixed-length leaf");
    case WT_PAGE_COL_INT:
        return ("column-store internal");
    case WT_PAGE_COL_VAR:
        return ("column-store variable-length leaf");
    case WT_PAGE_OVFL:
        return ("overflow");
    case WT_PAGE_ROW_INT:
        return ("row-store internal");
    case WT_PAGE_ROW_LEAF:
        return ("row-store leaf");
    default:
        return ("unknown");
    }
    /* NOTREACHED */
}

/*
 * __wt_page_is_root --
 *     Returns true if the page is a root page.
 */
static WT_INLINE bool
__wt_page_is_root(WT_PAGE *page)
{
    return (page->pg_intl_parent_ref == NULL || page->pg_intl_parent_ref->home == NULL);
}

/*
 * __wt_page_npos --
 *     Get the page's normalized position in the tree.
 */
double
__wt_page_npos(WT_SESSION_IMPL *session, WT_REF *ref)
{
    WT_PAGE *page, *parent_page;
    WT_REF *self_ref;
    double npos;
    uint32_t idx, entries;

    npos = 0.5;
    page = ref->home;
    if (!page)
        return (npos);

    for (;;) {
        WT_ASSERT(session, page != NULL);
        WT_ASSERT(session, WT_PAGE_IS_INTERNAL(page));

        self_ref = page->pg_intl_parent_ref; /* The field name is deceptive */
        parent_page = self_ref->home;

        idx = ref->pindex_hint;
        // ?? should be used with WT_WITH_PAGE_INDEX and WT_INTL_INDEX_GET()?
        entries = WT_INTL_INDEX_GET_SAFE(page)->entries;

        if (idx < entries)
            npos = (idx + npos) / entries;
        /*
         * If idx is incorrect, just leave the position as it is. We can scan all entries for the
         * correct position, but it's not worth it.
         */

        if (!parent_page || page == parent_page || __wt_page_is_root(page))
            break;

        ref = self_ref;
        page = parent_page;
    }

    return (npos);
}

/*
 * __wt_get_page_from_npos --
 *     Go to a leaf page given its normalized position. (The code is largely borrowed from
 *     __wt_random_descent)
 */
int
__wt_get_page_from_npos(WT_SESSION_IMPL *session, WT_REF **refp, uint32_t flags, double npos)
{
    WT_BTREE *btree;
    WT_DECL_RET;
    WT_PAGE *page;
    WT_PAGE_INDEX *pindex;
    WT_REF *current, *descent;
    double npos2;
    int i, idx, idx2, entries;
    bool eviction;

    *refp = NULL;

    btree = S2BT(session);
    current = NULL;
    /*
     * This function is called by eviction to find a page in the cache. That case is indicated by
     * the WT_READ_CACHE flag. Ordinary lookups in a tree will read pages into cache as needed.
     */
    eviction = LF_ISSET(WT_READ_CACHE);

    if (0) {
restart:
        /*
         * Discard the currently held page and restart the search from the root.
         */
        WT_RET(__wt_page_release(session, current, flags));
    }

    /* Search the internal pages of the tree. */
    current = &btree->root;
    npos2 = npos;
    for (;;) {
        if (F_ISSET(current, WT_REF_FLAG_LEAF))
            break;

        page = current->page;
        // WT_INTL_INDEX_GET(session, page, pindex);
        pindex = WT_INTL_INDEX_GET_SAFE(page);
        entries = (int)pindex->entries;

        npos2 *= entries;
        idx = (int)npos2;
        idx = WT_CLAMP(idx, 0, entries - 1);
        npos2 -= idx;
        descent = pindex->index[idx];

        /* Eviction just wants any child. */
        if (eviction)
            goto descend;

        /*
         * There may be empty pages in the tree, and they're useless to us. Find a closest non-empty
         * page.
         */
        if (descent->state == WT_REF_DISK || descent->state == WT_REF_MEM)
            goto descend;

        for (i = 1; i < entries && descent != NULL; ++i) {
            descent = NULL;

            idx2 = idx - i;
            if (idx2 >= 0) {
                descent = pindex->index[idx2];
                if (descent->state == WT_REF_DISK || descent->state == WT_REF_MEM)
                    goto descend;
            }

            idx2 = idx + i;
            if (idx2 < entries) {
                descent = pindex->index[idx2];
                if (descent->state == WT_REF_DISK || descent->state == WT_REF_MEM)
                    goto descend;
            }
        }

        /* Unable to find any suitable page */

        WT_RET(__wt_page_release(session, current, flags));
        return (WT_NOTFOUND);

        /*
         * Swap the current page for the child page. If the page splits while we're retrieving it,
         * restart the search at the root.
         *
         * On other error, simply return, the swap call ensures we're holding nothing on failure.
         */
descend:
        if ((ret = __wt_page_swap(session, current, descent, flags)) == 0) {
            current = descent;
            continue;
        }
        if (eviction && (ret == WT_NOTFOUND || ret == WT_RESTART))
            break;
        if (ret == WT_RESTART)
            goto restart;
        return (ret);
    }

    /*
     * There is no point starting with the root page: the walk will exit immediately. In that case
     * we aren't holding a hazard pointer so there is nothing to release.
     */
    if (!eviction || !__wt_ref_is_root(current))
        *refp = current;
    return (0);
}
