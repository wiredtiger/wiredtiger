/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * A diff walks two checkpoints of a row-store btree in lockstep, in key order. Checkpoints are
 * copy-on-write, and precise checkpoints write nothing newer than the checkpoint, so subtrees with
 * equal addresses have equal contents and are skipped without being read.
 *
 * Each side is always at its next leaf entry, its next unread subtree, or its end.
 */

/*
 * __btree_diff_ref_key_cmp --
 *     Compare the key of a subtree, the smallest key it can hold, with a key or the key of another
 *     subtree. The leftmost subtree has no key and sorts first.
 */
static int
__btree_diff_ref_key_cmp(WT_BTREE_DIFF *diff, const WT_ITEM *ref_key, const WT_ITEM *key, int *cmpp)
{
    if (ref_key->data == NULL) {
        *cmpp = key->data == NULL ? 0 : -1;
        return (0);
    }
    if (key->data == NULL) {
        *cmpp = 1;
        return (0);
    }

    return (__wt_compare(diff->session, diff->collator, ref_key, key, cmpp));
}

/*
 * __btree_diff_intl_search_lower_bound --
 *     Return the slot of the child of an internal page that holds the lower bound.
 */
static int
__btree_diff_intl_search_lower_bound(WT_BTREE_DIFF *diff, WT_PAGE *page, uint32_t *slotp)
{
    WT_ITEM key;
    WT_PAGE_INDEX *pindex;
    uint32_t base, limit, mid;
    int cmp;

    *slotp = 0;
    WT_CLEAR(key);
    WT_INTL_INDEX_GET(diff->session, page, pindex);

    /* Find the last key at or before the lower bound. The 0th key is not a real key. */
    for (base = 1, limit = pindex->entries; base < limit;) {
        mid = base + (limit - base) / 2;
        __wt_ref_key(page, pindex->index[mid], &key.data, &key.size);
        WT_RET(__wt_compare(diff->session, diff->collator, &key, diff->lower_bound, &cmp));
        if (cmp <= 0) {
            *slotp = mid;
            base = mid + 1;
        } else
            limit = mid;
    }

    return (0);
}

/*
 * __btree_diff_leaf_search_lower_bound --
 *     Position a side at the first entry of its leaf at or after the lower bound.
 */
static int
__btree_diff_leaf_search_lower_bound(WT_BTREE_DIFF *diff, WT_BTREE_DIFF_SIDE *side)
{
    WT_PAGE *page;
    uint32_t base, limit, mid;
    int cmp;

    page = side->leaf->page;

    for (base = 0, limit = page->entries; base < limit;) {
        mid = base + (limit - base) / 2;
        WT_RET(__wt_row_leaf_key(diff->session, page, &page->pg_row[mid], &side->key, false));
        WT_RET(__wt_compare(diff->session, diff->collator, &side->key, diff->lower_bound, &cmp));
        if (cmp < 0)
            base = mid + 1;
        else
            limit = mid;
    }

    side->leaf_slot = base;
    return (0);
}

/*
 * __btree_diff_side_release --
 *     Release a side's pages, ending the side.
 */
static int
__btree_diff_side_release(WT_SESSION_IMPL *session, WT_BTREE_DIFF_SIDE *side)
{
    WT_DECL_RET;

    side->subtree = NULL;

    if (side->leaf != NULL) {
        WT_TRET(__wt_page_release(session, side->leaf, 0));
        side->leaf = NULL;
    }

    /* Skip the root: the data handle pins it. */
    for (; side->depth > 1; --side->depth)
        WT_TRET(__wt_page_release(session, side->path[side->depth - 1].ref, 0));
    side->depth = 0;

    return (ret);
}

/*
 * __btree_diff_fail_prepared --
 *     Fail because the checkpoint holds a prepared update.
 */
static int
__btree_diff_fail_prepared(WT_SESSION_IMPL *session)
{
    WT_DATA_HANDLE *dhandle;

    /* A disaggregated stable checkpoint handle has its checkpoint in its name. */
    dhandle = session->dhandle;
    WT_RET_MSG(session, ENOTSUP, "btree diff: %s%s%s holds a prepared update", dhandle->name,
      dhandle->checkpoint == NULL ? "" : "/",
      dhandle->checkpoint == NULL ? "" : dhandle->checkpoint);
}

/*
 * __btree_diff_side_find_next_in_leaf --
 *     Move a side to the next live entry on its leaf, starting at the current one, and load its key
 *     and value. Return WT_NOTFOUND if the leaf has no more entries.
 */
static int
__btree_diff_side_find_next_in_leaf(WT_BTREE_DIFF *diff, WT_BTREE_DIFF_SIDE *side)
{
    WT_CELL_UNPACK_KV unpack;
    WT_PAGE *page;
    WT_ROW *rip;
    WT_SESSION_IMPL *session;
    int cmp;

    session = diff->session;
    page = side->leaf->page;

    for (; side->leaf_slot < page->entries; ++side->leaf_slot) {
        rip = &page->pg_row[side->leaf_slot];
        WT_RET(__wt_row_leaf_key(session, page, rip, &side->key, false));

        /* End the side at the upper bound. */
        if (diff->upper_bound != NULL) {
            WT_RET(__wt_compare(session, diff->collator, &side->key, diff->upper_bound, &cmp));
            if (cmp >= 0)
                return (__btree_diff_side_release(session, side));
        }

        /* A value encoded in the row reference is globally visible. */
        if (__wt_row_leaf_value(page, rip, &side->value))
            return (0);

        /* Otherwise check the cell's time window. */
        __wt_row_leaf_value_cell(session, page, rip, &unpack);
        if (WT_TIME_WINDOW_HAS_PREPARE(&unpack.tw))
            return (__btree_diff_fail_prepared(session));

        /* Precise checkpoints hold only committed stops, so a stop means the key is deleted. */
        if (WT_TIME_WINDOW_HAS_STOP(&unpack.tw))
            continue;

        return (__wt_page_cell_data_ref_kv(session, page, &unpack, &side->value));
    }

    return (WT_NOTFOUND);
}

/*
 * __btree_diff_side_find_next --
 *     Move a side to its next leaf entry or unread subtree, starting at its current position, or to
 *     its end. Never descends: it stops at a subtree for the caller to descend into.
 *
 * The caller tells where the side stopped from its fields. At a leaf entry, the leaf is set. At an
 *     unread subtree, the subtree is set instead. At the end, neither is set.
 */
static int
__btree_diff_side_find_next(WT_BTREE_DIFF *diff, WT_BTREE_DIFF_SIDE *side)
{
    WT_BTREE_DIFF_FRAME *frame;
    WT_DECL_RET;
    WT_PAGE_INDEX *pindex;
    WT_REF *child, *ref;
    WT_SESSION_IMPL *session;
    int cmp;

    session = diff->session;
    side->subtree = NULL;

    for (;;) {
        /* On a leaf: stop at its next entry, or release it and continue in the parent. */
        if (side->leaf != NULL) {
            WT_RET_NOTFOUND_OK(ret = __btree_diff_side_find_next_in_leaf(diff, side));
            if (ret == 0)
                return (0);

            WT_ASSERT(session, side->depth > 0);
            ref = side->leaf;
            side->leaf = NULL;
            ++side->path[side->depth - 1].slot;
            WT_RET(__wt_page_release(session, ref, 0));
            continue;
        }

        /* At the end. */
        if (side->depth == 0)
            return (0);

        /* Otherwise the side is on an internal page, the last frame on the path. */
        frame = &side->path[side->depth - 1];
        WT_INTL_INDEX_GET(session, frame->ref->page, pindex);
        WT_ASSERT(session, frame->slot <= pindex->entries);

        /* An exhausted internal page: pop it and continue in the parent. */
        if (frame->slot == pindex->entries) {
            /* The exhausted root ends the side; the data handle pins it. */
            WT_ASSERT(session, side->depth > 0);
            ref = frame->ref;
            if (--side->depth == 0)
                return (0);

            ++side->path[side->depth - 1].slot;
            WT_RET(__wt_page_release(session, ref, 0));
            continue;
        }

        /* Get the next child's key. The first child inherits the page's key. */
        child = pindex->index[frame->slot];
        if (frame->slot == 0)
            side->subtree_key = frame->key;
        else
            __wt_ref_key(frame->ref->page, child, &side->subtree_key.data, &side->subtree_key.size);

        /* End the side at the upper bound. */
        if (diff->upper_bound != NULL) {
            WT_RET(__btree_diff_ref_key_cmp(diff, &side->subtree_key, diff->upper_bound, &cmp));
            if (cmp >= 0)
                return (__btree_diff_side_release(session, side));
        }

        /*
         * Fail on a prepared update or prepared truncate. The aggregated time window covers the
         * whole subtree, so this also covers subtrees the walk skips.
         */
        if (!__wt_ref_addr_copy(session, child, &side->subtree_addr))
            side->subtree_addr.size = 0;
        if (side->subtree_addr.ta.prepare ||
          (side->subtree_addr.del_set &&
            (side->subtree_addr.del.prepare_state == WT_PREPARE_INPROGRESS ||
              side->subtree_addr.del.prepare_state == WT_PREPARE_LOCKED)))
            return (__btree_diff_fail_prepared(session));

        /*
         * Skip empty subtrees: one with no address is the placeholder leaf of an empty tree, and
         * precise checkpoints write a fast-truncated subtree only once the truncate is stable.
         */
        if (side->subtree_addr.size == 0 || side->subtree_addr.del_set) {
            ++frame->slot;
            continue;
        }

        side->subtree = child;
        return (0);
    }
}

/*
 * __btree_diff_side_descend_worker --
 *     Read a side's next subtree and move to its first leaf entry or unread subtree.
 */
static int
__btree_diff_side_descend_worker(WT_BTREE_DIFF *diff, WT_BTREE_DIFF_SIDE *side)
{
    WT_BTREE_DIFF_FRAME *frame;
    WT_REF *ref;
    WT_SESSION_IMPL *session;
    int cmp;
    bool search_lower_bound;

    session = diff->session;
    ref = side->subtree;

    /* Search for the lower bound only in the subtree that holds it. */
    search_lower_bound = false;
    if (diff->lower_bound != NULL) {
        WT_RET(__btree_diff_ref_key_cmp(diff, &side->subtree_key, diff->lower_bound, &cmp));
        search_lower_bound = cmp < 0;
    }

    /* Grow the path before pinning the page, so a failure cannot leak the pin. */
    if (F_ISSET(ref, WT_REF_FLAG_INTERNAL))
        WT_RET(__wt_realloc_def(session, &side->path_allocated, side->depth + 1, &side->path));
    WT_RET(__wt_page_in(session, ref, 0));
    ++diff->pages_read;

    if (F_ISSET(ref, WT_REF_FLAG_INTERNAL)) {
        /* Push an internal page onto the path. */
        frame = &side->path[side->depth++];
        frame->ref = ref;
        frame->key = side->subtree_key;
        frame->slot = 0;
        if (search_lower_bound)
            WT_RET(__btree_diff_intl_search_lower_bound(diff, ref->page, &frame->slot));
    } else {
        /*
         * Make a leaf the current page. Values are read from the page's cells only, so check that
         * it has no updates: checkpoint pages are read-only, and prepared and fast-truncated
         * subtrees, whose reads would create updates, are never read.
         */
        WT_ASSERT(session,
          ref->page->type == WT_PAGE_ROW_LEAF &&
            (ref->page->modify == NULL || ref->page->modify->mod_row_update == NULL));
        side->leaf = ref;
        side->leaf_slot = 0;
        if (search_lower_bound)
            WT_RET(__btree_diff_leaf_search_lower_bound(diff, side));
    }

    return (__btree_diff_side_find_next(diff, side));
}

/*
 * __btree_diff_side_advance --
 *     Move a side past its current leaf entry or subtree.
 */
static int
__btree_diff_side_advance(WT_BTREE_DIFF *diff, WT_BTREE_DIFF_SIDE *side)
{
    WT_DECL_RET;

    if (side->leaf != NULL)
        ++side->leaf_slot;
    else {
        WT_ASSERT(diff->session, side->subtree != NULL && side->depth > 0);
        ++side->path[side->depth - 1].slot;
    }

    WT_WITH_DHANDLE(diff->session, side->dhandle, ret = __btree_diff_side_find_next(diff, side));
    return (ret);
}

/*
 * __btree_diff_side_descend --
 *     Read a side's next subtree under its data handle.
 */
static int
__btree_diff_side_descend(WT_BTREE_DIFF *diff, WT_BTREE_DIFF_SIDE *side)
{
    WT_DECL_RET;

    WT_WITH_DHANDLE(
      diff->session, side->dhandle, ret = __btree_diff_side_descend_worker(diff, side));
    return (ret);
}

/*
 * __btree_diff_must_descend --
 *     Return whether a side must read its next subtree before the other side's entry can be
 *     returned.
 */
static int
__btree_diff_must_descend(
  WT_BTREE_DIFF *diff, WT_BTREE_DIFF_SIDE *side, WT_BTREE_DIFF_SIDE *other, bool *descendp)
{
    int cmp;

    *descendp = false;

    /* The side is at an entry or at its end: there is nothing to read. */
    if (side->subtree == NULL)
        return (0);

    /* The other side is at its end: only reading the subtree reaches the remaining keys. */
    if (other->leaf == NULL) {
        *descendp = true;
        return (0);
    }

    /*
     * This side is at an unread subtree, and the other side is at a leaf entry with key K. If this
     * side's subtree starts at or before K, it may hold K or a smaller key, so this side must read
     * it first.
     *
     * Otherwise every key in this side's subtree is greater than K: K is not on this side, and this
     * side waits at the subtree until the other side catches up, which may let the two sides skip
     * identical subtrees.
     */
    WT_RET(__btree_diff_ref_key_cmp(diff, &side->subtree_key, &other->key, &cmp));
    *descendp = cmp <= 0;
    return (0);
}

/*
 * __btree_diff_return --
 *     Fill in a difference from the sides' current entries.
 */
static void
__btree_diff_return(WT_BTREE_DIFF *diff, WT_BTREE_DIFF_TYPE type, WT_BTREE_DIFF_ENTRY *entry)
{
    WT_BTREE_DIFF_SIDE *new_side, *old_side;

    old_side = &diff->old_side;
    new_side = &diff->new_side;

    entry->type = type;
    entry->key = type == WT_BTREE_DIFF_DELETED ? &old_side->key : &new_side->key;
    entry->old_value = type == WT_BTREE_DIFF_ADDED ? NULL : &old_side->value;
    entry->new_value = type == WT_BTREE_DIFF_DELETED ? NULL : &new_side->value;

    /* Advance past the returned entries on the next call. */
    old_side->returned = type != WT_BTREE_DIFF_ADDED;
    new_side->returned = type != WT_BTREE_DIFF_DELETED;
}

/*
 * __btree_diff_next --
 *     Return the next difference, WT_NOTFOUND if there is none.
 */
static int
__btree_diff_next(WT_BTREE_DIFF *diff, WT_BTREE_DIFF_ENTRY *entry)
{
    WT_BTREE_DIFF_SIDE *new_side, *old_side;
    int cmp;
    bool descend;

    old_side = &diff->old_side;
    new_side = &diff->new_side;

    /* Move past the previous difference, whose items were valid until now. */
    if (old_side->returned) {
        old_side->returned = false;
        WT_RET(__btree_diff_side_advance(diff, old_side));
    }
    if (new_side->returned) {
        new_side->returned = false;
        WT_RET(__btree_diff_side_advance(diff, new_side));
    }

    for (;;) {
        /*
         * Both sides are at an unread subtree. Keys are consumed in order on both sides, so every
         * key before either subtree has been compared, and the subtrees can be compared whole.
         */
        if (old_side->subtree != NULL && new_side->subtree != NULL) {
            /* Skip identical subtrees. */
            if (old_side->subtree_addr.size == new_side->subtree_addr.size &&
              memcmp(old_side->subtree_addr.addr, new_side->subtree_addr.addr,
                old_side->subtree_addr.size) == 0) {
                ++diff->identical_skips;
                WT_RET(__btree_diff_side_advance(diff, old_side));
                WT_RET(__btree_diff_side_advance(diff, new_side));
                continue;
            }

            /* Read the subtree that starts first, or both if they start together. */
            WT_RET(
              __btree_diff_ref_key_cmp(diff, &old_side->subtree_key, &new_side->subtree_key, &cmp));
            if (cmp <= 0)
                WT_RET(__btree_diff_side_descend(diff, old_side));
            if (cmp >= 0)
                WT_RET(__btree_diff_side_descend(diff, new_side));
            continue;
        }

        /* One side at a subtree: read it if it may hold the other side's key. */
        WT_RET(__btree_diff_must_descend(diff, old_side, new_side, &descend));
        if (descend) {
            WT_RET(__btree_diff_side_descend(diff, old_side));
            continue;
        }
        WT_RET(__btree_diff_must_descend(diff, new_side, old_side, &descend));
        if (descend) {
            WT_RET(__btree_diff_side_descend(diff, new_side));
            continue;
        }

        /* Both sides at their end: done. */
        if (old_side->leaf == NULL && new_side->leaf == NULL)
            return (WT_NOTFOUND);

        /* Only one side at an entry: its key is not on the other side. */
        if (old_side->leaf == NULL) {
            __btree_diff_return(diff, WT_BTREE_DIFF_ADDED, entry);
            return (0);
        }
        if (new_side->leaf == NULL) {
            __btree_diff_return(diff, WT_BTREE_DIFF_DELETED, entry);
            return (0);
        }

        /* Both sides at an entry: the smaller key is on its side only. */
        WT_RET(__wt_compare(diff->session, diff->collator, &old_side->key, &new_side->key, &cmp));
        if (cmp != 0) {
            __btree_diff_return(diff, cmp < 0 ? WT_BTREE_DIFF_DELETED : WT_BTREE_DIFF_ADDED, entry);
            return (0);
        }

        /* The same key: return it if the values differ. */
        if (old_side->value.size != new_side->value.size ||
          (old_side->value.size != 0 &&
            memcmp(old_side->value.data, new_side->value.data, old_side->value.size) != 0)) {
            __btree_diff_return(diff, WT_BTREE_DIFF_MODIFIED, entry);
            return (0);
        }

        WT_RET(__btree_diff_side_advance(diff, old_side));
        WT_RET(__btree_diff_side_advance(diff, new_side));
    }
}

/*
 * __btree_diff_side_open --
 *     Position a side at the first leaf entry or unread subtree of its checkpoint.
 */
static int
__btree_diff_side_open(WT_BTREE_DIFF *diff, WT_BTREE_DIFF_SIDE *side, WT_DATA_HANDLE *dhandle)
{
    WT_BTREE *btree;
    WT_BTREE_DIFF_FRAME *frame;
    WT_DECL_RET;

    btree = dhandle->handle;
    side->dhandle = dhandle;

    /* Start the path at the root. */
    WT_RET(__wt_realloc_def(diff->session, &side->path_allocated, 1, &side->path));
    frame = &side->path[0];
    frame->ref = &btree->root;
    WT_CLEAR(frame->key);
    frame->slot = 0;
    side->depth = 1;

    if (diff->lower_bound != NULL)
        WT_RET(__btree_diff_intl_search_lower_bound(diff, btree->root.page, &frame->slot));

    WT_WITH_DHANDLE(diff->session, dhandle, ret = __btree_diff_side_find_next(diff, side));
    return (ret);
}

/*
 * __btree_diff_open_sides --
 *     Position both sides of a diff.
 */
static int
__btree_diff_open_sides(
  WT_BTREE_DIFF *diff, WT_DATA_HANDLE *old_dhandle, WT_DATA_HANDLE *new_dhandle)
{
    WT_RET(__btree_diff_side_open(diff, &diff->old_side, old_dhandle));
    return (__btree_diff_side_open(diff, &diff->new_side, new_dhandle));
}

/*
 * __btree_diff_check_dhandle --
 *     Check that a data handle is a row-store btree checkpoint, and return the btree's name in a
 *     buffer the caller frees.
 */
static int
__btree_diff_check_dhandle(
  WT_SESSION_IMPL *session, WT_DATA_HANDLE *dhandle, const char **namep, WT_ITEM **name_bufp)
{
    const char *checkpoint;

    /* Split off the checkpoint, which a disaggregated stable handle has in its name. */
    *namep = dhandle->name;
    checkpoint = dhandle->checkpoint;
    WT_RET(__wt_btree_shared_base_name(session, namep, &checkpoint, name_bufp));

    if (!WT_DHANDLE_BTREE(dhandle) || checkpoint == NULL)
        WT_RET_MSG(session, EINVAL, "btree diff: %s is not a btree checkpoint", dhandle->name);
    if (((WT_BTREE *)dhandle->handle)->type != BTREE_ROW)
        WT_RET_MSG(session, ENOTSUP, "btree diff: %s is not a row-store", dhandle->name);

    return (0);
}

/*
 * __btree_diff_check_dhandles --
 *     Check that two data handles are checkpoints of the same row-store btree.
 */
static int
__btree_diff_check_dhandles(
  WT_SESSION_IMPL *session, WT_DATA_HANDLE *old_dhandle, WT_DATA_HANDLE *new_dhandle)
{
    WT_DECL_ITEM(new_name_buf);
    WT_DECL_ITEM(old_name_buf);
    WT_DECL_RET;
    const char *new_name, *old_name;

    WT_ERR(__btree_diff_check_dhandle(session, old_dhandle, &old_name, &old_name_buf));
    WT_ERR(__btree_diff_check_dhandle(session, new_dhandle, &new_name, &new_name_buf));

    if (strcmp(old_name, new_name) != 0)
        WT_ERR_MSG(session, EINVAL, "btree diff: %s and %s are different btrees", old_dhandle->name,
          new_dhandle->name);

err:
    __wt_scr_free(session, &old_name_buf);
    __wt_scr_free(session, &new_name_buf);
    return (ret);
}

/*
 * __wt_btree_diff_open --
 *     Open a diff between two checkpoints of the same btree, optionally bounded to keys in [lower
 *     bound, upper bound). The caller keeps both data handles acquired until the diff is closed.
 */
int
__wt_btree_diff_open(WT_SESSION_IMPL *session, WT_DATA_HANDLE *old_dhandle,
  WT_DATA_HANDLE *new_dhandle, const WT_ITEM *lower_bound, const WT_ITEM *upper_bound,
  WT_BTREE_DIFF **diffp)
{
    WT_BTREE_DIFF *diff;
    WT_DECL_RET;

    *diffp = NULL;

    /* Validate the arguments. */
    if (!F_ISSET(S2C(session), WT_CONN_PRECISE_CHECKPOINT))
        WT_RET_MSG(session, ENOTSUP, "btree diff requires precise checkpoints");
    WT_RET(__btree_diff_check_dhandles(session, old_dhandle, new_dhandle));

    /* A missing subtree key has no data, so the bounds must not be empty. */
    if ((lower_bound != NULL && lower_bound->size == 0) ||
      (upper_bound != NULL && upper_bound->size == 0))
        WT_RET_MSG(session, EINVAL, "btree diff: empty keys are not permitted as bounds");

    /* Allocate the diff and copy the bounds. */
    WT_RET(__wt_calloc_one(session, &diff));
    diff->session = session;
    diff->collator = ((WT_BTREE *)old_dhandle->handle)->collator;
    if (lower_bound != NULL) {
        WT_ERR(__wt_buf_set(session, &diff->lower_bound_buf, lower_bound->data, lower_bound->size));
        diff->lower_bound = &diff->lower_bound_buf;
    }
    if (upper_bound != NULL) {
        WT_ERR(__wt_buf_set(session, &diff->upper_bound_buf, upper_bound->data, upper_bound->size));
        diff->upper_bound = &diff->upper_bound_buf;
    }

    /* Reading page indexes and addresses requires a split generation. */
    WT_WITH_PAGE_INDEX(session, ret = __btree_diff_open_sides(diff, old_dhandle, new_dhandle));
    WT_ERR(ret);

    *diffp = diff;
    return (0);

err:
    WT_TRET(__wt_btree_diff_close(&diff));
    return (ret);
}

/*
 * __wt_btree_diff_next --
 *     Return the next difference in key order, WT_NOTFOUND if there is none. After an error, the
 *     diff can only be closed.
 */
int
__wt_btree_diff_next(WT_BTREE_DIFF *diff, WT_BTREE_DIFF_ENTRY *entry)
{
    WT_DECL_RET;

    /* Reading page indexes and addresses requires a split generation. */
    WT_WITH_PAGE_INDEX(diff->session, ret = __btree_diff_next(diff, entry));
    return (ret);
}

/*
 * __btree_diff_side_close --
 *     Release a side's pages and memory, and its data handle if the diff owns it.
 */
static int
__btree_diff_side_close(WT_BTREE_DIFF *diff, WT_BTREE_DIFF_SIDE *side)
{
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    session = diff->session;

    if (side->dhandle != NULL) {
        WT_WITH_DHANDLE(session, side->dhandle, WT_TRET(__btree_diff_side_release(session, side)));
        if (diff->own_dhandles)
            WT_WITH_DHANDLE(session, side->dhandle, WT_TRET(__wt_session_release_dhandle(session)));
    }

    __wt_free(session, side->path);
    __wt_buf_free(session, &side->key);
    __wt_buf_free(session, &side->value);
    return (ret);
}

/*
 * __wt_btree_diff_close --
 *     Close a diff.
 */
int
__wt_btree_diff_close(WT_BTREE_DIFF **diffp)
{
    WT_BTREE_DIFF *diff;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    if ((diff = *diffp) == NULL)
        return (0);
    *diffp = NULL;
    session = diff->session;

    WT_TRET(__btree_diff_side_close(diff, &diff->old_side));
    WT_TRET(__btree_diff_side_close(diff, &diff->new_side));

    __wt_buf_free(session, &diff->lower_bound_buf);
    __wt_buf_free(session, &diff->upper_bound_buf);
    __wt_free(session, diff);
    return (ret);
}

/*
 * __wt_btree_diff_type_string --
 *     Return the name of a difference's type.
 */
const char *
__wt_btree_diff_type_string(WT_BTREE_DIFF_TYPE type)
{
    switch (type) {
    case WT_BTREE_DIFF_ADDED:
        return ("added");
    case WT_BTREE_DIFF_DELETED:
        return ("deleted");
    case WT_BTREE_DIFF_MODIFIED:
        return ("modified");
    }

    return ("unknown");
}

/*
 * __btree_diff_checkpoint_dhandle --
 *     Acquire the data handle of a named checkpoint.
 */
static int
__btree_diff_checkpoint_dhandle(
  WT_SESSION_IMPL *session, const char *uri, const char *checkpoint, WT_DATA_HANDLE **dhandlep)
{
    WT_DECL_RET;
    const char *last_name;

    *dhandlep = NULL;
    last_name = NULL;

    /* Resolve WiredTigerCheckpoint to the newest unnamed checkpoint. */
    if (strcmp(checkpoint, WT_CHECKPOINT) == 0) {
        WT_RET(__wt_meta_checkpoint_last_name(session, uri, &last_name, NULL, NULL));
        checkpoint = last_name;
    }

    /* Getting the handle sets the session's data handle, so restore it. */
    WT_SAVE_DHANDLE(session, {
        if ((ret = __wt_session_get_dhandle(session, uri, checkpoint, NULL, 0)) == 0)
            *dhandlep = session->dhandle;
    });

    __wt_free(session, last_name);
    return (ret);
}

/*
 * __wt_btree_diff_open_checkpoints --
 *     Open a diff between two named checkpoints of a btree. The diff owns the data handles.
 */
int
__wt_btree_diff_open_checkpoints(WT_SESSION_IMPL *session, const char *uri,
  const char *old_checkpoint, const char *new_checkpoint, const WT_ITEM *lower_bound,
  const WT_ITEM *upper_bound, WT_BTREE_DIFF **diffp)
{
    WT_DATA_HANDLE *new_dhandle, *old_dhandle;
    WT_DECL_RET;

    *diffp = NULL;
    new_dhandle = old_dhandle = NULL;

    WT_ERR(__btree_diff_checkpoint_dhandle(session, uri, old_checkpoint, &old_dhandle));
    WT_ERR(__btree_diff_checkpoint_dhandle(session, uri, new_checkpoint, &new_dhandle));

    WT_ERR(
      __wt_btree_diff_open(session, old_dhandle, new_dhandle, lower_bound, upper_bound, diffp));
    (*diffp)->own_dhandles = true;
    return (0);

err:
    if (old_dhandle != NULL)
        WT_WITH_DHANDLE(session, old_dhandle, WT_TRET(__wt_session_release_dhandle(session)));
    if (new_dhandle != NULL)
        WT_WITH_DHANDLE(session, new_dhandle, WT_TRET(__wt_session_release_dhandle(session)));
    return (ret);
}
