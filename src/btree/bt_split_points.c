/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_split_points_child_for_key --
 *     Return the index of the child whose subtree contains 'key'. A row-store internal page's 0th
 *     entry is a reconciliation sentinel that sorts below every real key, so child 0 holds anything
 *     below the 1st entry's key; the first child whose boundary key is greater than 'key' is not
 *     the containing child. Returns an index in [0, entries).
 */
static uint32_t
__wt_split_points_child_for_key(WT_SESSION_IMPL *session, WT_COLLATOR *collator, WT_PAGE *page,
  WT_PAGE_INDEX *pindex, WT_ITEM *key)
{
    WT_ITEM k;
    size_t keysz;
    uint32_t i, last;
    int cmpp;
    void *keyp;

    last = 0;
    for (i = 1; i < pindex->entries; ++i) {
        __wt_ref_key(page, pindex->index[i], &keyp, &keysz);
        k.data = keyp;
        k.size = keysz;
        WT_IGNORE_RET(__wt_compare(session, collator, &k, key, &cmpp));
        if (cmpp <= 0)
            last = i;
        else
            break;
    }
    return (last);
}

/*
 * __wt_split_points_realloc --
 *     Grow an array to at least n elements. __wt_realloc asserts it is growing, so only call it
 *     when the requested size exceeds the current allocation.
 */
static int
__wt_split_points_realloc(
  WT_SESSION_IMPL *session, size_t *allocp, size_t elemsz, uint32_t n, void **pp)
{
    size_t needed;

    needed = (size_t)n * elemsz;
    if (*allocp >= needed)
        return (0);
    return (__wt_realloc(session, allocp, needed, pp));
}

/*
 * __wt_split_points_gather --
 *     Walk down the B-tree internal pages covering the cursor's range, gathering the in-range
 *     boundary entries (the first key of each covered child page, which lives in the parent's
 *     on-page cell) in key order. Descend into overlapping internal children only when the current
 *     level does not offer enough in-range boundaries.
 *
 * This never reads a leaf page: an internal page's children are flagged INTERNAL or LEAF at
 *     page-read time (see __inmem_row_int), so a child's type is known without reading it, and we
 *     descend only into children already known to be internal. The in-range entries are filtered by
 *     the range (strictly after the leading key, and within the upper bound) before any sampling.
 *
 * On success, the caller's boundary out-array holds the in-range boundary WT_REFs in key order,
 *     each a valid boundary key raised from its parent cell; the caller holds a hazard pointer on
 *     every page in the page out-array and must release them.
 *
 * Returns WT_RESTART if a page split invalidated the walk; on WT_RESTART, and on any other error,
 *     it has released every hazard it held, so the caller may simply retry.
 */
static int
__wt_split_points_gather(WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt, WT_ITEM *leading,
  uint32_t needed, WT_ITEM **out_keys, uint32_t *nkeysp, WT_REF ***pagesp, uint32_t *npagesp)
{
    WT_BTREE *btree;
    WT_COLLATOR *collator;
    WT_CURSOR *cursor;
    WT_DECL_RET;
    WT_ITEM key;
    WT_ITEM *keys;
    WT_PAGE_INDEX *pindex;
    WT_REF **cur_refs, **next_refs, **refs;
    size_t cur_alloc, next_alloc, refs_alloc, keys_alloc;
    uint32_t entries, i, j, n_cur_refs, n_next_refs, nrefs, slot;

    btree = S2BT(session);
    cursor = &cbt->iface;
    collator = CUR2BT(cursor)->collator;

    cur_refs = next_refs = refs = NULL;
    keys = NULL;
    cur_alloc = next_alloc = refs_alloc = keys_alloc = 0;
    n_cur_refs = n_next_refs = nrefs = 0;
    *out_keys = NULL;
    *nkeysp = 0;
    *pagesp = NULL;
    *npagesp = 0;

    /* Current-level internal pages whose subtrees cover the range: start at the root. */
    WT_ERR(__wt_split_points_realloc(session, &cur_alloc, sizeof(WT_REF *), 1, (void **)&cur_refs));
    cur_refs[0] = &btree->root;
    n_cur_refs = 1;

    for (;;) {
        /*
         * Gather in-range boundary entries at the current level, in key order. Each boundary is the
         * first key of a covered child page, raised from the parent's on-page cell and copied out
         * so it stays valid after the page is released. A boundary is eligible only if it is
         * strictly after the leading key and inside the range.
         */
        nrefs = 0;
        WT_ERR(
          __wt_split_points_realloc(session, &refs_alloc, sizeof(WT_REF *), 1, (void **)&refs));
        WT_ERR(__wt_split_points_realloc(session, &keys_alloc, sizeof(WT_ITEM), 1, (void **)&keys));
        for (i = 0; i < n_cur_refs; ++i) {
            WT_PAGE *page = cur_refs[i]->page;
            if (!WT_PAGE_IS_INTERNAL(page))
                continue;
            WT_INTL_INDEX_GET(session, page, pindex);
            entries = pindex->entries;
            /*
             * Skip the 0th entry: on a row-store internal page it is a reconciliation sentinel that
             * sorts below every real key, not a usable boundary.
             */
            for (j = 1; j < entries; ++j) {
                WT_REF *ref = pindex->index[j];
                void *keyp;
                size_t keysz;
                int cmpp;
                bool out;

                __wt_ref_key(page, ref, &keyp, &keysz);
                key.data = keyp;
                key.size = keysz;

                WT_ERR(__wt_compare(session, collator, &key, leading, &cmpp));
                if (cmpp <= 0)
                    continue; /* at or before the leading key */
                if (F_ISSET(cursor, WT_CURSTD_BOUND_UPPER)) {
                    WT_ERR(__wt_compare_bounds(session, cursor, &key, WT_RECNO_OOB, true, &out));
                    if (out)
                        continue; /* at or beyond the upper bound */
                }

                WT_ERR(__wt_split_points_realloc(
                  session, &refs_alloc, sizeof(WT_REF *), nrefs + 1, (void **)&refs));
                WT_ERR(__wt_split_points_realloc(
                  session, &keys_alloc, sizeof(WT_ITEM), nrefs + 1, (void **)&keys));
                WT_ERR(__wt_buf_set(session, &keys[nrefs], keyp, keysz));
                refs[nrefs++] = ref;
            }
        }

        if (nrefs >= needed)
            break;

        /*
         * Not enough boundaries at this level: descend one level into the overlapping internal
         * children, in key order, for finer boundaries. The leading-containing child is included
         * even when its own boundary key is not in range (the leftmost child's 0th entry is a
         * sentinel below every key, so a range that begins there has no in-range boundary yet).
         */
        n_next_refs = 0;
        WT_ERR(__wt_split_points_realloc(
          session, &next_alloc, sizeof(WT_REF *), 1, (void **)&next_refs));

        /* First the leading-containing child of each frontier page, in key order. */
        for (i = 0; i < n_cur_refs; ++i) {
            WT_PAGE *page = cur_refs[i]->page;
            WT_REF *ref;

            if (!WT_PAGE_IS_INTERNAL(page))
                continue;
            WT_INTL_INDEX_GET(session, page, pindex);
            slot = __wt_split_points_child_for_key(session, collator, page, pindex, leading);
            ref = pindex->index[slot];
            if (WT_REF_GET_STATE(ref) != WT_REF_DELETED && F_ISSET(ref, WT_REF_FLAG_INTERNAL)) {
                WT_ERR(__wt_page_in(session, ref, WT_READ_RESTART_OK));
                WT_ERR(__wt_split_points_realloc(
                  session, &next_alloc, sizeof(WT_REF *), n_next_refs + 1, (void **)&next_refs));
                next_refs[n_next_refs++] = ref;
            }
        }

        /* Then the in-range internal children, skipping any already added above. */
        for (i = 0; i < nrefs; ++i) {
            WT_REF *ref = refs[i];
            bool already;

            if (WT_REF_GET_STATE(ref) == WT_REF_DELETED || !F_ISSET(ref, WT_REF_FLAG_INTERNAL))
                continue;
            already = false;
            for (j = 0; j < n_next_refs; ++j)
                if (next_refs[j] == ref) {
                    already = true;
                    break;
                }
            if (already)
                continue;
            WT_ERR(__wt_page_in(session, ref, WT_READ_RESTART_OK));
            WT_ERR(__wt_split_points_realloc(
              session, &next_alloc, sizeof(WT_REF *), n_next_refs + 1, (void **)&next_refs));
            next_refs[n_next_refs++] = ref;
        }

        if (n_next_refs == 0)
            break; /* deepest internal level: children are leaves, nothing finer to sample */

        /* Release the previous level's hazards, then swap the frontier down one level. */
        for (i = 0; i < n_cur_refs; ++i)
            WT_ERR(__wt_page_release(session, cur_refs[i], 0));
        __wt_free(session, cur_refs);
        cur_refs = next_refs;
        next_refs = NULL;
        n_cur_refs = n_next_refs;
        cur_alloc = next_alloc;
        next_alloc = 0;
        __wt_free(session, refs);
        refs = NULL;
        refs_alloc = 0;
        for (i = 0; i < nrefs; ++i)
            __wt_buf_free(session, &keys[i]);
        __wt_free(session, keys);
        keys = NULL;
        keys_alloc = 0;
    }

    /* Success: hand the copied boundary keys and the pages the caller must release. */
    *out_keys = keys;
    *nkeysp = nrefs;
    *pagesp = cur_refs;
    *npagesp = n_cur_refs;
    keys = NULL;
    cur_refs = NULL;
    return (0);

err:
    /* On any error (including WT_RESTART), release every hazard we hold, then re-throw. */
    if (cur_refs != NULL)
        for (i = 0; i < n_cur_refs; ++i)
            WT_TRET(__wt_page_release(session, cur_refs[i], 0));
    if (next_refs != NULL)
        for (i = 0; i < n_next_refs; ++i)
            WT_TRET(__wt_page_release(session, next_refs[i], 0));
    __wt_free(session, cur_refs);
    __wt_free(session, next_refs);
    __wt_free(session, refs);
    /* Free any copied boundary keys, and their element buffers. */
    if (keys != NULL) {
        for (i = 0; i < nrefs; ++i)
            __wt_buf_free(session, &keys[i]);
        __wt_free(session, keys);
    }
    return (ret);
}

/*
 * __wt_btree_split_points --
 *     Compute up to max_points keys that partition this cursor's range, and publish them on the
 *     btree cursor (split_points array, split_point_count, split_point_next). Returns 0 or errno.
 *
 * The caller supplies the leading key (the exact first key at or after the lower bound) in
 *     'leading', and returns without calling this for a range that is empty or needs no split. The
 *     remaining keys (up to max_points - 1 of them) are sampled from B-tree internal pages; no leaf
 *     page is read while sampling. 'leading' is emitted first, so the published count is at least
 *     one and at most max_points.
 *
 * WT_SPLIT_POINT_NO_IO is accepted but not yet honored. FIXME-WT-XXXX: honoring it (walk only
 *     resident pages, and never force I/O) belongs to the NO_IO task; until then it is accepted and
 *     ignored and the walk may page internal pages in from disk.
 */
int
__wt_btree_split_points(
  WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt, WT_ITEM *leading, int max_points, uint32_t flags)
{
    WT_BTREE *btree;
    WT_DECL_RET;
    WT_ITEM *keys, *sp;
    WT_REF **pages;
    uint32_t count, i, needed, nkeys, npages, stride;

    WT_UNUSED(flags); /* FIXME-WT-XXXX: NO_IO is deferred to its own task. */

    btree = S2BT(session);

    count = 0;
    keys = NULL;
    pages = NULL;
    nkeys = npages = 0;

    if (btree->type != BTREE_ROW)
        WT_RET_MSG(session, ENOTSUP, "split points not supported by this cursor type");

    needed = (uint32_t)(max_points - 1);

retry:
    WT_WITH_PAGE_INDEX(session,
      ret =
        __wt_split_points_gather(session, cbt, leading, needed, &keys, &nkeys, &pages, &npages));
    if (ret == WT_RESTART)
        goto retry;
    WT_ERR(ret);

    /* Sample the boundaries: the leading key, then evenly spaced in-range boundaries. */
    if (nkeys >= needed) {
        count = (uint32_t)max_points;
        stride = nkeys / needed;
    } else {
        count = nkeys + 1;
        stride = 0;
    }
    WT_ERR(__wt_calloc(session, count, sizeof(WT_ITEM), &cbt->split_points));
    sp = cbt->split_points;
    WT_ERR(__wt_buf_set(session, &sp[0], leading->data, leading->size));
    for (i = 0; i < count - 1; ++i) {
        uint32_t idx = (stride == 0) ? i : i * stride;
        WT_ERR(__wt_buf_set(session, &sp[i + 1], keys[idx].data, keys[idx].size));
    }

    /* Discard the sampled boundaries before reporting success. */
    for (i = 0; i < npages; ++i)
        WT_IGNORE_RET(__wt_page_release(session, pages[i], 0));
    for (i = 0; i < nkeys; ++i)
        __wt_buf_free(session, &keys[i]);
    __wt_free(session, keys);
    __wt_free(session, pages);
    keys = NULL;
    pages = NULL;

    cbt->split_point_count = count;
    cbt->split_point_next = 0;
    return (0);

err:
    if (pages != NULL)
        for (i = 0; i < npages; ++i)
            WT_TRET(__wt_page_release(session, pages[i], 0));
    if (keys != NULL) {
        for (i = 0; i < nkeys; ++i)
            __wt_buf_free(session, &keys[i]);
        __wt_free(session, keys);
    }
    if (pages != NULL)
        __wt_free(session, pages);
    if (cbt->split_points != NULL) {
        for (i = 0; i < count; ++i)
            __wt_buf_free(session, &cbt->split_points[i]);
        __wt_free(session, cbt->split_points);
        cbt->split_points = NULL;
    }
    cbt->split_point_count = 0;
    cbt->split_point_next = 0;
    return (ret);
}
