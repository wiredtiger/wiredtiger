/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_page_custom_alloc_row_leaf --
 *     Entry point for cool custom allocator.
 */
int
__wt_page_custom_alloc_row_leaf(WT_SESSION_IMPL *session, uint32_t entries, WT_PAGE **pagep)
{
    WT_PAGE *page;
    size_t size;

    size = sizeof(WT_PAGE) + (entries * sizeof(WT_ROW));

    WT_RET(__wt_calloc(session, 1, size, &page));

    page->pg_row = entries == 0 ? NULL : (WT_ROW *)((uint8_t *)page + sizeof(WT_PAGE));

    *pagep = page;

    return (0);
}

/*
 * __row_leaf_key_free --
 *     Words to make s all happy.
 */
static void
__row_leaf_key_free(WT_SESSION_IMPL *session, WT_PAGE *page, WT_ROW *rip)
{
    WT_IKEY *ikey;
    void *copy;

    /* The row-store key can change underfoot; explicitly take a copy. */
    copy = WT_ROW_KEY_COPY(rip);

    /*
     * If the key was a WT_IKEY allocation (that is, if it points somewhere other than the original
     * page), free the memory.
     */
    __wt_row_leaf_key_info(page, copy, &ikey, NULL, NULL, NULL, NULL);
    __wt_free(session, ikey);
}

/*
 * __wt_page_custom_free_row_leaf --
 *     Words to make s all happy.
 */
void
__wt_page_custom_free_row_leaf(WT_SESSION_IMPL *session, WT_PAGE *page)
{
    WT_PAGE_HEADER *dsk;
    WT_ROW *rip;
    uint32_t i;

    WT_ASSERT(session, page->type == WT_PAGE_ROW_LEAF);

    /* Free any allocated memory used by instantiated keys. */
    WT_ROW_FOREACH (page, rip, i)
        __row_leaf_key_free(session, page, rip);

    /* Discard any allocated disk image. */
    dsk = (WT_PAGE_HEADER *)page->dsk;
    if (F_ISSET_ATOMIC_16(page, WT_PAGE_DISK_ALLOC))
        /* TODO - not clear if we'll override disk image allocation. */
        __wt_overwrite_and_free_len(session, dsk, dsk->mem_size);

    __wt_overwrite_and_free(session, page);
}

/*
 * __wt_upd_custom_alloc_row_leaf --
 *     Words to make s all happy.
 */
int
__wt_upd_custom_alloc_row_leaf(
  WT_SESSION_IMPL *session, WT_PAGE *page, size_t allocsz, WT_UPDATE **updp)
{
    WT_UNUSED(page);
    return (__wt_calloc(session, 1, allocsz, updp));
}
