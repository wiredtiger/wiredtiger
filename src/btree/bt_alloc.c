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
