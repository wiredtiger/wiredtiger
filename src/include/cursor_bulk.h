/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

#include "cursor_btree.h"

/* This file defines __wt_cursor_bulk and related definitions. */

struct __wt_cursor_bulk {
    WT_CURSOR_BTREE cbt;

    /*
     * Variable-length column store compares values during bulk load as part of RLE compression,
     * row-store compares keys during bulk load to avoid corruption.
     */
    bool first_insert; /* First insert */
    WT_ITEM *last;     /* Last key/value inserted */

    /*
     * Additional column-store bulk load support.
     */
    uint64_t recno; /* Record number */
    uint64_t rle;   /* Variable-length RLE counter */

    /*
     * Additional fixed-length column store bitmap bulk load support: current entry in memory chunk
     * count, and the maximum number of records per chunk.
     */
    bool bitmap;    /* Bitmap bulk load */
    uint32_t entry; /* Entry count */
    uint32_t nrecs; /* Max records per chunk */

    void *reconcile; /* Reconciliation support */
    WT_REF *ref;     /* The leaf page */
    WT_PAGE *leaf;
};
