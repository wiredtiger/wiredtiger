/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

/* This file defines __wt_cursor_table and related definitions. */

struct __wt_cursor_table {
    WT_CURSOR iface;

    WT_TABLE *table;
    const char *plan;

    const char **cfg; /* Saved configuration string */

    WT_CURSOR **cg_cursors;
    WT_ITEM *cg_valcopy; /*
                          * Copies of column group values, for
                          * overlapping set_value calls.
                          */
    WT_CURSOR **idx_cursors;
};

#define WT_CURSOR_PRIMARY(cursor) (((WT_CURSOR_TABLE *)(cursor))->cg_cursors[0])
