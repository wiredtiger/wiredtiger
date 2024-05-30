/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

/* This file defines __wt_cursor_index and related definitions. */

struct __wt_cursor_index {
    WT_CURSOR iface;

    WT_TABLE *table;
    WT_INDEX *index;
    const char *key_plan, *value_plan;

    WT_CURSOR *child;
    WT_CURSOR **cg_cursors;
    uint8_t *cg_needvalue;
};
