/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

/* This file defines __wt_cursor_bounds_state and related definitions. */

struct __wt_cursor_bounds_state {
    WT_ITEM *lower_bound;
    WT_ITEM *upper_bound;
    uint64_t bound_flags;
};
