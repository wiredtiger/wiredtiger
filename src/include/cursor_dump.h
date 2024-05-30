/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

/* This file defines __wt_cursor_dump and related definitions. */

struct __wt_cursor_dump {
    WT_CURSOR iface;

    WT_CURSOR *child;
};
