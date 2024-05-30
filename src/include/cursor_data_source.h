/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

/* This file defines __wt_cursor_data_source and related definitions. */

struct __wt_cursor_data_source {
    WT_CURSOR iface;

    WT_COLLATOR *collator; /* Configured collator */
    int collator_owned;    /* Collator needs to be terminated */

    WT_CURSOR *source; /* Application-owned cursor */
};
