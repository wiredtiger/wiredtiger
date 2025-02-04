/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

struct __wt_bitmap {
    uint8_t *internal; /* The map itself. */
    size_t size;       /* The number if bits in the map. */
};
