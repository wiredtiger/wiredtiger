/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/* Initialize the fields in a page stat structure to their defaults. */
#define WT_PAGE_STAT_INIT(ps) \
    do {                      \
        (ps)->user_bytes = 0; \
        (ps)->records = 0;    \
    } while (0)

/* Copy the values from one time page stat structure to another. */
#define WT_PAGE_STAT_COPY(dest, source) (*(dest) = *(source))
