/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#define WT_STAT_NONE -1

/* Initialize the fields in a page stat structure to their defaults. */
#define WT_PAGE_STAT_INIT(ps)            \
    do {                                 \
        (ps)->user_bytes = WT_STAT_NONE; \
        (ps)->records = WT_STAT_NONE;    \
    } while (0)

/* Check if there is a valid file size byte count stored. */
#define WT_PAGE_STAT_HAS_USER_BYTES(ps) ((ps)->user_bytes != WT_STAT_NONE)

/* Check if there is a valid record count stored. */
#define WT_PAGE_STAT_HAS_RECORDS(ps) ((ps)->records != WT_STAT_NONE)

/* Copy the values from one time page stat structure to another. */
#define WT_PAGE_STAT_COPY(dest, source) (*(dest) = *(source))
