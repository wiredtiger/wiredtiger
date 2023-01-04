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
        (ps)->byte_count = WT_STAT_NONE; \
        (ps)->row_count = WT_STAT_NONE;  \
    } while (0)

/* Initialize the fields in a page stat structure to 0 for aggregation. */
#define WT_PAGE_STAT_INIT_AGGREGATE(ps) \
    do {                                \
        (ps)->byte_count = 0;           \
        (ps)->row_count = 0;            \
    } while (0)

/* Check if there is a valid file size byte count stored. */
#define WT_PAGE_STAT_HAS_BYTE_COUNT(ps) ((ps)->byte_count != WT_STAT_NONE)

/* Check if there is a valid row count stored. */
#define WT_PAGE_STAT_HAS_ROW_COUNT(ps) ((ps)->row_count != WT_STAT_NONE)

/* Copy the values from one time page stat structure to another. */
#define WT_PAGE_STAT_COPY(dest, source) (*(dest) = *(source))

/*
 * Update the page stat structure. For both the byte and row counts, we want to reset the page stat
 * value for the page if any of its children that were merged in had invalid stat values.
 */
#define WT_PAGE_STAT_ADD(ps, update)                                                  \
    do {                                                                              \
        /* Update byte count */                                                       \
        if ((ps)->byte_count == WT_STAT_NONE || (update)->byte_count == WT_STAT_NONE) \
            (ps)->byte_count = WT_STAT_NONE;                                          \
        else                                                                          \
            (ps)->byte_count += (update)->byte_count;                                 \
        /* Update row count */                                                        \
        if ((ps)->row_count == WT_STAT_NONE || (update)->row_count == WT_STAT_NONE)   \
            (ps)->row_count = WT_STAT_NONE;                                           \
        else                                                                          \
            (ps)->row_count += (update)->row_count;                                   \
    } while (0)
