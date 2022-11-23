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

/* Check if there is a valid file size byte count stored. */
#define WT_PAGE_STAT_HAS_BYTE_COUNT(ps) ((ps)->byte_count != WT_STAT_NONE)

/* Check if there is a valid row count stored. */
#define WT_PAGE_STAT_HAS_ROW_COUNT(ps) ((ps)->row_count != WT_STAT_NONE)

/* Copy the values from one time page stat structure to another. */
#define WT_PAGE_STAT_COPY(dest, source) (*(dest) = *(source))

/* Merge a page stat structure into another - summing the value from each. */
#define WT_PAGE_STAT_MERGE(ps, merge)                                                \
    do {                                                                             \
        /* Merge byte count */                                                       \
        if ((ps)->byte_count == WT_STAT_NONE || (merge)->byte_count == WT_STAT_NONE) \
            (ps)->byte_count = WT_STAT_NONE;                                         \
        else                                                                         \
            (ps)->byte_count += (merge)->byte_count;                                 \
        /* Merge row count */                                                        \
        if ((ps)->row_count == WT_STAT_NONE || (merge)->row_count == WT_STAT_NONE)   \
            (ps)->row_count = WT_STAT_NONE;                                          \
        else                                                                         \
            (ps)->row_count += (merge)->row_count;                                   \
    } while (0)

/* Update the page stat structure */
#define WT_PAGE_STAT_UPDATE(ps, update)                                                    \
    do {                                                                                   \
        /* Update byte count */                                                            \
        if ((ps)->byte_count == WT_STAT_NONE && (update)->byte_count != WT_STAT_NONE)      \
            (ps)->byte_count = (update)->byte_count;                                       \
        else if ((ps)->byte_count != WT_STAT_NONE && (update)->byte_count != WT_STAT_NONE) \
            (ps)->byte_count += (update)->byte_count;                                      \
        /* Update row count */                                                             \
        if ((ps)->row_count == WT_STAT_NONE && (update)->row_count != WT_STAT_NONE)        \
            (ps)->row_count = (update)->row_count;                                         \
        else if ((ps)->row_count != WT_STAT_NONE && (update)->row_count != WT_STAT_NONE)   \
            (ps)->row_count += (update)->row_count;                                        \
    } while (0)
