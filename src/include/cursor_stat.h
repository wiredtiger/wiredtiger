/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

/* This file defines __wt_cursor_stat and __wt_join_stats_group related definitions. */

struct __wt_join_stats_group {
    const char *desc_prefix; /* Prefix appears before description */
    WT_CURSOR_JOIN *join_cursor;
    ssize_t join_cursor_entry; /* Position in entries */
    WT_JOIN_STATS join_stats;
};

struct __wt_cursor_stat {
    WT_CURSOR iface;

    bool notinitialized; /* Cursor not initialized */
    bool notpositioned;  /* Cursor not positioned */

    int64_t *stats;  /* Statistics */
    int stats_base;  /* Base statistics value */
    int stats_count; /* Count of statistics values */
    int (*stats_desc)(WT_CURSOR_STAT *, int, const char **);
    /* Statistics descriptions */
    int (*next_set)(WT_SESSION_IMPL *, WT_CURSOR_STAT *, bool, bool); /* Advance to next set */

    union { /* Copies of the statistics */
        WT_DSRC_STATS dsrc_stats;
        WT_CONNECTION_STATS conn_stats;
        WT_JOIN_STATS_GROUP join_stats_group;
        WT_SESSION_STATS session_stats;
    } u;

    const char **cfg; /* Original cursor configuration */
    char *desc_buf;   /* Saved description string */

    int key;    /* Current stats key */
    uint64_t v; /* Current stats value */
    WT_ITEM pv; /* Current stats value (string) */

    /* Options declared in flags.py, shared by WT_CONNECTION::stat_flags */
    uint32_t flags;
};

/*
 * WT_CURSOR_STATS --
 *	Return a reference to a statistic cursor's stats structures.
 */
#define WT_CURSOR_STATS(cursor) (((WT_CURSOR_STAT *)(cursor))->stats)
