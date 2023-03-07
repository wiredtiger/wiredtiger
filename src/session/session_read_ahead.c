/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_session_readahead_check --
 *     Check to see whether cursors owned by this session might benefit from doing read ahead
 */
bool
__wt_session_readahead_check(WT_SESSION_IMPL *session, WT_REF *ref)
{
    /* A single read from disk is common - don't use it to guide readahead behavior. */
    if (session->readahead_disk_read_count < 2)
        return (false);

    WT_ASSERT_ALWAYS(session, session->readahead_prev_ref == NULL ||
            session->readahead_prev_ref->state == WT_REF_MEM,
            "Any ref being used for read-ahead better already be in cache.");

    WT_ASSERT_ALWAYS(session, session->readahead_prev_ref == NULL ||
            F_ISSET(session->readahead_prev_ref, WT_REF_FLAG_INTERNAL),
            "Any ref being used for read-ahead better reference an internal page");

    /*
     * If the previous read ahead was using the same home ref, it's already been preloaded. Note
     * that this heuristic probably needs to get more sophisticated - ideally it would preload a
     * number of pages, not necessarily all children of the current internal page.
     */
    if (session->readahead_prev_ref == NULL || session->readahead_prev_ref->page == ref->home)
        return (false);

    return (false);
}
