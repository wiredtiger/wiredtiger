/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_session_prefetch_check --
 *     Check if pre-fetching is configured on the session.
 */
bool
__wt_session_prefetch_check(WT_SESSION_IMPL *session, WT_REF *ref)
{
    if (S2C(session)->prefetch_queue_count > WT_MAX_PREFETCH_QUEUE)
        return (false);
    /*
     * Check if pre-fetching is enabled on the session level. We don't perform pre-fetching on
     * internal threads or internal pages (finding the right content to preload based on internal
     * pages is hard), so check for that too. The result of this check will be used by cursor logic
     * to determine if pre-fetching will be performed.
     */
    if (!F_ISSET(session, WT_SESSION_PREFETCH) || F_ISSET(session, WT_SESSION_INTERNAL) ||
      F_ISSET(ref, WT_REF_FLAG_INTERNAL)) {
        WT_STAT_CONN_INCR(session, block_prefetch_skipped);
        return (false);
    }

    session->pf.prefetch_skipped_with_parent = 0;
    if (session->pf.prefetch_disk_read_count == 1)
        WT_STAT_CONN_INCR(session, block_prefetch_disk_one);

    WT_STAT_CONN_INCR(session, block_prefetch_attempts);
    return (true);
}
