/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_blkcache_open --
 *     Open a file.
 *
 * FIXME-WT-18876 This is the shared block manager's file open, not block cache code: the function
 *     moves to block_mgr.c.
 *
 * FIXME-WT-18875 The blkcache name is a leftover from the removed block cache and is due to be
 *     renamed.
 */
int
__wt_blkcache_open(WT_SESSION_IMPL *session, const char *uri, const char *cfg[],
  bool forced_salvage, bool readonly, uint32_t allocsize, WT_LIVE_RESTORE_FH_META *lr_fh_meta,
  WT_BM **bmp)
{
    WT_BM *bm;
    WT_DECL_RET;

    *bmp = NULL;

    __wt_verbose(session, WT_VERB_BLKCACHE, "open: %s", uri);

    if (__wt_block_disagg_manager_owns_object(session, uri))
        return (__wt_block_disagg_manager_open(session, uri, cfg, forced_salvage, readonly, bmp));

    WT_RET(__wt_calloc_one(session, &bm));
    __wti_bm_method_set(bm, false);

    if (WT_PREFIX_MATCH(uri, "file:")) {
        uri += strlen("file:");
        WT_ERR(__wt_block_open(session, uri, WT_TIERED_OBJECTID_NONE, cfg, forced_salvage, readonly,
          false, allocsize, lr_fh_meta, &bm->block));
    } else {
        /* Leftover tiered URIs must not map onto a file. */
        WT_ERR(__wt_object_unsupported(session, uri));
    }

    *bmp = bm;
    return (0);

err:
    __wt_free(session, bm);
    return (ret);
}
