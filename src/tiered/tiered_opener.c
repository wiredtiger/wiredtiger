/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __tiered_opener_open --
 *     Open an object by number.
 */
int
__tiered_opener_open(WT_BLOCK_FILE_OPENER *opener, WT_SESSION_IMPL *session, void *cookie,
  uint64_t object_id, WT_FS_OPEN_FILE_TYPE type, u_int flags, WT_FH **fhp)
{
    WT_BUCKET_STORAGE *bstorage;
    WT_DECL_RET;
    WT_TIERED *tiered;
    const char *object_name;

    WT_UNUSED(opener);
    tiered = (WT_TIERED *)cookie;

    WT_ASSERT(session, object_id <= tiered->current_id);
    if (object_id == tiered->current_id) {
        bstorage = NULL;
        object_name = tiered->tiers[WT_TIERED_INDEX_LOCAL].name;
        if (!WT_PREFIX_SKIP(object_name, "file:"))
            WT_RET_MSG(session, EINVAL, "expected a 'file:' URI");
    } else {
#if 0
        WT_TIERED_TREE *tiered_tree;
        tiered_tree = (WT_TIERED_TREE *)tiered->tiers[WT_TIERED_INDEX_LOCAL].tier;
        /* TODO: get the WT_TIERED_OBJECT for this object_id, and hence its name */
#endif
        object_name = NULL;
        bstorage = tiered->bstorage;
    }
    WT_WITH_BUCKET_STORAGE(
      bstorage, session, { ret = __wt_open(session, object_name, type, flags, fhp); });
    return (ret);
}

/*
 * __wt_tiered_opener --
 *     Set up an opener for a tiered handle.
 */
int
__wt_tiered_opener(WT_SESSION_IMPL *session, WT_DATA_HANDLE *dhandle,
  WT_BLOCK_FILE_OPENER **openerp, const char **filenamep)
{
    WT_TIERED *tiered;
    const char *filename;

    filename = dhandle->name;
    *openerp = NULL;

    if (dhandle->type == WT_DHANDLE_TYPE_BTREE) {
        if (!WT_PREFIX_SKIP(filename, "file:"))
            WT_RET_MSG(session, EINVAL, "expected a 'file:' URI");
        *filenamep = filename;
    } else if (dhandle->type == WT_DHANDLE_TYPE_TIERED) {
        tiered = (WT_TIERED *)dhandle;
        tiered->opener.open = __tiered_opener_open;
        tiered->opener.cookie = tiered;
        *openerp = &tiered->opener;
        *filenamep = dhandle->name;
    } else
        WT_RET_MSG(session, EINVAL, "invalid URI: %s", dhandle->name);

    return (0);
}
