/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_blkcache_map_name --
 *     Map the name into something we'll open.
 */
int
__wt_blkcache_map_name(
  WT_SESSION_IMPL *session, WT_DATA_HANDLE *dhandle, const char *name, char **realnamep)
{
    WT_TIERED *tiered;

    tiered = (WT_TIERED *)dhandle;

    /* Check for a tiered storage name. */
    if (dhandle != NULL && dhandle->type == WT_DHANDLE_TYPE_TIERED)
        name = tiered->tiers[WT_TIERED_INDEX_LOCAL].name;

    /* Skip any file: URI. */
    if (WT_PREFIX_MATCH(name, "file:"))
        name += strlen("file:");

    return (__wt_strdup(session, name, realnamep));
}

/*
 * __wt_tiered_opener_open --
 *     Open an object by number.
 */
int
__wt_tiered_opener_open(WT_BLOCK_FILE_OPENER *opener, WT_SESSION_IMPL *session, uint32_t object_id,
  WT_FS_OPEN_FILE_TYPE type, u_int flags, WT_FH **fhp)
{
    WT_BUCKET_STORAGE *bstorage;
    WT_CONFIG_ITEM pfx;
    WT_DECL_RET;
    WT_TIERED *tiered;
    size_t len;
    char *tmp;
    const char *cfg[2], *object_name, *object_uri, *object_val;
    bool local_only;

    tiered = opener->cookie;
    object_uri = NULL;
    object_val = NULL;
    tmp = NULL;
    local_only = false;

    WT_ASSERT(session, object_id > 0 && object_id <= tiered->current_id);
    /*
     * First look for the local file. This will be the fastest access and we retain recent objects
     * in the local database for a while.
     */
    if (object_id == tiered->current_id) {   /* KEITH: should never be true */
        bstorage = NULL;
        object_name = tiered->tiers[WT_TIERED_INDEX_LOCAL].name;
        WT_PREFIX_SKIP_REQUIRED(session, object_name, "file:");
        local_only = true;
    } else {
        WT_ERR(
          __wt_tiered_name(session, &tiered->iface, object_id, WT_TIERED_NAME_OBJECT, &object_uri));
        object_name = object_uri;
        WT_PREFIX_SKIP_REQUIRED(session, object_name, "object:");
        LF_SET(WT_FS_OPEN_READONLY);
        WT_ASSERT(session, !FLD_ISSET(flags, WT_FS_OPEN_CREATE));
        F_SET(session, WT_SESSION_QUIET_TIERED);
    }
    ret = __wt_open(session, object_name, type, flags, fhp);
    F_CLR(session, WT_SESSION_QUIET_TIERED);

    /*
     * FIXME-WT-7590 we will need some kind of locking while we're looking at the tiered structure.
     * This can be called at any time, because we are opening the objects lazily.
     */
    if (!local_only && ret != 0) {
        /* Get the prefix from the object's metadata, not the connection. */
        WT_ERR(__wt_metadata_search(session, object_uri, (char **)&object_val));
        cfg[0] = object_val;
        cfg[1] = NULL;
        WT_ERR(__wt_config_gets(session, cfg, "tiered_storage.bucket_prefix", &pfx));
        /* We expect a prefix. */
        WT_ASSERT(session, pfx.len != 0);
        len = strlen(object_name) + pfx.len + 1;
        WT_ERR(__wt_calloc_def(session, len, &tmp));
        WT_ERR(__wt_snprintf(tmp, len, "%.*s%s", (int)pfx.len, pfx.str, object_name));
        bstorage = tiered->bstorage;
        LF_SET(WT_FS_OPEN_FIXED | WT_FS_OPEN_READONLY);
        WT_WITH_BUCKET_STORAGE(
          bstorage, session, { ret = __wt_open(session, tmp, type, flags, fhp); });
    }
err:
    __wt_free(session, object_uri);
    __wt_free(session, object_val);
    __wt_free(session, tmp);
    return (ret);
}

/*
 * __block_switch_writeable --
 *     Switch a new writeable object.
 */
static int
__block_switch_writeable(WT_SESSION_IMPL *session, WT_BLOCK *block, uint32_t object_id)
{
    WT_FH *new_fh, *old_fh;

    /*
     * FIXME-WT-7470: write lock while opening a new write handle.
     *
     * The block manager must always have valid file handle since other threads may have concurrent
     * requests in flight.
     */
    old_fh = block->fh;
    WT_RET(block->opener->open(
      block->opener, session, object_id, WT_FS_OPEN_FILE_TYPE_DATA, block->file_flags, &new_fh));
    block->fh = new_fh;
    block->objectid = object_id;

    return (__wt_close(session, &old_fh));
}

/*
 * __wt_block_fh --
 *     Get a block file handle.
 */
int
__wt_block_fh(WT_SESSION_IMPL *session, WT_BLOCK *block, uint32_t object_id, WT_FH **fhp)
{
    WT_DECL_RET;

    /* It's the local object if there's no object ID or the object ID matches our own. */
    if (object_id == 0 || object_id == block->objectid) {
        *fhp = block->fh;
        return (0);
    }

    /*
     * FIXME-WT-7470: take a read lock to get a handle, and a write lock to open a handle or extend
     * the array.
     *
     * If the object id isn't larger than the array of file handles, see if it's already opened.
     */
    if (object_id * sizeof(WT_FILE_HANDLE *) < block->ofh_alloc &&
      (*fhp = block->ofh[object_id]) != NULL)
        return (0);

    /* Ensure the array is big enough. */
    WT_RET(__wt_realloc_def(session, &block->ofh_alloc, object_id + 1, &block->ofh));
    if (object_id >= block->max_objectid)
        block->max_objectid = object_id + 1;
    if ((*fhp = block->ofh[object_id]) != NULL)
        return (0);

    /*
     * Fail gracefully if we don't have an opener, or if the opener fails: a release that can't read
     * tiered storage blocks might have been pointed at a file that it can read, but that references
     * files it doesn't know about, or there may have been some other mismatch. Regardless, we want
     * to log a specific error message, we're missing a file.
     */
    ret = block->opener->open == NULL ?
      WT_NOTFOUND :
      block->opener->open(block->opener, session, object_id, WT_FS_OPEN_FILE_TYPE_DATA,
        WT_FS_OPEN_READONLY | block->file_flags, &block->ofh[object_id]);
    if (ret == 0) {
        *fhp = block->ofh[object_id];
        return (0);
    }

    WT_RET_MSG(session, ret,
      "object %s with ID %" PRIu32 " referenced unknown object with ID %" PRIu32, block->name,
      block->objectid, object_id);
}

/*
 * __wt_block_switch_object --
 *     Modify an object.
 */
int
__wt_block_switch_object(
  WT_SESSION_IMPL *session, WT_BLOCK *block, uint32_t object_id, uint32_t flags)
{
    WT_UNUSED(flags);

    /*
     * FIXME-WT-7596 the flags argument will be used in the future to perform various tasks,
     * to efficiently mark objects in transition (that is during a switch):
     *  - mark this file as the writeable file (what currently happens)
     *  - disallow writes to this object (reads still allowed, we're about to switch)
     *  - close this object (about to move it, don't allow reopens yet)
     *  - allow opens on this object again
     */
    return (__block_switch_writeable(session, block, object_id));
}
