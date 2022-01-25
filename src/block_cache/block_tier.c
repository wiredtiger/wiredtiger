/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __blkcache_tiered_match --
 *     Check for an already open block matching the tiered name.
 */
static bool
__blkcache_tiered_match(
  WT_SESSION_IMPL *session, const char *name, uint32_t objectid, WT_BLOCK **blockp)
{
    WT_BLOCK *block;
    WT_CONNECTION_IMPL *conn;

    *blockp = NULL;

    conn = S2C(session);

    /*
     * KEITH XXX: Assume the name and object ID pair is unique, and used consistently.
     *
     * Lock and search for the object.
     */
    __wt_spin_lock(session, &conn->block_lock);
    TAILQ_FOREACH (block, &conn->blockqh, q)
        if (block->objectid == objectid && strcmp(block->name, name) == 0) {
            ++block->ref;
            *blockp = block;
            break;
        }
    __wt_spin_unlock(session, &conn->block_lock);
    return (*blockp != NULL);
}

/*
 * __wt_blkcache_tiered_open --
 *     Open a tiered object.
 */
int
__wt_blkcache_tiered_open(
  WT_SESSION_IMPL *session, const char *uri, uint32_t objectid, WT_BLOCK **blockp)
{
    WT_BLOCK *block;
    WT_BUCKET_STORAGE *bstorage;
    WT_CONFIG_ITEM pfx, v;
    WT_DECL_ITEM(tmp);
    WT_DECL_RET;
    WT_TIERED *tiered;
    uint32_t allocsize;
    const char *cfg[2], *object_name, *object_uri, *object_val;
    bool local_only, readonly;

    *blockp = NULL;

    tiered = (WT_TIERED *)session->dhandle;
    object_uri = object_val = NULL;

    WT_ASSERT(session, objectid <= tiered->current_id);
    WT_ASSERT(session, uri == NULL || WT_PREFIX_MATCH(uri, "tiered:"));
    WT_ASSERT(session, (uri == NULL && objectid != 0) || (uri != NULL && objectid == 0));

    /*
     * First look for the local file. This will be the fastest access and we retain recent objects
     * in the local database for awhile. If we're passed a name to open, then by definition it's a
     * local file.
     *
     * FIXME-WT-7590 we will need some kind of locking while we're looking at the tiered structure.
     * This can be called at any time, because we are opening the objects lazily.
     */
    if (uri != NULL)
        objectid = tiered->current_id;
    if (objectid == tiered->current_id) {
        object_uri = tiered->tiers[WT_TIERED_INDEX_LOCAL].name;
        object_name = object_uri;
        WT_PREFIX_SKIP_REQUIRED(session, object_name, "file:");
        local_only = true;
        readonly = false;
    } else {
        WT_ERR(
          __wt_tiered_name(session, &tiered->iface, objectid, WT_TIERED_NAME_OBJECT, &object_uri));
        object_name = object_uri;
        WT_PREFIX_SKIP_REQUIRED(session, object_name, "object:");
        local_only = false;
        readonly = true;

        F_SET(session, WT_SESSION_QUIET_TIERED); /* KEITH XXX: use exists call instead */
    }

    /* Check for an already opened block structure. */
    if (__blkcache_tiered_match(session, object_name, objectid, blockp))
        return (0);

    /*
     * KEITH XXX: We're doing a ton of work to get the allocation size; the block code can do that
     * work, we don't need to do it here. Fix the btree code to pass any changed alloc size in the
     * cfg[] and get rid of the argument and this code.
     */
    WT_ERR(__wt_metadata_search(session, object_uri, (char **)&object_val));
    cfg[0] = object_val;
    cfg[1] = NULL;
    WT_ERR(__wt_config_gets(session, cfg, "allocation_size", &v));
    allocsize = (uint32_t)v.val;

    ret = __wt_block_open(session, object_name, cfg, false, readonly, false, allocsize, &block);
    F_CLR(session, WT_SESSION_QUIET_TIERED);

    if (!local_only && ret != 0) {
        /* We expect a prefix. */
        WT_ERR(__wt_config_gets(session, cfg, "tiered_storage.bucket_prefix", &pfx));
        WT_ASSERT(session, pfx.len != 0);

        WT_ERR(__wt_scr_alloc(session, 0, &tmp));
        WT_ERR(__wt_buf_fmt(session, tmp, "%.*s%s", (int)pfx.len, pfx.str, object_name));

        /* Check for an already opened block structure. */
        if (__blkcache_tiered_match(session, tmp->mem, objectid, blockp))
            return (0);

        bstorage = tiered->bstorage;
        WT_WITH_BUCKET_STORAGE(bstorage, session,
          ret = __wt_block_open(session, tmp->mem, cfg, false, true, true, allocsize, &block));
    }
    WT_ERR(ret);

    block->has_objects = true;
    block->objectid = objectid;
    *blockp = block;

err:
    if (!local_only)
        __wt_free(session, object_uri);
    __wt_free(session, object_val);
    __wt_scr_free(session, &tmp);
    return (ret);
}

/*
 * __wt_blkcache_get_handle --
 *     Get a block handle for an object, creating it if it doesn't exist, optionally cache a
 *     reference.
 */
int
__wt_blkcache_get_handle(
  WT_SESSION_IMPL *session, WT_BLOCK *orig, uint32_t objectid, WT_BLOCK **blockp)
{
    u_int i;

    *blockp = NULL;

    /* We should never be looking for our own object. */
    WT_ASSERT(session, orig == NULL || orig->objectid != objectid);

    /*
     * Check the local cache for the object. We don't have to check the name because we can only
     * reference objects in our name space.
     */
    if (orig != NULL) {
        for (i = 0; i < orig->related_next; ++i)
            if (orig->related[i]->objectid == objectid) {
                *blockp = orig->related[i];
                return (0);
            }

        /* Allocate space to store a reference (do first for less complicated cleanup). */
        WT_RET(__wt_realloc_def(
          session, &orig->related_allocated, orig->related_next + 1, &orig->related));
    }

    /*
     * Get a reference to the object, opening it as necessary.
     *
     * KEITH XXX: could we search the list of block structures without doing the name mapping the
     * tiered-open function implies? If so, that would be faster. I'm pretty sure the answer is
     * "no", but I don't know that for a fact.
     */
    WT_RET(__wt_blkcache_tiered_open(session, NULL, objectid, blockp));

    /* Save a reference in the block in which we started for fast subsequent access. */
    if (orig != NULL)
        orig->related[orig->related_next++] = *blockp;
    return (0);
}
