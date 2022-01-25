/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_blkcache_tiered_open --
 *     Open a tiered object.
 */
int
__wt_blkcache_tiered_open(
  WT_SESSION_IMPL *session, const char *uri, uint32_t object_id, WT_BLOCK **blockp)
{
    WT_BLOCK *block;
    WT_BUCKET_STORAGE *bstorage;
    WT_CONFIG_ITEM pfx, v;
    WT_DECL_RET;
    WT_TIERED *tiered;
    size_t len;
    uint32_t allocsize;
    char *tmp;
    const char *cfg[2], *object_name, *object_uri, *object_val;
    bool local_only, readonly;

    *blockp = NULL;

    tiered = (WT_TIERED *)session->dhandle;
    object_uri = NULL;
    object_val = NULL;
    tmp = NULL;

    WT_ASSERT(session, object_id <= tiered->current_id);
    WT_ASSERT(session, uri == NULL || WT_PREFIX_MATCH(uri, "tiered:"));
    WT_ASSERT(session, (uri == NULL && object_id != 0) || (uri != NULL && object_id == 0));

    /*
     * First look for the local file. This will be the fastest access and we retain recent objects
     * in the local database for awhile.
     */
    if (uri != NULL)
        object_id = tiered->current_id;
    if (object_id == tiered->current_id) {
        object_uri = tiered->tiers[WT_TIERED_INDEX_LOCAL].name;
        object_name = object_uri;
        WT_PREFIX_SKIP_REQUIRED(session, object_name, "file:");
        local_only = true;
        readonly = false;
    } else {
        WT_ERR(
          __wt_tiered_name(session, &tiered->iface, object_id, WT_TIERED_NAME_OBJECT, &object_uri));
        object_name = object_uri;
        WT_PREFIX_SKIP_REQUIRED(session, object_name, "object:");
        local_only = false;
        readonly = true;

        F_SET(session, WT_SESSION_QUIET_TIERED); /* KEITH: use exists call instead */
    }
    WT_ERR(__wt_metadata_search(session, object_uri, (char **)&object_val));
    cfg[0] = object_val;
    cfg[1] = NULL;
    WT_ERR(__wt_config_gets(session, cfg, "allocation_size", &v));
    allocsize = (uint32_t)v.val;
    ret = __wt_block_open(session, object_name, cfg, false, readonly, false, allocsize, &block);
    F_CLR(session, WT_SESSION_QUIET_TIERED);

    /*
     * FIXME-WT-7590 we will need some kind of locking while we're looking at the tiered structure.
     * This can be called at any time, because we are opening the objects lazily.
     */
    if (!local_only && ret != 0) {
        WT_ERR(__wt_config_gets(session, cfg, "tiered_storage.bucket_prefix", &pfx));
        /* We expect a prefix. */
        WT_ASSERT(session, pfx.len != 0);
        len = strlen(object_name) + pfx.len + 1;
        WT_ERR(__wt_calloc_def(session, len, &tmp)); /* KEITH XXX use a scratch buffer */
        WT_ERR(__wt_snprintf(tmp, len, "%.*s%s", (int)pfx.len, pfx.str, object_name));
        bstorage = tiered->bstorage;
        WT_WITH_BUCKET_STORAGE(bstorage, session,
          ret = __wt_block_open(session, object_name, cfg, false, true, true, allocsize, &block));
        WT_ERR(ret);
    }

    block->has_objects = true;
    block->objectid = object_id;
    *blockp = block;

err:
    if (!local_only)
        __wt_free(session, object_uri);
    __wt_free(session, object_val);
    __wt_free(session, tmp);
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
    WT_BLOCK *block;
    WT_CONNECTION_IMPL *conn;
    u_int i;
    const char *name;

    *blockp = NULL;

    conn = S2C(session);

    /* We should never be looking for our own object. */
    WT_ASSERT(session, orig == NULL || orig->objectid != objectid);

    /* Check the local cache for the object. */
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
     * Lock and search for the object.
     *
     * KEITH XXX: Assume the name and object ID pair is unique, and used consistently. I'm not sure
     * that's the case, and taking the name from the session dhandle isn't right.
     */
    name = orig == NULL ? session->dhandle->name : orig->name;
    __wt_spin_lock(session, &conn->block_lock);
    TAILQ_FOREACH (block, &conn->blockqh, q)
        if (block->objectid == objectid && strcmp(block->name, name) == 0) {
            ++block->ref;
            *blockp = block;
            break;
        }
    __wt_spin_unlock(session, &conn->block_lock);

    /* Open if it wasn't there. */
    if (block == NULL)
        WT_RET(__wt_blkcache_tiered_open(session, NULL, objectid, blockp));

    /* Save a reference in the block in which we started for fast subsequent access. */
    if (orig != NULL)
        orig->related[orig->related_next++] = *blockp;
    return (0);
}
