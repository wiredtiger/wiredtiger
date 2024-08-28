/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

#if 0
/*
 * __wt_block_pantry_manager_drop --
 *     Drop a file - this isn't currently called. Pantry manager isn't being notified when an object
 *     should be cleaned up. Doing so will require wiring in non-default block managers such that
 *     the metadata tracking knows which to call. It should also move into the block manager
 *     interface, rather than being a direct call.
 */
int
__wt_block_pantry_manager_drop(WT_SESSION_IMPL *session, const char *filename, bool durable)
{
    return (0);
}
#endif

/*
 * __wt_block_pantry_manager_create --
 *     Create a file - it's a bit of a game with a new block manager. The file is created when
 *     adding a new table to the metadata, before a btree handle is open. The block storage manager
 *     is generally created when the btree handle is opened. The caller of this will need to check
 *     for and instantiate a storage source.
 */
int
__wt_block_pantry_manager_create(
  WT_SESSION_IMPL *session, WT_BUCKET_STORAGE *bstorage, const char *filename)
{
#if 1
    WT_UNUSED(session);
    WT_UNUSED(bstorage);
    WT_UNUSED(filename);

    /*
     * The default block manager creates the physical underlying file here and writes an initial
     * block into it. At the moment we don't need to do that for our special storage source - it's
     * going to magically create the file on first access and doesn't have a block manager provided
     * special leading descriptor block.
     */
    return (0);
#else
    WT_DECL_ITEM(tmp);
    WT_DECL_RET;
    WT_FH *fh;
    int suffix;
    bool exists;

    /*
     * Create the underlying file and open a handle.
     *
     * Since WiredTiger schema operations are (currently) non-transactional, it's possible to see a
     * partially-created file left from a previous create. Further, there's nothing to prevent users
     * from creating files in our space. Move any existing files out of the way and complain.
     */
    for (;;) {
        ret = bstorage->file_system->fs_open_file(bstorage->file_system, &session->iface, filename,
          WT_FS_OPEN_FILE_TYPE_DATA, WT_FS_OPEN_CREATE | WT_FS_OPEN_DURABLE | WT_FS_OPEN_EXCLUSIVE,
          &fh);
        if (ret == 0)
            break;
        WT_ERR_TEST(ret != EEXIST, ret, false);
    }

    /* Write out the file's meta-data. */
    ret = __wt_desc_write(session, fh, allocsize);

    /* Close the file handle. */
    WT_TRET(__wt_close(session, &fh));

    /* Undo any create on error. */
    if (ret != 0)
        WT_TRET(__wt_fs_remove(session, filename, false, false));

err:
    __wt_scr_free(session, &tmp);

    return (ret);
#endif
}

/*
 * __block_pantry_destroy --
 *     Destroy a block handle.
 */
static int
__block_pantry_destroy(WT_SESSION_IMPL *session, WT_BLOCK_PANTRY *block_pantry)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    uint64_t bucket, hash;

    conn = S2C(session);
    hash = __wt_hash_city64(block_pantry->name, strlen(block_pantry->name));
    bucket = hash & (conn->hash_size - 1);
    WT_CONN_BLOCK_REMOVE(conn, block_pantry, bucket);

    __wt_free(session, block_pantry->name);

    if (block_pantry->fh != NULL)
        WT_TRET(__wt_close(session, &block_pantry->fh));

    __wt_overwrite_and_free(session, block_pantry);

    return (ret);
}

/*
 * __wt_block_pantry_open --
 *     Open a block handle.
 */
int
__wt_block_pantry_open(WT_SESSION_IMPL *session, const char *filename, const char *cfg[],
  bool forced_salvage, bool readonly, WT_BLOCK **blockp)
{
    WT_BLOCK *block;
    WT_BLOCK_PANTRY *block_pantry;
    WT_BUCKET_STORAGE *bstorage;
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    uint64_t bucket, hash;
    uint32_t flags;

    WT_UNUSED(cfg);
    WT_UNUSED(forced_salvage);
    WT_UNUSED(readonly);

    *blockp = NULL;
    block_pantry = NULL;
    flags = WT_FS_OPEN_CREATE; /* Eventually the create would ideally be done earlier */

    if (S2C(session)->iface.stable_follower_prefix != NULL)
        flags |= WT_FS_OPEN_FIXED;

    __wt_verbose(session, WT_VERB_BLOCK, "open: %s", filename);

    conn = S2C(session);
    hash = __wt_hash_city64(filename, strlen(filename));
    bucket = hash & (conn->hash_size - 1);
    __wt_spin_lock(session, &conn->block_lock);
    TAILQ_FOREACH (block, &conn->blockhash[bucket], hashq) {
        /* TODO: Should check to make sure this is the right type of block */
        if (strcmp(filename, block->name) == 0) {
            ++block->ref;
            *blockp = block;
            __wt_spin_unlock(session, &conn->block_lock);
            return (0);
        }
    }

    bstorage = ((WT_BTREE *)session->dhandle->handle)->bstorage;

    WT_ASSERT_ALWAYS(session, bstorage != NULL,
      "pantry tables need a custom data source that supports object storage");

    /*
     * Basic structure allocation, initialization.
     *
     * Note: set the block's name-hash value before any work that can fail because cleanup calls the
     * block destroy code which uses that hash value to remove the block from the underlying linked
     * lists.
     */
    WT_ERR(__wt_calloc_one(session, &block_pantry));
    block_pantry->ref = 1;
    WT_CONN_BLOCK_INSERT(conn, (WT_BLOCK *)block_pantry, bucket);

    WT_ERR(__wt_strdup(session, filename, &block_pantry->name));

    WT_ERR(__wt_open_fs(session, filename, WT_FS_OPEN_FILE_TYPE_DATA, flags, bstorage->file_system,
      &block_pantry->fh));

    WT_ASSERT_ALWAYS(session, block_pantry->fh->handle->fh_obj_put != NULL,
      "pantry tables need a file interface that supports object storage");

    *blockp = (WT_BLOCK *)block_pantry;
    __wt_spin_unlock(session, &conn->block_lock);
    return (0);

err:
    if (block_pantry != NULL)
        WT_TRET(__block_pantry_destroy(session, block_pantry));
    __wt_spin_unlock(session, &conn->block_lock);
    return (ret);
}

/*
 * __wt_block_pantry_close --
 *     Close a block handle.
 */
int
__wt_block_pantry_close(WT_SESSION_IMPL *session, WT_BLOCK_PANTRY *block_pantry)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;

    if (block_pantry == NULL) /* Safety check */
        return (0);

    conn = S2C(session);

    __wt_verbose(
      session, WT_VERB_BLOCK, "close: %s", block_pantry->name == NULL ? "" : block_pantry->name);

    __wt_spin_lock(session, &conn->block_lock);

    /* Reference count is initialized to 1. */
    if (block_pantry->ref == 0 || --block_pantry->ref == 0)
        ret = __block_pantry_destroy(session, block_pantry);

    __wt_spin_unlock(session, &conn->block_lock);

    return (ret);
}

/*
 * __wt_block_pantry_stat --
 *     Set the statistics for a live block handle.
 */
void
__wt_block_pantry_stat(
  WT_SESSION_IMPL *session, WT_BLOCK_PANTRY *block_pantry, WT_DSRC_STATS *stats)
{
    WT_UNUSED(block_pantry);

    /* Fill this out. */
    WT_STAT_WRITE(session, stats, block_magic, WT_BLOCK_MAGIC);
}

/*
 * __wt_block_pantry_manager_size --
 *     Return the size of a live block handle.
 */
int
__wt_block_pantry_manager_size(WT_BM *bm, WT_SESSION_IMPL *session, wt_off_t *sizep)
{
    WT_UNUSED(session);

    *sizep = bm->block->size;
    return (0);
}
