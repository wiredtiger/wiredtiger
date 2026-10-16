/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wti_block_inmem_io_err --
 *     Per-file in_memory objects do not persist block addresses.
 */
int
__wti_block_inmem_io_err(WT_SESSION_IMPL *session)
{
    WT_RET_MSG(session, EINVAL, "in-memory file: block manager I/O is not supported");
}

/*
 * __block_inmem_destroy --
 *     Destroy a block handle.
 */
static int
__block_inmem_destroy(WT_SESSION_IMPL *session, WT_BLOCK_INMEM *block_inmem)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    uint64_t bucket, hash;

    conn = S2C(session);
    hash = __wt_hash_city64(block_inmem->name, strlen(block_inmem->name));
    bucket = hash & (conn->hash_size - 1);
    WT_CONN_BLOCK_REMOVE(conn, (WT_BLOCK *)block_inmem, bucket);

    __wt_free(session, block_inmem->name);
    __wt_overwrite_and_free(session, block_inmem);

    return (ret);
}

/*
 * __wti_block_inmem_open --
 *     Open a block handle.
 */
int
__wti_block_inmem_open(WT_SESSION_IMPL *session, const char *filename, const char *cfg[],
  bool forced_salvage, bool readonly, WT_BLOCK **blockp)
{
    WT_BLOCK *block;
    WT_BLOCK_INMEM *block_inmem;
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    uint64_t bucket, hash, tableid;

    WT_UNUSED(cfg);
    WT_UNUSED(forced_salvage);
    WT_UNUSED(readonly);

    *blockp = NULL;
    block_inmem = NULL;

    WT_ASSERT(session, filename != NULL);

    __wt_verbose(session, WT_VERB_BLOCK, "open: %s (table_id: %" PRIu64 ")", filename,
      (uint64_t)S2BT(session)->id);

    conn = S2C(session);
    tableid = S2BT(session)->id;
    hash = __wt_hash_city64(filename, strlen(filename));
    bucket = hash & (conn->hash_size - 1);
    __wt_spin_lock(session, &conn->block_lock);
    TAILQ_FOREACH (block, &conn->blockhash[bucket], hashq) {
        if (strcmp(filename, block->name) == 0 && ((WT_BLOCK_INMEM *)block)->tableid == tableid) {
            ++block->ref;
            *blockp = block;
            __wt_spin_unlock(session, &conn->block_lock);
            return (0);
        }
    }

    WT_ERR(__wt_calloc_one(session, &block_inmem));
    block_inmem->ref = 1;
    WT_CONN_BLOCK_INSERT(conn, (WT_BLOCK *)block_inmem, bucket);

    WT_ERR(__wt_strdup(session, filename, &block_inmem->name));
    block_inmem->tableid = tableid;

    *blockp = (WT_BLOCK *)block_inmem;
    __wt_spin_unlock(session, &conn->block_lock);
    return (0);

err:
    if (block_inmem != NULL)
        WT_TRET(__block_inmem_destroy(session, block_inmem));
    __wt_spin_unlock(session, &conn->block_lock);
    return (ret);
}

/*
 * __wti_block_inmem_close --
 *     Close a block handle.
 */
int
__wti_block_inmem_close(WT_SESSION_IMPL *session, WT_BLOCK_INMEM *block_inmem)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;

    if (block_inmem == NULL)
        return (0);

    conn = S2C(session);

    __wt_verbose(session, WT_VERB_BLOCK, "close: %s (table_id: %" PRIu64 ")",
      block_inmem->name == NULL ? "" : block_inmem->name, block_inmem->tableid);

    __wt_spin_lock(session, &conn->block_lock);

    if (block_inmem->ref == 0 || --block_inmem->ref == 0)
        ret = __block_inmem_destroy(session, block_inmem);

    __wt_spin_unlock(session, &conn->block_lock);

    return (ret);
}

/*
 * __wti_block_inmem_addr_invalid --
 *     Return an error code if an address cookie is invalid.
 */
int
__wti_block_inmem_addr_invalid(WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size)
{
    if (addr == NULL || addr_size == 0)
        return (0);

    WT_RET_MSG(session, EINVAL, "in-memory file: invalid block address cookie");
}

/*
 * __wti_block_inmem_write_size --
 *     Return the buffer size required to write a block.
 */
int
__wti_block_inmem_write_size(WT_SESSION_IMPL *session, size_t *sizep)
{
    WT_BTREE *btree;

    btree = S2BT(session);
    *sizep = (size_t)WT_ALIGN(*sizep + WT_BLOCK_HEADER_BYTE_SIZE, btree->allocsize);
    if (*sizep > UINT32_MAX - 1024)
        WT_RET_MSG(session, EINVAL, "requested block size is too large");

    return (0);
}

/*
 * __wti_block_inmem_manager_size --
 *     Return the size of a live block handle.
 */
int
__wti_block_inmem_manager_size(WT_BM *bm, WT_SESSION_IMPL *session, wt_off_t *sizep)
{
    WT_UNUSED(bm);
    WT_UNUSED(session);

    *sizep = 0;
    return (0);
}

/*
 * __wti_block_inmem_stat --
 *     Block-manager statistics.
 */
int
__wti_block_inmem_stat(WT_SESSION_IMPL *session, WT_BLOCK_INMEM *block_inmem, WT_DSRC_STATS *stats)
{
    WT_UNUSED(block_inmem);
    WT_UNUSED(session);
    WT_UNUSED(stats);

    return (0);
}
