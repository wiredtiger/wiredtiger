/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wti_block_inmem_checkpoint_load --
 *     Load a checkpoint.
 */
int
__wti_block_inmem_checkpoint_load(WT_BM *bm, WT_SESSION_IMPL *session, const uint8_t *addr,
  size_t addr_size, uint8_t *root_addr, size_t *root_addr_sizep, bool checkpoint)
{
    WT_UNUSED(session);
    WT_UNUSED(root_addr);

    bm->is_live = !checkpoint;
    *root_addr_sizep = 0;

    if (addr != NULL && addr_size != 0)
        WT_RET_MSG(
          session, EINVAL, "in-memory file: cannot load a checkpoint with on-disk block addresses");

    return (0);
}

/*
 * __wti_block_inmem_checkpoint_unload --
 *     Unload a checkpoint point.
 */
int
__wti_block_inmem_checkpoint_unload(WT_BM *bm, WT_SESSION_IMPL *session)
{
    WT_UNUSED(bm);
    WT_UNUSED(session);

    return (0);
}

/*
 * __wti_block_inmem_block_header_init --
 *     Initialize the block header of a disk image laid out for writing.
 */
void
__wti_block_inmem_block_header_init(WT_BM *bm, WT_SESSION_IMPL *session, void *dsk)
{
    WT_UNUSED(bm);
    WT_UNUSED(session);

    memset(WT_BLOCK_HEADER_REF(dsk), 0, WT_BLOCK_HEADER_SIZE);
}

/*
 * __wti_block_inmem_block_header_read --
 *     Return the size of the block header when reading an existing header.
 */
u_int
__wti_block_inmem_block_header_read(WT_BM *bm, WT_SESSION_IMPL *session, const void *dsk)
{
    WT_UNUSED(bm);
    WT_UNUSED(session);
    WT_UNUSED(dsk);

    return ((u_int)WT_BLOCK_HEADER_SIZE);
}

/*
 * __wti_block_inmem_block_header_write --
 *     Return the size of the block header when writing a new header.
 */
u_int
__wti_block_inmem_block_header_write(WT_BM *bm, WT_SESSION_IMPL *session)
{
    WT_UNUSED(bm);
    WT_UNUSED(session);

    return ((u_int)WT_BLOCK_HEADER_SIZE);
}
