/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_bmp_checkpoint_load --
 *     Load a checkpoint.
 */
int
__wt_bmp_checkpoint_load(WT_BM *bm, WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size,
  uint8_t *root_addr, size_t *root_addr_sizep, bool checkpoint)
{
    WT_UNUSED(bm);
    WT_UNUSED(session);
    WT_UNUSED(addr);
    WT_UNUSED(addr_size);
    WT_UNUSED(root_addr);
    WT_UNUSED(root_addr_sizep);
    WT_UNUSED(checkpoint);
    return (0);
}

/*
 * __wt_bmp_checkpoint_resolve --
 *     Resolve the checkpoint.
 */
int
__wt_bmp_checkpoint_resolve(WT_BM *bm, WT_SESSION_IMPL *session, bool failed)
{
    WT_UNUSED(bm);
    WT_UNUSED(session);
    WT_UNUSED(failed);
    return (0);
}

/*
 * __wt_bmp_checkpoint_start --
 *     Start the checkpoint.
 */
int
__wt_bmp_checkpoint_start(WT_BM *bm, WT_SESSION_IMPL *session)
{
    WT_UNUSED(bm);
    WT_UNUSED(session);
    return (0);
}

/*
 * __wt_bmp_checkpoint_unload --
 *     Unload a checkpoint point.
 */
int
__wt_bmp_checkpoint_unload(WT_BM *bm, WT_SESSION_IMPL *session)
{
    WT_UNUSED(bm);
    WT_UNUSED(session);
    return (0);
}

/*
 * __wt_bmp_compact_end --
 *     End a block manager compaction.
 */
int
__wt_bmp_compact_end(WT_BM *bm, WT_SESSION_IMPL *session)
{
    WT_UNUSED(bm);
    WT_UNUSED(session);
    return (0);
}

/*
 * __wt_bmp_compact_page_skip --
 *     Return if a page is useful for compaction.
 */
int
__wt_bmp_compact_page_skip(
  WT_BM *bm, WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size, bool *skipp)
{
    WT_UNUSED(bm);
    WT_UNUSED(session);
    WT_UNUSED(addr);
    WT_UNUSED(addr_size);
    WT_UNUSED(skipp);
    return (0);
}

/*
 * __wt_bmp_compact_skip --
 *     Return if a file can be compacted.
 */
int
__wt_bmp_compact_skip(WT_BM *bm, WT_SESSION_IMPL *session, bool *skipp)
{
    WT_UNUSED(bm);
    WT_UNUSED(session);
    WT_UNUSED(skipp);
    return (0);
}

/*
 * __wt_bmp_compact_start --
 *     Start a block manager compaction.
 */
int
__wt_bmp_compact_start(WT_BM *bm, WT_SESSION_IMPL *session)
{
    WT_UNUSED(bm);
    WT_UNUSED(session);
    return (0);
}

/*
 * __wt_bmp_is_mapped --
 *     Return if the file is mapped into memory.
 */
bool
__wt_bmp_is_mapped(WT_BM *bm, WT_SESSION_IMPL *session)
{
    WT_UNUSED(bm);
    WT_UNUSED(session);
    return (false);
}

/*
 * __wt_bmp_map_discard --
 *     Discard a mapped segment.
 */
int
__wt_bmp_map_discard(WT_BM *bm, WT_SESSION_IMPL *session, void *map, size_t len)
{
    WT_UNUSED(bm);
    WT_UNUSED(session);
    WT_UNUSED(map);
    WT_UNUSED(len);
    return (0);
}

/*
 * __wt_bmp_salvage_end --
 *     End a block manager salvage.
 */
int
__wt_bmp_salvage_end(WT_BM *bm, WT_SESSION_IMPL *session)
{
    WT_UNUSED(bm);
    WT_UNUSED(session);
    return (0);
}

/*
 * __wt_bmp_salvage_next --
 *     Return the next block from the file.
 */
int
__wt_bmp_salvage_next(
  WT_BM *bm, WT_SESSION_IMPL *session, uint8_t *addr, size_t *addr_sizep, bool *eofp)
{
    WT_UNUSED(bm);
    WT_UNUSED(session);
    WT_UNUSED(addr);
    WT_UNUSED(addr_sizep);
    WT_UNUSED(eofp);
    return (0);
}

/*
 * __wt_bmp_salvage_start --
 *     Start a block manager salvage.
 */
int
__wt_bmp_salvage_start(WT_BM *bm, WT_SESSION_IMPL *session)
{
    WT_UNUSED(bm);
    WT_UNUSED(session);
    return (0);
}

/*
 * __wt_bmp_salvage_valid --
 *     Inform salvage a block is valid.
 */
int
__wt_bmp_salvage_valid(
  WT_BM *bm, WT_SESSION_IMPL *session, uint8_t *addr, size_t addr_size, bool valid)
{
    WT_UNUSED(bm);
    WT_UNUSED(session);
    WT_UNUSED(addr);
    WT_UNUSED(addr_size);
    WT_UNUSED(valid);
    return (0);
}

/*
 * __wt_bmp_sync --
 *     Flush a file to disk.
 */
int
__wt_bmp_sync(WT_BM *bm, WT_SESSION_IMPL *session, bool block)
{
    WT_UNUSED(bm);
    WT_UNUSED(session);
    WT_UNUSED(block);
    return (0);
}

/*
 * __wt_bmp_verify_addr --
 *     Verify an address.
 */
int
__wt_bmp_verify_addr(WT_BM *bm, WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size)
{
    WT_UNUSED(bm);
    WT_UNUSED(session);
    WT_UNUSED(addr);
    WT_UNUSED(addr_size);
    return (0);
}

/*
 * __wt_bmp_verify_end --
 *     End a block manager verify.
 */
int
__wt_bmp_verify_end(WT_BM *bm, WT_SESSION_IMPL *session)
{
    WT_UNUSED(bm);
    WT_UNUSED(session);
    return (0);
}

/*
 * __wt_bmp_verify_start --
 *     Start a block manager verify.
 */
int
__wt_bmp_verify_start(WT_BM *bm, WT_SESSION_IMPL *session, WT_CKPT *ckptbase, const char *cfg[])
{
    WT_UNUSED(bm);
    WT_UNUSED(session);
    WT_UNUSED(ckptbase);
    WT_UNUSED(cfg);
    return (0);
}
