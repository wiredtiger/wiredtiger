/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wti_block_inmem_addr_string --
 *     Return a printable string representation of an address cookie.
 */
int
__wti_block_inmem_addr_string(
  WT_BM *bm, WT_SESSION_IMPL *session, WT_ITEM *buf, const uint8_t *addr, size_t addr_size)
{
    WT_UNUSED(bm);
    WT_UNUSED(buf);
    WT_UNUSED(addr);
    WT_UNUSED(addr_size);

    return (__wti_block_inmem_io_err(session));
}

/*
 * __wti_block_inmem_read --
 *     Read a buffer from a block.
 */
int
__wti_block_inmem_read(WT_BM *bm, WT_SESSION_IMPL *session, WT_ITEM *buf,
  WT_PAGE_BLOCK_META *block_meta, const uint8_t *addr, size_t addr_size)
{
    WT_UNUSED(bm);
    WT_UNUSED(buf);
    WT_UNUSED(block_meta);
    WT_UNUSED(addr);
    WT_UNUSED(addr_size);

    return (__wti_block_inmem_io_err(session));
}

/*
 * __wti_block_inmem_write --
 *     Write a buffer into a block.
 */
int
__wti_block_inmem_write(WT_BM *bm, WT_SESSION_IMPL *session, WT_ITEM *buf,
  WT_PAGE_BLOCK_META *block_meta, size_t page_image_size, uint8_t *addr, size_t *addr_sizep,
  bool data_checksum, bool checkpoint_io)
{
    WT_UNUSED(bm);
    WT_UNUSED(buf);
    WT_UNUSED(block_meta);
    WT_UNUSED(page_image_size);
    WT_UNUSED(addr);
    WT_UNUSED(addr_sizep);
    WT_UNUSED(data_checksum);
    WT_UNUSED(checkpoint_io);

    return (__wti_block_inmem_io_err(session));
}

/*
 * __wti_block_inmem_checkpoint --
 *     Write a checkpoint.
 */
int
__wti_block_inmem_checkpoint(WT_BM *bm, WT_SESSION_IMPL *session, WT_ITEM *buf,
  WT_PAGE_BLOCK_META *block_meta, WT_CKPT *ckptbase, bool data_checksum)
{
    WT_UNUSED(bm);
    WT_UNUSED(buf);
    WT_UNUSED(block_meta);
    WT_UNUSED(ckptbase);
    WT_UNUSED(data_checksum);

    return (__wti_block_inmem_io_err(session));
}

/*
 * __wti_block_inmem_checkpoint_last --
 *     Return the last checkpoint information.
 */
int
__wti_block_inmem_checkpoint_last(WT_BM *bm, WT_SESSION_IMPL *session, char **metadatap,
  char **checkpoint_listp, WT_ITEM *checkpoint)
{
    WT_UNUSED(bm);
    WT_UNUSED(metadatap);
    WT_UNUSED(checkpoint_listp);
    WT_UNUSED(checkpoint);

    return (__wti_block_inmem_io_err(session));
}

/*
 * __wti_block_inmem_checkpoint_resolve --
 *     Resolve a checkpoint.
 */
int
__wti_block_inmem_checkpoint_resolve(WT_BM *bm, WT_SESSION_IMPL *session, bool failed)
{
    WT_UNUSED(bm);
    WT_UNUSED(failed);

    return (__wti_block_inmem_io_err(session));
}

/*
 * __wti_block_inmem_checkpoint_start --
 *     Start the checkpoint.
 */
int
__wti_block_inmem_checkpoint_start(WT_BM *bm, WT_SESSION_IMPL *session)
{
    WT_UNUSED(bm);

    return (__wti_block_inmem_io_err(session));
}

/*
 * __wti_block_inmem_map_discard --
 *     Discard a mapped segment.
 */
int
__wti_block_inmem_map_discard(WT_BM *bm, WT_SESSION_IMPL *session, void *map, size_t len)
{
    WT_UNUSED(bm);
    WT_UNUSED(map);
    WT_UNUSED(len);

    return (__wti_block_inmem_io_err(session));
}

/*
 * __wti_block_inmem_salvage_next --
 *     Return the next block from the file.
 */
int
__wti_block_inmem_salvage_next(
  WT_BM *bm, WT_SESSION_IMPL *session, uint8_t *addr, size_t *addr_sizep, bool *eofp)
{
    WT_UNUSED(bm);
    WT_UNUSED(addr);
    WT_UNUSED(addr_sizep);
    WT_UNUSED(eofp);

    return (__wti_block_inmem_io_err(session));
}

/*
 * __wti_block_inmem_salvage_valid --
 *     Inform salvage a block is valid.
 */
int
__wti_block_inmem_salvage_valid(
  WT_BM *bm, WT_SESSION_IMPL *session, uint8_t *addr, size_t addr_size, bool valid)
{
    WT_UNUSED(bm);
    WT_UNUSED(addr);
    WT_UNUSED(addr_size);
    WT_UNUSED(valid);

    return (__wti_block_inmem_io_err(session));
}

/*
 * __wti_block_inmem_verify_end --
 *     End a block manager verify.
 */
int
__wti_block_inmem_verify_end(WT_BM *bm, WT_SESSION_IMPL *session, bool verify_success)
{
    WT_UNUSED(bm);
    WT_UNUSED(verify_success);

    return (__wti_block_inmem_io_err(session));
}

/*
 * __wti_block_inmem_verify_start --
 *     Start a block manager verify.
 */
int
__wti_block_inmem_verify_start(
  WT_BM *bm, WT_SESSION_IMPL *session, WT_CKPT *ckptbase, const char *cfg[])
{
    WT_UNUSED(bm);
    WT_UNUSED(ckptbase);
    WT_UNUSED(cfg);

    return (__wti_block_inmem_io_err(session));
}

/*
 * __wti_block_inmem_verify_addr --
 *     Verify an address.
 */
int
__wti_block_inmem_verify_addr(
  WT_BM *bm, WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size)
{
    WT_UNUSED(bm);
    WT_UNUSED(addr);
    WT_UNUSED(addr_size);

    return (__wti_block_inmem_io_err(session));
}

/*
 * __wti_block_inmem_compact_page_skip --
 *     Return if a page is useful for compaction.
 */
int
__wti_block_inmem_compact_page_skip(
  WT_BM *bm, WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size, bool *skipp)
{
    WT_UNUSED(bm);
    WT_UNUSED(addr);
    WT_UNUSED(addr_size);
    WT_UNUSED(skipp);

    return (__wti_block_inmem_io_err(session));
}

/*
 * __wti_block_inmem_compact_skip --
 *     Return if a file can be compacted.
 */
int
__wti_block_inmem_compact_skip(WT_BM *bm, WT_SESSION_IMPL *session, bool *skipp)
{
    WT_UNUSED(bm);
    WT_UNUSED(skipp);

    return (__wti_block_inmem_io_err(session));
}

/*
 * __wti_block_inmem_compact_page_rewrite --
 *     Rewrite a page for compaction.
 */
int
__wti_block_inmem_compact_page_rewrite(
  WT_BM *bm, WT_SESSION_IMPL *session, uint8_t *addr, size_t *addr_sizep, bool *writtenp)
{
    WT_UNUSED(bm);
    WT_UNUSED(addr);
    WT_UNUSED(addr_sizep);
    WT_UNUSED(writtenp);

    return (__wti_block_inmem_io_err(session));
}

/*
 * __wti_block_inmem_corrupt --
 *     Report a corrupt block.
 */
int
__wti_block_inmem_corrupt(WT_BM *bm, WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size)
{
    WT_UNUSED(bm);
    WT_UNUSED(addr);
    WT_UNUSED(addr_size);

    WT_RET_MSG(session, EINVAL, "in-memory file: corrupt block reporting is not supported");
}

/*
 * __wti_block_inmem_free --
 *     Free a block of space to the underlying file.
 */
int
__wti_block_inmem_free(
  WT_BM *bm, WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size, bool is_root)
{
    WT_UNUSED(bm);
    WT_UNUSED(session);
    WT_UNUSED(addr);
    WT_UNUSED(addr_size);
    WT_UNUSED(is_root);

    return (0);
}

/*
 * __wti_block_inmem_sync --
 *     Flush a file to disk.
 */
int
__wti_block_inmem_sync(WT_BM *bm, WT_SESSION_IMPL *session, bool block)
{
    WT_UNUSED(bm);
    WT_UNUSED(session);
    WT_UNUSED(block);

    return (0);
}

/*
 * __wti_block_inmem_is_mapped --
 *     Return if the file is mapped into memory.
 */
bool
__wti_block_inmem_is_mapped(WT_BM *bm, WT_SESSION_IMPL *session)
{
    WT_UNUSED(bm);
    WT_UNUSED(session);

    return (false);
}

/*
 * __wti_block_inmem_compact_progress --
 *     Report compaction progress.
 */
void
__wti_block_inmem_compact_progress(WT_BM *bm, WT_SESSION_IMPL *session)
{
    WT_UNUSED(bm);
    WT_UNUSED(session);
}

/*
 * __wti_block_inmem_can_truncate --
 *     Return whether there's free space at the end of the file.
 */
bool
__wti_block_inmem_can_truncate(WT_BM *bm, WT_SESSION_IMPL *session)
{
    WT_UNUSED(bm);
    WT_UNUSED(session);

    return (false);
}
