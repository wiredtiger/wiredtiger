/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_block_disagg_header_byteswap --
 *     Place holder - it might be necessary to swap things into network byte order.
 */
void
__wt_block_disagg_header_byteswap(WT_BLOCK_DISAGG_HEADER *blk)
{
    WT_UNUSED(blk);
}

/*
 * __wt_block_disagg_header_byteswap_copy --
 *     Place holder - might be necessaryt to handle network order.
 */
void
__wt_block_disagg_header_byteswap_copy(WT_BLOCK_DISAGG_HEADER *from, WT_BLOCK_DISAGG_HEADER *to)
{
    *to = *from;
}

/*
 * __wt_block_disagg_write_size --
 *     Return the buffer size required to write a block.
 */
int
__wt_block_disagg_write_size(size_t *sizep)
{
    /*
     * We write the page size, in bytes, into the block's header as a 4B unsigned value, and it's
     * possible for the engine to accept an item we can't write. For example, a huge key/value where
     * the allocation size has been set to something large will overflow 4B when it tries to align
     * the write. We could make this work, but it's not worth the effort, writing 4GB objects into a
     * btree makes no sense. Limit the writes to (4GB - 1KB), it gives us potential mode bits, and
     * I'm not interested in debugging corner cases anyway.
     */
    *sizep = (size_t)(*sizep + WT_BLOCK_DISAGG_HEADER_BYTE_SIZE);
    return (*sizep > UINT32_MAX - 1024 ? EINVAL : 0);
}

/*
 * __wt_block_disagg_write_internal --
 *     Write a buffer into a block, returning the block's id, size and checksum.
 */
int
__wt_block_disagg_write_internal(WT_SESSION_IMPL *session, WT_BLOCK_DISAGG *block_disagg,
  WT_ITEM *buf, WT_PAGE_BLOCK_META *block_meta, uint32_t *sizep, uint32_t *checksump,
  bool data_checksum, bool checkpoint_io)
{
    WT_BLOCK_DISAGG_HEADER *blk;
    WT_PAGE_LOG_HANDLE *plhandle;
    uint64_t page_id;
    uint32_t checksum, flags;

    WT_ASSERT(session, block_meta != NULL);
    WT_ASSERT(session, block_meta->page_id != WT_BLOCK_INVALID_PAGE_ID);

    *sizep = 0;     /* -Werror=maybe-uninitialized */
    *checksump = 0; /* -Werror=maybe-uninitialized */

    plhandle = block_disagg->plhandle;

    WT_ASSERT_ALWAYS(session, plhandle != NULL, "Disaggregated block store requires page log");

    /*
     * Clear the block header to ensure all of it is initialized, even the unused fields.
     */
    blk = WT_BLOCK_HEADER_REF(buf->mem);
    memset(blk, 0, sizeof(*blk));

    /* Buffers should be aligned for writing. */
    if (!F_ISSET(buf, WT_ITEM_ALIGNED)) {
        WT_ASSERT(session, F_ISSET(buf, WT_ITEM_ALIGNED));
        WT_RET_MSG(session, EINVAL, "direct I/O check: write buffer incorrectly allocated");
    }

    if (buf->size > UINT32_MAX) {
        WT_ASSERT(session, buf->size <= UINT32_MAX);
        WT_RET_MSG(session, EINVAL, "buffer size check: write buffer too large to write");
    }

    /* Get the page ID. */
    page_id = block_meta->page_id;

    /*
     * Update the block's checksum: if our caller specifies, checksum the complete data, otherwise
     * checksum the leading WT_BLOCK_COMPRESS_SKIP bytes. The assumption is applications with good
     * compression support turn off checksums and assume corrupted blocks won't decompress
     * correctly. However, if compression failed to shrink the block, the block wasn't compressed,
     * in which case our caller will tell us to checksum the data to detect corruption. If
     * compression succeeded, we still need to checksum the first WT_BLOCK_COMPRESS_SKIP bytes
     * because they're not compressed, both to give salvage a quick test of whether a block is
     * useful and to give us a test so we don't lose the first WT_BLOCK_COMPRESS_SKIP bytes without
     * noticing.
     *
     * Checksum a little-endian version of the header, and write everything in little-endian format.
     * The checksum is (potentially) returned in a big-endian format, swap it into place in a
     * separate step.
     */
    blk->flags = 0;
    if (data_checksum)
        F_SET(blk, WT_BLOCK_DATA_CKSUM);
    blk->checksum = 0;
    __wt_block_disagg_header_byteswap(blk);
    blk->checksum = checksum =
      __wt_checksum(buf->mem, data_checksum ? buf->size : WT_BLOCK_COMPRESS_SKIP);

    /* XXX Set encrypted, compressed, delta flags */
    flags = 0;

    /* Write the block. */
    /* XXX Set backlink_checkpoint_id, base_checkpoint_id */
    WT_RET(plhandle->plh_put(
      plhandle, &session->iface, page_id, block_meta->checkpoint_id, 0, 0, flags, buf));

    WT_STAT_CONN_INCR(session, disagg_block_put);
    WT_STAT_CONN_INCR(session, block_write);
    WT_STAT_CONN_INCRV(session, block_byte_write, buf->size);
    if (checkpoint_io)
        WT_STAT_CONN_INCRV(session, block_byte_write_checkpoint, buf->size);

    __wt_verbose(session, WT_VERB_WRITE, "off %" PRIuMAX ", size %" PRIuMAX ", checksum %" PRIu32,
      (uintmax_t)page_id, (uintmax_t)buf->size, checksum);

    *sizep = WT_STORE_SIZE(buf->size);
    *checksump = checksum;

    return (0);
}

/*
 * __wt_block_disagg_write --
 *     Write a buffer into a block, returning the block's address cookie.
 */
int
__wt_block_disagg_write(WT_SESSION_IMPL *session, WT_BLOCK *block, WT_ITEM *buf,
  WT_PAGE_BLOCK_META *block_meta, uint8_t *addr, size_t *addr_sizep, bool data_checksum,
  bool checkpoint_io)
{
    WT_BLOCK_DISAGG *block_disagg;
    uint32_t checksum, size;
    uint8_t *endp;

    /*
     * The data structure needs to be cleaned up, so it can be specialized similarly to how a
     * session has public and private parts. That involves a bunch of mechanical replacement in the
     * existing block manager code, so for now just cheat and specialize inside the disagg block
     * code.
     */
    block_disagg = (WT_BLOCK_DISAGG *)block;
    /*
     * Ensure the page header is in little endian order; this doesn't belong here, but it's the best
     * place to catch all callers. After the write, swap values back to native order so callers
     * never see anything other than their original content.
     */
    __wt_page_header_byteswap(buf->mem);
    WT_RET(__wt_block_disagg_write_internal(
      session, block_disagg, buf, block_meta, &size, &checksum, data_checksum, checkpoint_io));
    __wt_page_header_byteswap(buf->mem);

    endp = addr;
    WT_RET(__wt_block_disagg_addr_pack(&endp, block_meta->page_id, block_meta->checkpoint_id,
      block_meta->reconciliation_id, size, checksum));
    *addr_sizep = WT_PTRDIFF(endp, addr);

    return (0);
}
