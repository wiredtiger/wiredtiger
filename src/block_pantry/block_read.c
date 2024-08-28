/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_bmp_corrupt --
 *     Report a block has been corrupted, external API.
 */
int
__wt_bmp_corrupt(WT_BM *bm, WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size)
{
    WT_DECL_ITEM(tmp);
    WT_DECL_RET;
    uint64_t page_id;
    uint32_t checksum, size;

    /* Read the block. */
    WT_RET(__wt_scr_alloc(session, 0, &tmp));
    WT_ERR(__wt_bmp_read(bm, session, tmp, addr, addr_size));

    /* Crack the cookie, dump the block. */
    WT_ERR(__wt_block_pantry_addr_unpack(&addr, &page_id, &size, &checksum));
    WT_ERR(__wt_bm_corrupt_dump(session, tmp, 0, (wt_off_t)page_id, size, checksum));

err:
    __wt_scr_free(session, &tmp);
    return (ret);
}

/*
 * __block_pantry_read --
 *     Read an addr/size pair referenced block into a buffer.
 */
static int
__block_pantry_read(WT_SESSION_IMPL *session, WT_BLOCK_PANTRY *block_pantry, WT_ITEM *buf,
  uint64_t pantry_id, uint32_t size, uint32_t checksum)
{
    WT_BLOCK_PANTRY_HEADER *blk, swap;
    size_t bufsize;

    __wt_verbose(session, WT_VERB_READ, "off %" PRIuMAX ", size %" PRIu32 ", checksum %" PRIu32,
      (uintmax_t)pantry_id, size, checksum);

    WT_STAT_CONN_INCR(session, block_read);
    WT_STAT_CONN_INCRV(session, block_byte_read, size);

    /*
     * Grow the buffer as necessary and read the block. Buffers should be aligned for reading, but
     * there are lots of buffers (for example, file cursors have two buffers each, key and value),
     * and it's difficult to be sure we've found all of them. If the buffer isn't aligned, it's an
     * easy fix: set the flag and guarantee we reallocate it. (Most of the time on reads, the buffer
     * memory has not yet been allocated, so we're not adding any additional processing time.)
     */
    if (F_ISSET(buf, WT_ITEM_ALIGNED))
        bufsize = size;
    else {
        F_SET(buf, WT_ITEM_ALIGNED);
        bufsize = WT_MAX(size, buf->memsize + 10);
    }
    WT_RET(__wt_buf_init(session, buf, bufsize));
    WT_RET(block_pantry->fh->handle->fh_obj_get(
      block_pantry->fh->handle, &session->iface, pantry_id, buf));

    /*
     * We incrementally read through the structure before doing a checksum, do little- to big-endian
     * handling early on, and then select from the original or swapped structure as needed.
     */
    blk = WT_BLOCK_HEADER_REF(buf->mem);
    __wt_block_pantry_header_byteswap_copy(blk, &swap);
    if (swap.checksum == checksum) {
        blk->checksum = 0;
        if (__wt_checksum_match(buf->mem,
              F_ISSET(&swap, WT_BLOCK_DATA_CKSUM) ? size : WT_BLOCK_COMPRESS_SKIP, checksum)) {
            /*
             * Swap the page-header as needed; this doesn't belong here, but it's the best place to
             * catch all callers.
             */
            __wt_page_header_byteswap(buf->mem);
            return (0);
        }

        if (!F_ISSET(session, WT_SESSION_QUIET_CORRUPT_FILE))
            __wt_errx(session,
              "%s: read checksum error for %" PRIu32
              "B block at "
              "offset %" PRIuMAX
              ": calculated block checksum "
              " doesn't match expected checksum",
              block_pantry->name, size, (uintmax_t)pantry_id);
    } else if (!F_ISSET(session, WT_SESSION_QUIET_CORRUPT_FILE))
        __wt_errx(session,
          "%s: read checksum error for %" PRIu32
          "B block at "
          "offset %" PRIuMAX
          ": block header checksum "
          "of %" PRIu32
          " doesn't match expected checksum "
          "of %" PRIu32,
          block_pantry->name, size, (uintmax_t)pantry_id, swap.checksum, checksum);

    if (!F_ISSET(session, WT_SESSION_QUIET_CORRUPT_FILE))
        WT_IGNORE_RET(__wt_bm_corrupt_dump(session, buf, 0, (wt_off_t)pantry_id, size, checksum));

    /* Panic if a checksum fails during an ordinary read. */
    F_SET(S2C(session), WT_CONN_DATA_CORRUPTION);
    if (F_ISSET(session, WT_SESSION_QUIET_CORRUPT_FILE))
        return (WT_ERROR);
    WT_RET_PANIC(session, WT_ERROR, "%s: fatal read error", block_pantry->name);
}

/*
 * __wt_bmp_read --
 *     Map or read address cookie referenced block into a buffer.
 */
int
__wt_bmp_read(
  WT_BM *bm, WT_SESSION_IMPL *session, WT_ITEM *buf, const uint8_t *addr, size_t addr_size)
{
    WT_BLOCK_PANTRY *block_pantry;
    uint64_t page_id;
    uint32_t checksum, size;

    WT_UNUSED(addr_size);
    block_pantry = (WT_BLOCK_PANTRY *)bm->block;

    /* Crack the cookie. */
    WT_RET(__wt_block_pantry_addr_unpack(&addr, &page_id, &size, &checksum));

    /* Read the block. */
    WT_RET(__block_pantry_read(session, block_pantry, buf, page_id, size, checksum));

    return (0);
}
