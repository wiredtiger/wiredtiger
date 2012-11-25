/*-
 * Copyright (c) 2008-2012 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_block_read --
 *	Read filesystem cookie referenced block into a buffer.
 */
int
__wt_block_read(WT_SESSION_IMPL *session,
    WT_BLOCK *block, WT_ITEM *buf, const uint8_t *addr, uint32_t addr_size)
{
	off_t offset;
	uint32_t size, cksum;

	WT_UNUSED(addr_size);

	/* Crack the cookie. */
	WT_RET(__wt_block_buffer_to_addr(block, addr, &offset, &size, &cksum));

	/* Read the block. */
	WT_RET(__wt_block_read_off(session, block, buf, offset, size, cksum));

	return (0);
}

/*
 * __wt_block_read_off --
 *	Read an addr/size pair referenced block into a buffer.
 */
int
__wt_block_read_off(WT_SESSION_IMPL *session,
    WT_BLOCK *block, WT_ITEM *buf, off_t offset, uint32_t size, uint32_t cksum)
{
	WT_BLOCK_HEADER *blk;
	uint32_t alloc_size, page_cksum;

	WT_VERBOSE_RET(session, read,
	    "off %" PRIuMAX ", size %" PRIu32 ", cksum %" PRIu32,
	    (uintmax_t)offset, size, cksum);

#ifdef HAVE_DIAGNOSTIC
	/*
	 * In diagnostic mode, verify the block we're about to read isn't on
	 * either the available or discard lists.
	 *
	 * Don't check during salvage, it's possible we're reading an already
	 * freed overflow page.
	 */
	if (!F_ISSET(session, WT_SESSION_SALVAGE_QUIET_ERR))
		WT_RET(
		    __wt_block_misplaced(session, block, "read", offset, size));
#endif

	/*
	 * Grow the buffer as necessary and read the block.  Buffers should be
	 * aligned for reading, but there are lots of buffers (for example, file
	 * cursors have two buffers each, key and value), and it's difficult to
	 * be sure we've found all of them.  If the buffer isn't aligned, it's
	 * an easy fix: set the flag and guarantee we reallocate it.  (Most of
	 * the time on reads, the buffer memory has not yet been allocated, so
	 * we're not adding any additional processing time.)
	 */
	if (F_ISSET(buf, WT_ITEM_ALIGNED))
		alloc_size = size;
	else {
		F_SET(buf, WT_ITEM_ALIGNED);
		alloc_size = (uint32_t)WT_MAX(size, buf->memsize + 10);
	}
	WT_RET(__wt_buf_init(session, buf, alloc_size));
	WT_RET(__wt_read(session, block->fh, offset, size, buf->mem));
	buf->size = size;

	/* Validate the checksum. */
	blk = WT_BLOCK_HEADER_REF(buf->mem);
	blk->cksum = 0;
	page_cksum = __wt_cksum(buf->mem,
	    F_ISSET(blk, WT_BLOCK_DATA_CKSUM) ? size : WT_BLOCK_COMPRESS_SKIP);
	if (cksum != page_cksum) {
		if (!F_ISSET(session, WT_SESSION_SALVAGE_QUIET_ERR))
			__wt_errx(session,
			    "read checksum error [%"
			    PRIu32 "B @ %" PRIuMAX ", %"
			    PRIu32 " != %" PRIu32 "]",
			    size, (uintmax_t)offset, cksum, page_cksum);
		return (WT_ERROR);
	}

	WT_CSTAT_INCR(session, block_read);
	WT_CSTAT_INCRV(session, byte_read, size);

	return (0);
}
