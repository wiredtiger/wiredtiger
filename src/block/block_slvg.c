/*-
 * Copyright (c) 2008-2012 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_block_salvage_start --
 *	Start a file salvage.
 */
int
__wt_block_salvage_start(WT_SESSION_IMPL *session, WT_BLOCK *block)
{
	off_t len;
	uint32_t allocsize;

	/* Reset the description sector. */
	WT_RET(__wt_desc_init(session, block->fh));

	/*
	 * Salvage creates a new checkpoint when it's finished, set up for
	 * rolling an empty file forward.
	 */
	WT_RET(__wt_block_ckpt_init(session, block, &block->live, "live", 1));

	/*
	 * Truncate the file to an initial sector plus N allocation size
	 * units (bytes trailing the last multiple of an allocation size
	 * unit must be garbage, by definition).
	 */
	if (block->fh->file_size > WT_BLOCK_DESC_SECTOR) {
		allocsize = block->allocsize;
		len = block->fh->file_size - WT_BLOCK_DESC_SECTOR;
		len = (len / allocsize) * allocsize;
		len += WT_BLOCK_DESC_SECTOR;
		if (len != block->fh->file_size)
			WT_RET(__wt_ftruncate(session, block->fh, len));
	} else
		len = WT_BLOCK_DESC_SECTOR;

	/*
	 * The first sector of the file is the description record, skip it as
	 * we read the file.
	 */
	block->slvg_off = WT_BLOCK_DESC_SECTOR;

	/*
	 * The only checkpoint extent we care about is the allocation list.
	 * Start with the entire file on the allocation list, we'll "free"
	 * any blocks we don't want as we process the file.
	 */
	WT_RET(__wt_block_insert_ext(session, &block->live.alloc,
	    WT_BLOCK_DESC_SECTOR, len - WT_BLOCK_DESC_SECTOR));

	return (0);
}

/*
 * __wt_block_salvage_end --
 *	End a file salvage.
 */
int
__wt_block_salvage_end(WT_SESSION_IMPL *session, WT_BLOCK *block)
{
	/* Discard the checkpoint. */
	return (__wt_block_checkpoint_unload(session, block));
}

/*
 * __wt_block_salvage_next --
 *	Return the address for the next potential block from the file.
 */
int
__wt_block_salvage_next(WT_SESSION_IMPL *session,
    WT_BLOCK *block, uint8_t *addr, uint32_t *addr_sizep, int *eofp)
{
	WT_BLOCK_HEADER *blk;
	WT_DECL_ITEM(tmp);
	WT_DECL_RET;
	WT_FH *fh;
	off_t max, offset;
	uint32_t allocsize, cksum, size;
	uint8_t *endp;

	*eofp = 0;

	fh = block->fh;
	allocsize = block->allocsize;
	WT_ERR(__wt_scr_alloc(session, allocsize, &tmp));

	/* Read through the file, looking for pages. */
	for (max = fh->file_size;;) {
		offset = block->slvg_off;
		if (offset >= max) {			/* Check eof. */
			*eofp = 1;
			goto done;
		}

		/*
		 * Read the start of a possible page (an allocation-size block),
		 * and get a page length from it.  Move to the next allocation
		 * sized boundary, we'll never consider this one again.
		 */
		WT_ERR(__wt_read(session, fh, offset, allocsize, tmp->mem));
		blk = WT_BLOCK_HEADER_REF(tmp->mem);
		block->slvg_off += allocsize;

		/*
		 * The page can't be more than the min/max page size, or past
		 * the end of the file.
		 */
		size = blk->disk_size;
		cksum = blk->cksum;
		if (size == 0 ||
		    size % allocsize != 0 ||
		    size > WT_BTREE_PAGE_SIZE_MAX ||
		    offset + (off_t)size > max)
			goto skip;

		/*
		 * The block size isn't insane, read the entire block.  Reading
		 * the block validates the checksum; if reading the block fails,
		 * ignore it.  If reading the block succeeds, return its address
		 * as a possible page.
		 */
		if (__wt_block_read_off(
		    session, block, tmp, offset, size, cksum) == 0)
			break;

skip:		WT_VERBOSE_ERR(session, salvage,
		    "skipping %" PRIu32 "B at file offset %" PRIuMAX,
		    allocsize, (uintmax_t)offset);

		/* Free the allocation-size block. */
		WT_ERR(__wt_block_off_free(
		    session, block, offset, (off_t)allocsize));
	}

	/* Re-create the address cookie that should reference this block. */
	endp = addr;
	WT_ERR(__wt_block_addr_to_buffer(block, &endp, offset, size, cksum));
	*addr_sizep = WT_PTRDIFF32(endp, addr);

done:
err:	__wt_scr_free(&tmp);
	return (ret);
}

/*
 * __wt_block_salvage_valid --
 *	Inform salvage a block is valid.
 */
int
__wt_block_salvage_valid(WT_SESSION_IMPL *session,
    WT_BLOCK *block, uint8_t *addr, uint32_t addr_size)
{
	off_t offset;
	uint32_t size, cksum;

	WT_UNUSED(session);
	WT_UNUSED(addr_size);

	/*
	 * The upper layer accepted a block we gave it, move past it.
	 *
	 * Crack the cookie.
	 */
	WT_RET(__wt_block_buffer_to_addr(block, addr, &offset, &size, &cksum));
	block->slvg_off = offset + size;

	return (0);
}
