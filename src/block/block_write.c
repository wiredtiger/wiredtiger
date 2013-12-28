/*-
 * Copyright (c) 2008-2013 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_block_header --
 *	Return the size of the block-specific header.
 */
u_int
__wt_block_header(WT_BLOCK *block)
{
	WT_UNUSED(block);

	return ((u_int)WT_BLOCK_HEADER_SIZE);
}

/*
 * __wt_block_write_size --
 *	Return the buffer size required to write a block.
 */
int
__wt_block_write_size(WT_SESSION_IMPL *session, WT_BLOCK *block, size_t *sizep)
{
	WT_UNUSED(session);

	/*
	 * We write the page size, in bytes, into the block's header as a 4B
	 * unsigned value, and it's possible for the engine to accept an item
	 * we can't write.  For example, a huge key/value where the allocation
	 * size has been set to something large will overflow 4B when it tries
	 * to align the write.  We could make this work (for example, writing
	 * the page size in units of allocation size or something else), but
	 * it's not worth the effort, writing 4GB objects into a btree makes
	 * no sense.  Limit the writes to (4GB - 1KB), it gives us potential
	 * mode bits, and I'm not interested in debugging corner cases anyway.
	 */
	*sizep = (size_t)
	    WT_ALIGN(*sizep + WT_BLOCK_HEADER_BYTE_SIZE, block->allocsize);
	return (*sizep > UINT32_MAX - 1024 ? EINVAL : 0);
}

/*
 * __wt_block_write --
 *	Write a buffer into a block, returning the block's address cookie.
 */
int
__wt_block_write(WT_SESSION_IMPL *session, WT_BLOCK *block,
    WT_ITEM *buf, uint8_t *addr, size_t *addr_sizep, int data_cksum)
{
	off_t offset;
	uint32_t size, cksum;
	uint8_t *endp;

	WT_RET(__wt_block_write_off(
	    session, block, buf, &offset, &size, &cksum, data_cksum, 0));

	endp = addr;
	WT_RET(__wt_block_addr_to_buffer(block, &endp, offset, size, cksum));
	*addr_sizep = WT_PTRDIFF(endp, addr);

	return (0);
}

/*
 * __wt_block_write_off --
 *	Write a buffer into a block, returning the block's offset, size and
 * checksum.
 */
int
__wt_block_write_off(WT_SESSION_IMPL *session, WT_BLOCK *block,
    WT_ITEM *buf, off_t *offsetp, uint32_t *sizep, uint32_t *cksump,
    int data_cksum, int locked)
{
	WT_BLOCK_HEADER *blk;
	WT_DECL_RET;
	WT_FH *fh;
	size_t align_size;
	off_t offset;

	blk = WT_BLOCK_HEADER_REF(buf->mem);
	fh = block->fh;

	/* Buffers should be aligned for writing. */
	if (!F_ISSET(buf, WT_ITEM_ALIGNED)) {
		WT_ASSERT(session, F_ISSET(buf, WT_ITEM_ALIGNED));
		WT_RET_MSG(session, EINVAL,
		    "direct I/O check: write buffer incorrectly allocated");
	}

	/*
	 * Align the size to an allocation unit.
	 *
	 * The buffer must be big enough for us to zero to the next allocsize
	 * boundary, this is one of the reasons the btree layer must find out
	 * from the block-manager layer the maximum size of the eventual write.
	 */
	align_size = WT_ALIGN(buf->size, block->allocsize);
	if (align_size > buf->memsize) {
		WT_ASSERT(session, align_size <= buf->memsize);
		WT_RET_MSG(session, EINVAL,
		    "buffer size check: write buffer incorrectly allocated");
	}
	if (align_size > UINT32_MAX) {
		WT_ASSERT(session, align_size <= UINT32_MAX);
		WT_RET_MSG(session, EINVAL,
		    "buffer size check: write buffer too large to write");
	}

	/* Zero out any unused bytes at the end of the buffer. */
	memset((uint8_t *)buf->mem + buf->size, 0, align_size - buf->size);

	/*
	 * Set the disk size so we don't have to incrementally read blocks
	 * during salvage.
	 */
	blk->disk_size = WT_STORE_SIZE(align_size);

	/*
	 * Update the block's checksum: if our caller specifies, checksum the
	 * complete data, otherwise checksum the leading WT_BLOCK_COMPRESS_SKIP
	 * bytes.  The assumption is applications with good compression support
	 * turn off checksums and assume corrupted blocks won't decompress
	 * correctly.  However, if compression failed to shrink the block, the
	 * block wasn't compressed, in which case our caller will tell us to
	 * checksum the data to detect corruption.   If compression succeeded,
	 * we still need to checksum the first WT_BLOCK_COMPRESS_SKIP bytes
	 * because they're not compressed, both to give salvage a quick test
	 * of whether a block is useful and to give us a test so we don't lose
	 * the first WT_BLOCK_COMPRESS_SKIP bytes without noticing.
	 */
	blk->flags = 0;
	if (data_cksum)
		F_SET(blk, WT_BLOCK_DATA_CKSUM);
	blk->cksum = 0;
	blk->cksum = __wt_cksum(
	    buf->mem, data_cksum ? align_size : WT_BLOCK_COMPRESS_SKIP);

	if (!locked) {
		WT_RET(__wt_block_ext_prealloc(session, 5));
		__wt_spin_lock(session, &block->live_lock);
	}
	ret = __wt_block_alloc(session, block, &offset, (off_t)align_size);
	if (!locked)
		__wt_spin_unlock(session, &block->live_lock);
	WT_RET(ret);

#if defined(HAVE_POSIX_FALLOCATE) || defined(HAVE_FTRUNCATE)
	/*
	 * Extend the file in chunks.  We aren't holding a lock and we'd prefer
	 * to limit the number of threads extending the file at the same time,
	 * so choose the one thread that's crossing the extended boundary.  We
	 * don't extend newly created files, and it's theoretically possible we
	 * might wait so long our extension of the file is passed by another
	 * thread writing single blocks, that's why there's a check in case the
	 * extended file size becomes too small: if the file size catches up,
	 * every thread will try to extend it.
	 */
	if (fh->extend_len != 0 &&
	    (fh->extend_size <= fh->size ||
	    (offset + fh->extend_len <= fh->extend_size &&
	    offset + fh->extend_len + (off_t)align_size >= fh->extend_size))) {
		fh->extend_size = offset + fh->extend_len * 2;
		WT_RET(__wt_fallocate(session, fh, offset, fh->extend_len * 2));
	}
#endif
	if ((ret =
	    __wt_write(session, fh, offset, align_size, buf->mem)) != 0) {
		if (!locked)
			__wt_spin_lock(session, &block->live_lock);
		WT_TRET(__wt_block_off_free(
		    session, block, offset, (off_t)align_size));
		if (!locked)
			__wt_spin_unlock(session, &block->live_lock);
		WT_RET(ret);
	}

#ifdef HAVE_SYNC_FILE_RANGE
	/*
	 * Optionally schedule writes for dirty pages in the system buffer
	 * cache.
	 */
	if (block->os_cache_dirty_max != 0 &&
	    (block->os_cache_dirty += align_size) > block->os_cache_dirty_max) {
		block->os_cache_dirty = 0;
		if ((ret = sync_file_range(fh->fd,
		    (off64_t)0, (off64_t)0, SYNC_FILE_RANGE_WRITE)) != 0)
			WT_RET_MSG(
			    session, ret, "%s: sync_file_range", block->name);
	}
#endif
#ifdef HAVE_POSIX_FADVISE
	/* Optionally discard blocks from the system buffer cache. */
	if (block->os_cache_max != 0 &&
	    (block->os_cache += align_size) > block->os_cache_max) {
		block->os_cache = 0;
		if ((ret = posix_fadvise(fh->fd,
		    (off_t)0, (off_t)0, POSIX_FADV_DONTNEED)) != 0)
			WT_RET_MSG(
			    session, ret, "%s: posix_fadvise", block->name);
	}
#endif
	WT_STAT_FAST_CONN_INCR(session, block_write);
	WT_STAT_FAST_CONN_INCRV(session, block_byte_write, align_size);

	WT_VERBOSE_RET(session, write,
	    "off %" PRIuMAX ", size %" PRIuMAX ", cksum %" PRIu32,
	    (uintmax_t)offset, (uintmax_t)align_size, blk->cksum);

	*offsetp = offset;
	*sizep = WT_STORE_SIZE(align_size);
	*cksump = blk->cksum;

	return (ret);
}
