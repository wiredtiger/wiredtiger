/*-
 * Copyright (c) 2014-2019 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * In historic WiredTiger files, it wasn't possible to open standalone files,
 * you're done if you lose the file's associated metadata. That was a mistake
 * and this code is the workaround. Separately we store file creation metadata
 * in the file's descriptor block. The file creation metadata is enough to read
 * a file: it includes allocation size, compression and so on, with it we can
 * open a file and read the blocks. The other thing we need to verify a file is
 * a list of active checkpoints as of the file's clean shutdown (also normally
 * stored in the database metadata). The last write done in a block manager's
 * checkpoint is the avail list. If we include checkpoint information with that
 * write, we're close. We could then open the file, read the blocks, scan until
 * we find an avail list, and read the active checkpoint information from there.
 *	Two problems remain: first, the checkpoint information can't be correct
 * until we write the avail list, the checkpoint information has to include the
 * avail list address plus the final file size after the write. Fortunately,
 * when scanning the file for the avail lists, we're figuring out exactly the
 * information needed to fix up the checkpoint information we wrote, that is,
 * the avail list's offset, size and checksum triplet. As for the final file
 * size, we allocate space in the file before we calculate the block's checksum,
 * so we can do that allocation and then fill in the final file size before
 * writing the block.
 *	The second problem is we have to be able to find the avail lists that
 * include checkpoint information (ignoring previous files created by previous
 * releases, and, of course, making upgrade/downgrade work seamlessly). Extent
 * list are written to their own pages, and we could version this change using
 * the page header version. Extent lists have WT_PAGE_BLOCK_MANAGER page types,
 * we could version this change using the upcoming WT_PAGE_VERSION_TS upgrade.
 * However, that requires waiting a release (we would have to first release a
 * version that ignores those new page header versions so downgrade works), and
 * we're not planning a release that writes WT_PAGE_VERSION_TS page headers for
 * awhile. Happily, historic WiredTiger releases have a bug. Extent lists
 * consist of a set of offset/size pairs, with magic offset/size pairs at the
 * beginning and end of the list. Historic releases only verified the offset of
 * the special pairs, ignoring the size. To detect avail lists that include the
 * checkpoint information, This change adds a version to the extent list: if the
 * size is WT_BLOCK_EXTLIST_VERSION_CKPT, then checkpoint information follows.
 */

/*
 * __wt_block_checkpoint_info_set --
 *	Append the file checkpoint recovery information to the buffer.
 */
int
__wt_block_checkpoint_info_set(WT_SESSION_IMPL *session,
    WT_BLOCK *block, WT_ITEM *buf, uint8_t **file_sizep)
{
	WT_BLOCK_CKPT *ci;
	WT_CKPT *ckpt, *ckptbase;
	WT_DECL_ITEM(tmp);
	WT_DECL_RET;
	size_t align_size, size;
	uint8_t *endp, *p;

	ci = block->final_ci;
	ckpt = block->final_ckpt;
	ckptbase = block->final_ckptbase;
	p = (uint8_t *)buf->mem + buf->size;

	/*
	 * First, add in a counter to uniquely order checkpoints at our level.
	 * While there's time information in the checkpoint itself, it's only
	 * at the granularity of a second.
	 */
	size = buf->size + WT_INTPACK64_MAXSIZE;
	WT_RET(__wt_buf_extend(session, buf, size));
	p = (uint8_t *)buf->mem + buf->size;
	WT_RET(__wt_vpack_uint(&p, 0, ++block->final_count));
	buf->size = WT_PTRDIFF(p, buf->mem);

	/*
	 * Second, add space for the final file size as a packed value. We don't
	 * know how large it will be so skip the maximum possible space.
	 */
	size = buf->size + WT_INTPACK64_MAXSIZE;
	WT_RET(__wt_buf_extend(session, buf, size));
	*file_sizep = (uint8_t *)buf->mem + buf->size;
	buf->size = size;

	/*
	 * Third, build the not-quite-right checkpoint information. Not correct
	 * because we haven't written the avail list and so don't know how that
	 * will play out, but it's close enough we can fix it up later.
	 *
	 * It's not necessary, but discard old information for the avail list,
	 * so if there's a bug we'll fail hard, not reading random blocks.
	 */
	ci->avail.offset = 0;
	ci->avail.size = ci->avail.checksum = 0;

	WT_RET(__wt_buf_init(session, &ckpt->raw, WT_BTREE_MAX_ADDR_COOKIE));
	endp = ckpt->raw.mem;
	WT_RET(__wt_block_ckpt_to_buffer(session, block, &endp, ci));
	ckpt->raw.size = WT_PTRDIFF(endp, ckpt->raw.mem);
	WT_RET(__wt_scr_alloc(session, 0, &tmp));
	WT_ERR(__wt_meta_ckptlist_to_meta(session, ckptbase, tmp));

	/* Fourth, copy the checkpoint information into the buffer. */
	size = buf->size + tmp->size;
	WT_ERR(__wt_buf_extend(session, buf, size));
	p = (uint8_t *)buf->mem + buf->size;
	memcpy(p, tmp->data, tmp->size);
	p += tmp->size;
	buf->size = WT_PTRDIFF(p, buf->mem);

	/*
	 * We might have grown the buffer beyond the original allocation size,
	 * make sure that we're still in compliance.
	 */
	align_size = WT_ALIGN(buf->size, block->allocsize);
	if (align_size < buf->memsize)
		WT_ERR(__wt_buf_extend(session, buf, align_size));

err:	__wt_scr_free(session, &tmp);
	return (ret);
}

#define	WT_BLOCK_SKIP(a) do {						\
	if ((a) != 0)							\
		continue;						\
} while (0)

/*
 * __wt_block_checkpoint_info --
 *	Scan a file looking for checkpoints.
 */
int
__wt_block_checkpoint_info(WT_SESSION_IMPL *session, WT_BLOCK *block)
{
	WT_BLOCK_CKPT ci;
	WT_BLOCK_HEADER *blk;
	WT_CKPT *ckpt, *ckptbase;
	WT_DECL_ITEM(tmp);
	WT_DECL_RET;
	WT_FH *fh;
	const WT_PAGE_HEADER *dsk;
	wt_off_t ext_off, ext_size, offset;
	uint64_t live_counter, nblocks;
	uint32_t allocsize, checksum, size;
	uint8_t *endp;
	const uint8_t *p, *t;
	const char *name;
	struct {
		uint64_t live_counter;
		uint64_t file_size;
		wt_off_t offset;
		uint32_t checksum, size;
		char *metadata;
	} saved;

	ckptbase = NULL;
	name = session->dhandle->name;
	memset(&saved, 0, sizeof(saved));

	ext_off = 0;			/* [-Werror=maybe-uninitialized] */
	ext_size = 0;
	live_counter = 0;

	WT_RET(__wt_scr_alloc(session, 64 * 1024, &tmp));

	/*
	 * Scan the file, starting after the descriptor block, looking for
	 * pages.
	 */
	fh = block->fh;
	allocsize = block->allocsize;
	for (nblocks = 0,
	    offset = allocsize; offset < block->size; offset += size) {
		/* Report progress occasionally. */
#define	WT_CHECKPOINT_LIST_PROGRESS_INTERVAL	100
		if (++nblocks % WT_CHECKPOINT_LIST_PROGRESS_INTERVAL == 0)
			WT_ERR(__wt_progress(session, NULL, nblocks));

		/*
		 * Read the start of a possible page (an allocation-size block),
		 * and get a block length from it. Move to the next allocation
		 * sized boundary, we'll never consider this one again.
		 */
		size = allocsize;
		if ((ret = __wt_read(
		    session, fh, offset, (size_t)allocsize, tmp->mem)) != 0)
			break;
		blk = WT_BLOCK_HEADER_REF(tmp->mem);
		__wt_block_header_byteswap(blk);
		checksum = blk->checksum;

		/*
		 * Check the block size: if it's not insane, read the block.
		 * Reading the block validates any checksum. The file might
		 * reasonably have garbage at the end, and we're not here to
		 * detect that. Ignore problems, subsequent file verification
		 * can deal with any corruption.
		 */
		if (__wt_block_offset_invalid(block, offset, size) ||
		    __wt_block_read_off(
		    session, block, tmp, offset, size, checksum) != 0)
			continue;

		/* Block successfully read, skip it. */
		size = blk->disk_size;

		dsk = tmp->mem;
		if (dsk->type != WT_PAGE_BLOCK_MANAGER)
			continue;

		p = WT_BLOCK_HEADER_BYTE(tmp->mem);
		WT_BLOCK_SKIP(__wt_extlist_read_pair(&p, &ext_off, &ext_size));
		if (ext_off != WT_BLOCK_EXTLIST_MAGIC || ext_size != 0)
			continue;
		for (;;) {
			if ((ret = __wt_extlist_read_pair(
			    &p, &ext_off, &ext_size)) != 0)
				break;
			if (ext_off == WT_BLOCK_INVALID_OFFSET)
				break;
		}
		if (ret != 0) {
			ret = 0;
			continue;
		}
		if (ext_size < WT_BLOCK_EXTLIST_VERSION_CKPT)
			continue;

		/*
		 * Skip any entries that aren't the most recent we've seen so
		 * far.
		 */
		WT_BLOCK_SKIP(__wt_vunpack_uint(&p, 0, &live_counter));
		if (live_counter < saved.live_counter)
			continue;

		__wt_verbose(session, WT_VERB_VERIFY,
		    "block scan: checkpoint block #%" PRIu64 " at %" PRIuMAX,
		    live_counter, (uintmax_t)offset);

		saved.live_counter = live_counter;

		/*
		 * The file size is in a fixed-size chunk of data, although it's
		 * packed (for portability).
		 */
		t = p;
		WT_BLOCK_SKIP(__wt_vunpack_uint(&t, 0, &saved.file_size));
		p += WT_INTPACK64_MAXSIZE;

		saved.offset = offset;
		saved.size = size;
		saved.checksum = checksum;

		__wt_free(session, saved.metadata);
		WT_ERR(__wt_strdup(session, (const char *)p, &saved.metadata));
	}

	if (saved.metadata == NULL)
		WT_ERR_MSG(session, WT_NOTFOUND,
		    "%s: no checkpoint information found during file scan",
		    block->name);

	/*
	 * We have the metadata from immediately before the block manager wrote
	 * the checkpoint. The only thing that's missing is the avail extent
	 * list. We just read that, so we have all of the necessary information.
	 *
	 * Updating the checkpoint information isn't pretty: (1) we have the
	 * metadata string from before the checkpoint, update the metadata;
	 * (2) get the checkpoint information from the metadata as a WT_CKPT
	 * list; (3) convert the next-to-last WT_CKPT structure's checkpoint
	 * information into a WT_BLOCK_CKPT; (4) update the WT_BLOCK_CKPT with
	 * the avail list's information; (5) update the metadata.
	 *
	 * Note that we're updating the next-to-last entry. Entries are sorted
	 * in creation order, so that's always going to be the entry to update
	 * (the last entry is the entry in case we're adding a new checkpoint).
	 */
	WT_ERR(__wt_meta_checkpoint_set(session, name, saved.metadata));
	WT_ERR(__wt_meta_ckptlist_get(session, name, &ckptbase));
	WT_CKPT_FOREACH(ckptbase, ckpt)
		if ((ckpt + 1)->name == NULL)
			break;
	WT_ERR(__wt_block_buffer_to_ckpt(session, block, ckpt->raw.data, &ci));
	ci.avail.offset = saved.offset;
	ci.avail.size = saved.size;
	ci.avail.checksum = saved.checksum;
	ci.file_size = (wt_off_t)saved.file_size;
	endp = ckpt->raw.mem;
	WT_ERR(__wt_block_ckpt_to_buffer(session, block, &endp, &ci));
	ckpt->raw.size = WT_PTRDIFF(endp, ckpt->raw.mem);
	F_SET(ckpt, WT_CKPT_UPDATE);
	WT_ERR(__wt_meta_ckptlist_set(session, name, ckptbase, NULL));

err:	__wt_meta_ckptlist_free(session, &ckptbase);
	__wt_scr_free(session, &tmp);
	__wt_free(session, saved.metadata);

	return (ret);
}
