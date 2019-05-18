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
 * and this code is the workaround. First, we store file creation metadata in
 * the file's descriptor block. The file creation metadata is enough to read
 * a file: it includes allocation size, compression, encryptors and so on, with
 * it we can open a file and read the blocks. The other thing we need to verify
 * a file is a list of active checkpoints as of the file's clean shutdown (also
 * normally stored in the database metadata). The last write done in a block
 * manager's checkpoint is the avail list. If we include checkpoint information
 * with that write, we're close. We can then open the file, read the blocks,
 * scan until we find the avail list, and read the active checkpoint information
 * from there.
 *	This is a pretty large violation of layering: the block manager has to
 * match the behavior of the upper layers in creating checkpoint information,
 * and ideally the block manager wouldn't know anything about that. Regardless,
 * it was deemed important enough to be able to crack standalone files that we
 * went in this direction.
 *	Three problems remain: first, the checkpoint information isn't correct
 * until we write the avail list, the checkpoint information has to include the
 * avail list address plus the final file size after the write. Fortunately,
 * when scanning the file for the avail lists, we're figuring out exactly the
 * information needed to fix up the checkpoint information we wrote, that is,
 * the avail list's offset, size and checksum triplet. As for the final file
 * size, we allocate all space in the file before we calculate block checksums,
 * so we can do that space allocation, then fill in the final file size before
 * calculating the checksum and writing the actual block.
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
 * checkpoint information, this change adds a version to the extent list: if the
 * size is WT_BLOCK_EXTLIST_VERSION_CKPT, then checkpoint information follows.
 *	The third problem is that we'd like to have the current file metadata
 * so we have correct app_metadata information, for example. To solve this, the
 * upper layers of the checkpoint code pass down the file's metadata with each
 * checkpoint, and we simply include it in the information we're writing.
 */

/*
 * __wt_block_checkpoint_info_set --
 *	Append the file checkpoint recovery information to a buffer.
 */
int
__wt_block_checkpoint_info_set(WT_SESSION_IMPL *session,
    WT_BLOCK *block, WT_ITEM *buf, uint8_t **file_sizep)
{
	WT_CKPT *ckpt;
	size_t align_size, metadata_len, size;
	uint8_t *p;

	ckpt = block->final_ckpt;
	metadata_len = strlen(ckpt->metadata);
	p = (uint8_t *)buf->mem + buf->size;

	/*
	 * First, add in a counter to uniquely order checkpoints at our level.
	 * There's order and time information in the checkpoint itself, but the
	 * order isn't written and the time is only at second granularity.
	 */
	size = buf->size + WT_INTPACK64_MAXSIZE;
	WT_RET(__wt_buf_extend(session, buf, size));
	p = (uint8_t *)buf->mem + buf->size;
	WT_RET(__wt_vpack_uint(&p, 0, ++block->final_count));
	buf->size = WT_PTRDIFF(p, buf->mem);

	/*
	 * Second, add space for the final file size as a packed value. We don't
	 * know how large it will be so skip the maximum required space.
	 */
	size = buf->size + WT_INTPACK64_MAXSIZE;
	WT_RET(__wt_buf_extend(session, buf, size));
	*file_sizep = (uint8_t *)buf->mem + buf->size;
	buf->size = size;

	/* Third, copy the length of the file metadata into the buffer. */
	size = buf->size + WT_INTPACK64_MAXSIZE;
	WT_RET(__wt_buf_extend(session, buf, size));
	p = (uint8_t *)buf->mem + buf->size;
	WT_RET(__wt_vpack_uint(&p, 0, (uint64_t)metadata_len));
	buf->size = WT_PTRDIFF(p, buf->mem);

	/* Fourth, copy the file metadata into the buffer. */
	size = buf->size + metadata_len;
	WT_RET(__wt_buf_extend(session, buf, size));
	p = (uint8_t *)buf->mem + buf->size;
	memcpy(p, ckpt->metadata, metadata_len);
	buf->size = size;

	/*
	 * Fifth, copy the length of the not-quite-right checkpoint information
	 * into the buffer.
	 */
	size = buf->size + WT_INTPACK64_MAXSIZE;
	WT_RET(__wt_buf_extend(session, buf, size));
	p = (uint8_t *)buf->mem + buf->size;
	WT_RET(__wt_vpack_uint(&p, 0, (uint64_t)ckpt->raw.size));
	buf->size = WT_PTRDIFF(p, buf->mem);

	/*
	 * Sixth, copy the not-quite-right checkpoint information into the
	 * buffer.
	 */
	size = buf->size + ckpt->raw.size;
	WT_RET(__wt_buf_extend(session, buf, size));
	p = (uint8_t *)buf->mem + buf->size;
	memcpy(p, ckpt->raw.data, ckpt->raw.size);
	buf->size = size;

	/*
	 * We might have grown the buffer beyond the original allocation size,
	 * make sure that we're still in compliance.
	 */
	align_size = WT_ALIGN(buf->size, block->allocsize);
	if (align_size < buf->memsize)
		WT_RET(__wt_buf_extend(session, buf, align_size));

	return (0);
}

struct saved_block_info {
	uint64_t live_counter;
	wt_off_t offset;
	uint32_t size;
	uint32_t checksum;
	uint64_t file_size;
	char	*checkpoint;
	char	*metadata;
};

/*
 * __block_metadata_update --
 *	Update the metadata information for the file.
 */
static int
__block_metadata_update(
    WT_SESSION_IMPL *session, const char *name, struct saved_block_info *saved)
{
	WT_DECL_RET;
	const char *filecfg[] = {
	   WT_CONFIG_BASE((WT_SESSION_IMPL *)session, file_meta), NULL, NULL };
	char *fileconf;

	/*
	 * Add the default configuration to the metadata we read, where the
	 * read metadata overrides the defaults, flatten it and insert it.
	 */
	filecfg[1] = saved->metadata;
	WT_RET(__wt_config_collapse(session, filecfg, &fileconf));
	ret = __wt_metadata_insert(session, name, fileconf);
	__wt_free(session, fileconf);
	return (ret);
}

/*
 * __block_checkpoint_update --
 *	Update the checkpoint information for the file.
 */
static int
__block_checkpoint_update(WT_SESSION_IMPL *session,
    WT_BLOCK *block, const char *name,  struct saved_block_info *saved)
{
	WT_BLOCK_CKPT ci;
	WT_CKPT *ckpt, *ckptbase;
	WT_DECL_RET;
	uint8_t *endp;

	ckptbase = NULL;

	/*
	 * We have the checkpoint information from immediately before the final
	 * checkpoint (we just updated the database's file metadata), we read
	 * the final checkpoint data blob (except for avail list information),
	 * from the file, and we have the avail list information from scanning
	 * the file. Put it all together.
	 *
	 * Get the checkpoint information from the file's metadata as an array
	 * of WT_CKPT structures. We're going to add a new entry for the final
	 * checkpoint at the end, move to that entry.
	 */
	WT_ERR(__wt_meta_ckptlist_get(session, name, true, &ckptbase));
	WT_CKPT_FOREACH(ckptbase, ckpt)
		if (ckpt->name == NULL)
			break;

	/*
	 * Convert the final checkpoint data blob to a WT_BLOCK_CKPT structure,
	 * update it with the avail list information, and convert it back to a
	 * data blob.
	 */
	WT_ERR(__wt_block_buffer_to_ckpt(
	    session, block, (uint8_t *)saved->checkpoint, &ci));
	ci.avail.offset = saved->offset;
	ci.avail.size = saved->size;
	ci.avail.checksum = saved->checksum;
	ci.file_size = (wt_off_t)saved->file_size;
	WT_ERR(__wt_buf_extend(
	    session, &ckpt->raw, WT_BLOCK_CHECKPOINT_BUFFER));
	endp = ckpt->raw.mem;
	WT_ERR(__wt_block_ckpt_to_buffer(session, block, &endp, &ci, false));
	ckpt->raw.size = WT_PTRDIFF(endp, ckpt->raw.mem);

	if (WT_VERBOSE_ISSET(session, WT_VERB_CHECKPOINT))
		__wt_ckpt_verbose(session, block, "scan", NULL, ckpt->raw.data);

	/* Update the file's metadata with the new checkpoint information. */
	WT_ERR(__wt_meta_ckptlist_set(session, name, ckptbase, NULL));

err:	__wt_meta_ckptlist_free(session, &ckptbase);
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
	struct saved_block_info saved;
	WT_BLOCK_HEADER *blk;
	WT_DECL_ITEM(tmp);
	WT_DECL_RET;
	WT_FH *fh;
	const WT_PAGE_HEADER *dsk;
	wt_off_t ext_off, ext_size, offset;
	uint64_t len, live_counter, nblocks;
	uint32_t allocsize, checksum, size;
	const uint8_t *p, *t;
	const char *name;

	memset(&saved, 0, sizeof(saved));
	name = session->dhandle->name;

	ext_off = 0;			/* [-Werror=maybe-uninitialized] */
	ext_size = 0;
	live_counter = 0;

	F_SET(session, WT_SESSION_QUIET_CORRUPT_FILE);

	WT_ERR(__wt_scr_alloc(session, 64 * 1024, &tmp));

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
		if ((ret = __wt_read(
		    session, fh, offset, (size_t)allocsize, tmp->mem)) != 0)
			break;
		blk = WT_BLOCK_HEADER_REF(tmp->mem);
		__wt_block_header_byteswap(blk);
		size = blk->disk_size;
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
		    session, block, tmp, offset, size, checksum) != 0) {
			size = allocsize;
			continue;
		}

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
		/*
		 * Check less-than, that way we can extend this with additional
		 * values in the future.
		 */
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
		saved.offset = offset;
		saved.size = size;
		saved.checksum = checksum;

		/*
		 * The file size is in a fixed-size chunk of data, although it's
		 * packed (for portability).
		 */
		t = p;
		WT_BLOCK_SKIP(__wt_vunpack_uint(&t, 0, &saved.file_size));
		p += WT_INTPACK64_MAXSIZE;

		/* Save a copy of the metadata information. */
		__wt_free(session, saved.metadata);
		WT_BLOCK_SKIP(__wt_vunpack_uint(&p, 0, &len));
		WT_ERR(__wt_strndup(
		    session, (const char *)p, (size_t)len, &saved.metadata));
		p += len;

		/* Save a copy of the checkpoint information. */
		__wt_free(session, saved.checkpoint);
		WT_BLOCK_SKIP(__wt_vunpack_uint(&p, 0, &len));
		WT_ERR(__wt_strndup(
		    session, (const char *)p, (size_t)len, &saved.checkpoint));
	}

	if (saved.checkpoint == NULL)
		WT_ERR_MSG(session, WT_NOTFOUND,
		    "%s: no saved metadata information found during file scan",
		    block->name);

	/* We have the metadata information, update the database. */
	WT_ERR(__block_metadata_update(session, name, &saved));

	/* We have the checkpoint information, update the database. */
	WT_ERR(__block_checkpoint_update(session, block, name, &saved));

err:
	__wt_free(session, saved.checkpoint);
	__wt_free(session, saved.metadata);
	__wt_scr_free(session, &tmp);

	F_CLR(session, WT_SESSION_QUIET_CORRUPT_FILE);
	return (ret);
}
