/*-
 * Copyright (c) 2008-2013 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

static int __desc_read(WT_SESSION_IMPL *, WT_BLOCK *);

/*
 * __wt_block_manager_truncate --
 *	Truncate a file.
 */
int
__wt_block_manager_truncate(
    WT_SESSION_IMPL *session, const char *filename, uint32_t allocsize)
{
	WT_DECL_RET;
	WT_FH *fh;

	/* Open the underlying file handle. */
	WT_RET(__wt_open(session, filename, 0, 0, 1, &fh));

	/* Truncate the file. */
	WT_ERR(__wt_ftruncate(session, fh, (off_t)0));

	/* Write out the file's meta-data. */
	ret = __wt_desc_init(session, fh, allocsize);

	/* Close the file handle. */
err:	WT_TRET(__wt_close(session, fh));

	return (ret);
}

/*
 * __wt_block_manager_create --
 *	Create a file.
 */
int
__wt_block_manager_create(
    WT_SESSION_IMPL *session, const char *filename, uint32_t allocsize)
{
	WT_DECL_RET;
	WT_FH *fh;

	/* Create the underlying file and open a handle. */
	WT_RET(__wt_open(session, filename, 1, 1, 1, &fh));

	/* Write out the file's meta-data. */
	ret = __wt_desc_init(session, fh, allocsize);

	/* Close the file handle. */
	WT_TRET(__wt_close(session, fh));

	/* Undo any create on error. */
	if (ret != 0)
		WT_TRET(__wt_remove(session, filename));

	return (ret);
}

/*
 * __block_destroy --
 *	Destroy a block handle.
 */
static int
__block_destroy(WT_SESSION_IMPL *session, WT_BLOCK *block)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;

	conn = S2C(session);
	TAILQ_REMOVE(&conn->blockqh, block, q);

	if (block->name != NULL)
		__wt_free(session, block->name);

	if (block->fh != NULL)
		WT_TRET(__wt_close(session, block->fh));

	__wt_spin_destroy(session, &block->live_lock);

	__wt_block_ext_cleanup(session, block);

	__wt_overwrite_and_free(session, block);

	return (ret);
}

/*
 * __wt_block_open --
 *	Open a block handle.
 */
int
__wt_block_open(WT_SESSION_IMPL *session, const char *filename,
    const char *cfg[], int forced_salvage, WT_BLOCK **blockp)
{
	WT_BLOCK *block;
	WT_CONFIG_ITEM cval;
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;

	WT_UNUSED(cfg);
	WT_VERBOSE_TRET(session, block, "open: %s", filename);

	conn = S2C(session);
	*blockp = NULL;

	__wt_spin_lock(session, &conn->block_lock);
	TAILQ_FOREACH(block, &conn->blockqh, q)
		if (strcmp(filename, block->name) == 0) {
			++block->ref;
			*blockp = block;
			__wt_spin_unlock(session, &conn->block_lock);
			return (0);
		}

	/* Basic structure allocation, initialization. */
	WT_ERR(__wt_calloc_def(session, 1, &block));
	block->ref = 1;
	TAILQ_INSERT_HEAD(&conn->blockqh, block, q);

	WT_ERR(__wt_strdup(session, filename, &block->name));

	/* Get the allocation size. */
	WT_ERR(__wt_config_gets(session, cfg, "allocation_size", &cval));
	block->allocsize = (uint32_t)cval.val;

	/* Configuration: optional OS buffer cache maximum size. */
	WT_ERR(__wt_config_gets(session, cfg, "os_cache_max", &cval));
	block->os_cache_max = cval.val;
#ifdef HAVE_POSIX_FADVISE
	if (conn->direct_io && block->os_cache_max)
		WT_ERR_MSG(session, EINVAL,
		    "os_cache_max not supported in combination with direct_io");
#else
	if (block->os_cache_max)
		WT_ERR_MSG(session, EINVAL,
		    "os_cache_max not supported if posix_fadvise not "
		    "available");
#endif

	/* Configuration: optional immediate write scheduling flag. */
	WT_ERR(__wt_config_gets(session, cfg, "os_cache_dirty_max", &cval));
	block->os_cache_dirty_max = cval.val;
#ifdef HAVE_SYNC_FILE_RANGE
	if (conn->direct_io && block->os_cache_dirty_max)
		WT_ERR_MSG(session, EINVAL,
		    "os_cache_dirty_max not supported in combination with "
		    "direct_io");
#else
	if (block->os_cache_dirty_max)
		WT_ERR_MSG(session, EINVAL,
		    "os_cache_dirty_max not supported if sync_file_range not "
		    "available");
#endif

	/* Open the underlying file handle. */
	WT_ERR(__wt_open(session, filename, 0, 0, 1, &block->fh));

	/* Initialize the live checkpoint's lock. */
	__wt_spin_init(session, &block->live_lock);

	/*
	 * Read the description information from the first block.
	 *
	 * Salvage is a special case: if we're forcing the salvage, we don't
	 * look at anything, including the description information.
	 */
	if (!forced_salvage)
		WT_ERR(__desc_read(session, block));

	*blockp = block;
	__wt_spin_unlock(session, &conn->block_lock);
	return (0);

err:	WT_TRET(__block_destroy(session, block));
	__wt_spin_unlock(session, &conn->block_lock);
	return (ret);
}

/*
 * __wt_block_close --
 *	Close a block handle.
 */
int
__wt_block_close(WT_SESSION_IMPL *session, WT_BLOCK *block)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;

	if (block == NULL)				/* Safety check */
		return (0);

	conn = S2C(session);

	WT_VERBOSE_TRET(session,
	    block, "close: %s", block->name == NULL ? "" : block->name );

	__wt_spin_lock(session, &conn->block_lock);

			/* Reference count is initialized to 1. */
	if (block->ref == 0 || --block->ref == 0)
		WT_TRET(__block_destroy(session, block));

	__wt_spin_unlock(session, &conn->block_lock);

	return (ret);
}

/*
 * __wt_desc_init --
 *	Write a file's initial descriptor structure.
 */
int
__wt_desc_init(WT_SESSION_IMPL *session, WT_FH *fh, uint32_t allocsize)
{
	WT_BLOCK_DESC *desc;
	WT_DECL_ITEM(buf);
	WT_DECL_RET;

	/* Use a scratch buffer to get correct alignment for direct I/O. */
	WT_RET(__wt_scr_alloc(session, allocsize, &buf));
	memset(buf->mem, 0, allocsize);

	desc = buf->mem;
	desc->magic = WT_BLOCK_MAGIC;
	desc->majorv = WT_BLOCK_MAJOR_VERSION;
	desc->minorv = WT_BLOCK_MINOR_VERSION;

	/* Update the checksum. */
	desc->cksum = 0;
	desc->cksum = __wt_cksum(desc, allocsize);

	ret = __wt_write(session, fh, (off_t)0, allocsize, desc);

	__wt_scr_free(&buf);
	return (ret);
}

/*
 * __desc_read --
 *	Read and verify the file's metadata.
 */
static int
__desc_read(WT_SESSION_IMPL *session, WT_BLOCK *block)
{
	WT_BLOCK_DESC *desc;
	WT_DECL_ITEM(buf);
	WT_DECL_RET;
	uint32_t cksum;

	/* Use a scratch buffer to get correct alignment for direct I/O. */
	WT_RET(__wt_scr_alloc(session, block->allocsize, &buf));

	/* Read the first allocation-sized block and verify the file format. */
	WT_ERR(__wt_read(
	    session, block->fh, (off_t)0, block->allocsize, buf->mem));

	desc = buf->mem;
	WT_VERBOSE_ERR(session, block,
	    "%s: magic %" PRIu32
	    ", major/minor: %" PRIu32 "/%" PRIu32
	    ", checksum %#" PRIx32,
	    block->name, desc->magic,
	    desc->majorv, desc->minorv,
	    desc->cksum);

	/*
	 * We fail the open if the checksum fails, or the magic number is wrong
	 * or the major/minor numbers are unsupported for this version.  This
	 * test is done even if the caller is verifying or salvaging the file:
	 * it makes sense for verify, and for salvage we don't overwrite files
	 * without some reason to believe they are WiredTiger files.  The user
	 * may have entered the wrong file name, and is now frantically pounding
	 * their interrupt key.
	 */
	cksum = desc->cksum;
	desc->cksum = 0;
	if (desc->magic != WT_BLOCK_MAGIC ||
	    cksum != __wt_cksum(desc, block->allocsize))
		WT_ERR_MSG(session, WT_ERROR,
		    "%s does not appear to be a WiredTiger file", block->name);

	if (desc->majorv > WT_BLOCK_MAJOR_VERSION ||
	    (desc->majorv == WT_BLOCK_MAJOR_VERSION &&
	    desc->minorv > WT_BLOCK_MINOR_VERSION))
		WT_ERR_MSG(session, WT_ERROR,
		    "unsupported WiredTiger file version: this build only "
		    "supports major/minor versions up to %d/%d, and the file "
		    "is version %d/%d",
		    WT_BLOCK_MAJOR_VERSION, WT_BLOCK_MINOR_VERSION,
		    desc->majorv, desc->minorv);

err:	__wt_scr_free(&buf);
	return (ret);
}

/*
 * __wt_block_stat --
 *	Block statistics
 */
void
__wt_block_stat(WT_SESSION_IMPL *session, WT_BLOCK *block, WT_DSRC_STATS *stats)
{
	/*
	 * We're looking inside the live system's structure, which normally
	 * requires locking: the chances of a corrupted read are probably
	 * non-existent, and it's statistics information regardless, but it
	 * isn't like this is a common function for an application to call.
	 */
	__wt_spin_lock(session, &block->live_lock);
	WT_STAT_SET(session, stats, block_allocsize, block->allocsize);
	WT_STAT_SET(
	    session, stats, block_checkpoint_size, block->live.ckpt_size);
	WT_STAT_SET(session, stats, block_magic, WT_BLOCK_MAGIC);
	WT_STAT_SET(session, stats, block_major, WT_BLOCK_MAJOR_VERSION);
	WT_STAT_SET(session, stats, block_minor, WT_BLOCK_MINOR_VERSION);
	WT_STAT_SET(session, stats, block_size, block->fh->file_size);
	__wt_spin_unlock(session, &block->live_lock);
}
