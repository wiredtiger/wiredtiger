/*-
 * Copyright (c) 2014-2019 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_block_checkpoint_rewrite --
 *	Return the block/size pairs required to upgrade a file from one
 * checkpoint to subsequent one.
 */
int
__wt_block_checkpoint_rewrite(WT_SESSION_IMPL *session,
    WT_BLOCK *block, WT_CKPT *ckptbase, uint64_t **listp, uint64_t *list_countp)
{
	WT_BLOCK_CKPT *a, *b;
	WT_CKPT *ckpt;
	WT_EXT *ext;
	uint64_t *list;
	bool start, stop;

	a = NULL;
	start = stop = false;
	WT_CKPT_FOREACH(ckptbase, ckpt) {
		if (F_ISSET(ckpt, WT_CKPT_FAKE))
			continue;

		/*
		 * Find the starting checkpoint. We don't care about its blocks,
		 * the backup already has those.
		 */
		if (F_ISSET(ckpt, WT_CKPT_INCR_START)) {
			start = true;
			continue;
		}
		if (!start)
			continue;

		WT_RET(__wt_ckpt_extlist_read(session, block, ckpt, false));
		if (a == NULL)
			a = ckpt->bpriv;
		else {
			/*
			 * Once we've started, continue reading each allocation
			 * extent, aggregating them as we go.
			 */
			b = ckpt->bpriv;
			if (b->alloc.entries != 0)
				WT_RET(__wt_block_extlist_merge(
				    session, block, &a->alloc, &b->alloc));
			__wt_block_ckpt_destroy(session, a);
			a = b;
		}

		if (a->alloc.offset != WT_BLOCK_INVALID_OFFSET)
			WT_RET(__wt_block_insert_ext(session, block,
			    &a->alloc, a->alloc.offset, a->alloc.size));
		if (a->discard.offset != WT_BLOCK_INVALID_OFFSET)
			WT_RET(__wt_block_insert_ext(session, block,
			    &a->alloc, a->discard.offset, a->discard.size));
		if (a->avail.offset != WT_BLOCK_INVALID_OFFSET)
			WT_RET(__wt_block_insert_ext(session, block,
			    &a->alloc, a->avail.offset, a->avail.size));

		if (F_ISSET(ckpt, WT_CKPT_INCR_STOP)) {
			stop = true;
			break;
		}
	}

	if (!start || !stop)
		WT_RET_MSG(session, EINVAL,
		    "missing or unmatched start/stop checkpoints specified");

	WT_RET(__wt_calloc_def(session, a->alloc.entries * 2, &list));
	*listp = list;
	*list_countp = a->alloc.entries * 2;

	WT_EXT_FOREACH(ext, a->alloc.off) {
		*list++ = (uint64_t)ext->off;
		*list++ = (uint64_t)ext->size;
	}

	__wt_block_ckpt_destroy(session, a);

	return (0);
}
