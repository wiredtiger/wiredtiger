/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __block_switch_writeable --
 *     Switch a new writeable object.
 */
static int
__block_switch_writeable(WT_SESSION_IMPL *session, WT_BLOCK *block, uint64_t object_id)
{
    WT_DECL_RET;

    WT_ERR(__wt_close(session, &block->fh));

    WT_ERR(block->opener->open(
      block->opener, session, object_id, WT_FS_OPEN_FILE_TYPE_DATA, block->file_flags, &block->fh));

#if 0
    /*
     * TODO: tiered: examine this prototype code, and see if we should use any of its ideas. It uses
     * a different code path to create a new object file, and sets up a checkpoint entry.
     */

    /* Bump to a new file ID. */
    ++block->objectid;
    WT_ERR(__wt_buf_fmt(session, tmp, "%s.%08" PRIu32, block->name, block->objectid));
    filename = tmp->data;

    WT_WITH_BUCKET_STORAGE(session->bucket_storage, session, {
        ret = __wt_open(session, filename, WT_FS_OPEN_FILE_TYPE_DATA,
          WT_FS_OPEN_CREATE | block->file_flags, &block->fh);
    });
    WT_ERR(ret);
    WT_ERR(__wt_desc_write(session, block->fh, block->allocsize));

    block->size = block->allocsize;
    __wt_block_ckpt_destroy(session, &block->live);
    WT_ERR(__wt_block_ckpt_init(session, &block->live, "live"));
#endif

err:
    return (ret);
}

/*
 * __wt_block_tiered_fh --
 *     Open an object from the shared tier.
 */
int
__wt_block_tiered_fh(WT_SESSION_IMPL *session, WT_BLOCK *block, uint32_t object_id, WT_FH **fhp)
{
    WT_DECL_ITEM(tmp);
    WT_DECL_RET;

    /* TODO: tiered: fh readlock; we may want a reference count on each file handle given out. */

    if (object_id * sizeof(WT_FILE_HANDLE *) < block->ofh_alloc &&
      (*fhp = block->ofh[object_id]) != NULL)
        return (0);

    /* TODO: tiered: fh writelock */
    /* Ensure the array goes far enough. */
    WT_RET(__wt_realloc_def(session, &block->ofh_alloc, object_id + 1, &block->ofh));
    if (object_id >= block->max_objectid)
        block->max_objectid = object_id + 1;
    if ((*fhp = block->ofh[object_id]) != NULL)
        return (0);

    WT_RET(__wt_scr_alloc(session, 0, &tmp));
    WT_ERR(block->opener->open(block->opener, session, object_id, WT_FS_OPEN_FILE_TYPE_DATA,
      WT_FS_OPEN_READONLY | block->file_flags, &block->ofh[object_id]));
    *fhp = block->ofh[object_id];
    WT_ASSERT(session, *fhp != NULL);

err:
    __wt_scr_free(session, &tmp);
    return (ret);
}

/*
 * __wt_block_switch_object --
 *     Modify an object.
 */
int
__wt_block_switch_object(
  WT_SESSION_IMPL *session, WT_BLOCK *block, uint64_t object_id, uint32_t flags)
{
    WT_UNUSED(flags);

    /*
     * Note: the flags will be used in the future to perform various tasks,
     * to efficiently mark objects in transition (that is during a switch):
     *  - mark this file as the writeable file (what currently happens)
     *  - disallow writes to this object (reads still allowed, we're about to switch)
     *  - close this object (about to move it, don't allow reopens yet)
     *  - allow opens on this object again
     */
    return (__block_switch_writeable(session, block, object_id));
}

/*
 * __wt_block_tiered_load --
 *     Set up log-structured processing when loading a new root page.
 */
int
__wt_block_tiered_load(WT_SESSION_IMPL *session, WT_BLOCK *block, WT_BLOCK_CKPT *ci)
{
    /*
     * TODO: tiered: this call currently advances the object id, that's probably not appropriate for
     * readonly opens. Perhaps it's also not appropriate for opening at an older checkpoint?
     */
    if (block->has_objects) {
        block->objectid = ci->root_objectid;

        /* Advance to the next file for future changes. */
        WT_RET(__block_switch_writeable(session, block, ci->root_objectid + 1));
    }
    return (0);
}
