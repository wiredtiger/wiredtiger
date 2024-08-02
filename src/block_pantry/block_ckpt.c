/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_bmp_checkpoint --
 *     Write a buffer into a block, creating a checkpoint.
 */
int
__wt_bmp_checkpoint(
  WT_BM *bm, WT_SESSION_IMPL *session, WT_ITEM *buf, WT_CKPT *ckptbase, bool data_checksum)
{
    WT_BLOCK_PANTRY *block_pantry;
    WT_FILE_HANDLE *handle;

    WT_UNUSED(buf);
    WT_UNUSED(ckptbase);
    WT_UNUSED(data_checksum);

    block_pantry = (WT_BLOCK_PANTRY *)bm->block;
    handle = block_pantry->fh->handle;

    WT_RET(handle->fh_obj_checkpoint(handle, &session->iface, NULL));

    return (0);
}
