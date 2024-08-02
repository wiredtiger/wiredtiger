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
    WT_DECL_ITEM(tmp);
    WT_DECL_ITEM(tmp2);
    char *value;
    const char *uri;
    int ret;

    WT_UNUSED(buf);
    WT_UNUSED(ckptbase);
    WT_UNUSED(data_checksum);

    block_pantry = (WT_BLOCK_PANTRY *)bm->block;
    handle = block_pantry->fh->handle;
    WT_RET(__wt_scr_alloc(session, 4096, &tmp));
    WT_RET(__wt_scr_alloc(session, 4096, &tmp2));

    WT_RET(__wt_buf_fmt(session, tmp, "file:%s", &handle->name[2])); /* TODO less hacky way to get URI */
    uri = tmp->data;
    ret = __wt_metadata_search(session, uri, &value);

    WT_RET(__wt_buf_fmt(session, tmp2, "%s\n%s\n", uri, value)); /* TODO less hacky way to get URI */

    WT_RET(handle->fh_obj_checkpoint(handle, &session->iface, tmp2));

    /* TODO WT_ERR etc otherwise this leaks on error */
    __wt_scr_free(session, &tmp);
    __wt_scr_free(session, &tmp2);

    return (0);
}
