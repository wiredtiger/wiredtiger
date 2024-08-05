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

/*
 * __wt_bmp_checkpoint_load --
 *     Load a checkpoint.
 */
int
__wt_bmp_checkpoint_load(WT_BM *bm, WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size,
  uint8_t *root_addr, size_t *root_addr_sizep, bool checkpoint)
{
    WT_BLOCK_PANTRY *block_pantry;
    WT_FILE_HANDLE *handle;
    char buf[4096], *key, *value;
    int i;

    WT_UNUSED(addr);
    WT_UNUSED(addr_size);
    WT_UNUSED(root_addr);
    WT_UNUSED(root_addr_sizep);
    WT_UNUSED(checkpoint);

    block_pantry = (WT_BLOCK_PANTRY *)bm->block;
    handle = block_pantry->fh->handle;

    buf[0] = '\0';
    WT_RET(handle->fh_obj_checkpoint_load(handle, &session->iface, buf, 4096));

    key = &buf[0];
    for (i = 0; i < 4096; i++)
        if (buf[i] == '\n')
            break;
    if (i == 4096)
        return (EINVAL); /* TODO think this through */
    buf[i] = '\0';
    value = &buf[i+1];

    /* Tidy up the final newline */
    for (; i < 4096; i++)
        if (buf[i] == '\n') {
            buf[i] = '\0';
            break;
        }

    /* TODO make this conditional - only on "secondary"? */
    WT_RET(__wt_metadata_insert(session, key, value));

    return (0);
}
