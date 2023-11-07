/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_curblock_init --
 *     Initialize a block cursor.
 */
int
__wt_curblock_init(WT_SESSION_IMPL *session, WT_CURSOR_BLOCK *cblock)
{
    WT_CURSOR *cursor;
    WT_CURSOR_BTREE *cbt;
    WT_DECL_RET;

    cursor = &cblock->cbt.iface;
    cbt = &cblock->cbt;

    if (CUR2BT(cbt)->type != BTREE_ROW)
        WT_ERR_MSG(session, EINVAL, "block cursor only supports row store");

    if (!WT_STREQ(cursor->key_format, "u") || !WT_STREQ(cursor->value_format, "u"))
        WT_ERR_MSG(session, EINVAL, "block cursor only supports raw format");

    cursor->next_raw_n = __wt_cursor_next_raw_n_notsup;
    cursor->prev_raw_n = __wt_cursor_next_raw_n_notsup;

    WT_CLEAR(cblock->keys);
    WT_CLEAR(cblock->values);

err:
    return (ret);
}

/*
 * __wt_curblock_close --
 *     Close a block cursor.
 */
void
__wt_curblock_close(WT_SESSION_IMPL *session, WT_CURSOR_BLOCK *cblock)
{
    size_t i;

    for (i = 0; i < MAX_BLOCK_ITEM; i++) {
        __wt_buf_free(session, &cblock->keys[i]);
        __wt_buf_free(session, &cblock->values[i]);
    }
}
