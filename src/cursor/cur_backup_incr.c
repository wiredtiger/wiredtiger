/*-
 * Copyright (c) 2014-2019 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __curbackup_incr_next --
 *     WT_CURSOR->next method for the btree cursor type when configured with incremental_backup.
 */
static int
__curbackup_incr_next(WT_CURSOR *cursor)
{
    WT_BLOCK_MODS *blk_mods;
    WT_BTREE *btree;
    WT_CURSOR_BACKUP *cb;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;
    wt_off_t size;
    uint64_t entries;
    uint64_t *list, *p;
    uint32_t i, raw;

    cb = (WT_CURSOR_BACKUP *)cursor;
    btree = cb->incr_cursor == NULL ? NULL : ((WT_CURSOR_BTREE *)cb->incr_cursor)->btree;
    raw = F_MASK(cursor, WT_CURSTD_RAW);
    CURSOR_API_CALL(cursor, session, get_value, btree);
    F_CLR(cursor, WT_CURSTD_RAW);

    if (cb->incr_init) {
        /* We have this object's incremental information, Check if we're done. */
        if (cb->incr_list_offset >= cb->incr_list_count - WT_BACKUP_INCR_COMPONENTS)
            return (WT_NOTFOUND);

        /*
         * If we returned all of the data, step to the next block, otherwise return the next chunk
         * of the current block.
         */
        if (cb->incr_list[cb->incr_list_offset + 1] <= cb->incr_granularity)
            cb->incr_list_offset += WT_BACKUP_INCR_COMPONENTS;
        else {
            cb->incr_list[cb->incr_list_offset] += cb->incr_granularity;
            cb->incr_list[cb->incr_list_offset + 1] -= cb->incr_granularity;
            cb->incr_list[cb->incr_list_offset + 2] = WT_BACKUP_RANGE;
        }
    } else if (btree == NULL || F_ISSET(cb, WT_CURBACKUP_FORCE_FULL)) {
        /* We don't have this object's incremental information, and it's a full file copy. */
        WT_ERR(__wt_fs_size(session, cb->incr_file, &size));

        cb->incr_list_count = WT_BACKUP_INCR_COMPONENTS;
        cb->incr_init = true;
        cb->incr_list_offset = 0;
        __wt_cursor_set_key(cursor, 0, size, WT_BACKUP_FILE);
    } else {
        /*
         * We don't have this object's incremental information, and it's not a full file copy. Get a
         * list of the block modifications for the file. The block modifications are from the
         * incremental identifier starting point. Walk the list looking for one with a source of our
         * id.
         */
        blk_mods = NULL;
        WT_ERR(__wt_btree_get_blkmods(session, btree, blk_mods));
        /*
         * If there is no block modification information for this file, there is no information to
         * return to the user.
         */
        if (blk_mods == NULL)
            WT_ERR(WT_NOTFOUND);

        for (i = 0; i < WT_BLKINCR_MAX; ++i, ++blk_mods)
            if (strcmp(blk_mods->id_str, cb->incr_src) == 0)
                break;
        /*
         * XXX Need to coordinate with checkpoint? Cannot have checkpoint changing the block list
         * while we walk it. It might reallocate it, freeing our memory out from under us.
         */

        /*
         * There is no block information from this source identifier. There is no information to
         * return to the user.
         */
        if (i == WT_BLKINCR_MAX)
            WT_ERR(WT_NOTFOUND);

        /*
         * Set up our list to return to the user.
         */
        WT_ASSERT(session, F_ISSET(blk_mods, WT_BLOCK_MODS_VALID));
        WT_ERR(__wt_calloc_def(
          session, blk_mods->alloc_list_entries * WT_BACKUP_INCR_COMPONENTS, &cb->incr_list));
        cb->incr_list_count = blk_mods->alloc_list_entries;
        cb->incr_list_offset = 0;
        for (entries = 0, list = cb->incr_list, p = blk_mods->alloc_list;
             entries < blk_mods->alloc_list_entries; ++entries, p += WT_BACKUP_INCR_COMPONENTS) {
            *list++ = p[0];
            *list++ = p[1];
            *list++ = p[2];
        }
        cb->incr_init = true;

        F_SET(cursor, WT_CURSTD_KEY_EXT | WT_CURSTD_VALUE_EXT);
    }

err:
    F_SET(cursor, raw);
    API_END_RET(session, ret);
}

/*
 * __wt_curbackup_free_incr --
 *     Free the duplicate backup cursor for a file-based incremental backup.
 */
void
__wt_curbackup_free_incr(WT_SESSION_IMPL *session, WT_CURSOR_BACKUP *cb)
{
    __wt_free(session, cb->incr_file);
    if (cb->incr_cursor != NULL)
        __wt_cursor_close(cb->incr_cursor);
    __wt_free(session, cb->incr_list);
}

/*
 * __wt_curbackup_open_incr --
 *     Initialize the duplicate backup cursor for a file-based incremental backup.
 */
int
__wt_curbackup_open_incr(WT_SESSION_IMPL *session, const char *uri, WT_CURSOR *other,
  WT_CURSOR *cursor, const char *cfg[], WT_CURSOR **cursorp)
{
    WT_CURSOR_BACKUP *cb, *other_cb;
    WT_DECL_ITEM(open_checkpoint);
    WT_DECL_ITEM(open_uri);
    WT_DECL_RET;
    const char **new_cfg;

    cb = (WT_CURSOR_BACKUP *)cursor;
    other_cb = (WT_CURSOR_BACKUP *)other;
    cursor->key_format = WT_UNCHECKED_STRING(qqq);
    cursor->value_format = "";
    new_cfg = NULL;

    WT_ASSERT(session, other_cb->incr_start != NULL);
    if (F_ISSET(other_cb->incr_start, WT_BLKINCR_FULL)) {
        __wt_verbose(session, WT_VERB_BACKUP, "Forcing full file copies for id %s",
          other_cb->incr_start->id_str);
        F_SET(cb, WT_CURBACKUP_FORCE_FULL);
    }

    /*
     * Inherit from the backup cursor but reset specific functions for incremental.
     */
    cursor->next = __curbackup_incr_next;
    cursor->get_key = __wt_cursor_get_key;
    cursor->get_value = __wt_cursor_get_value_notsup;
    cb->incr_granularity = other_cb->incr_granularity;
    cb->incr_start = other_cb->incr_start;

    /*
     * Set up the incremental backup information, if we are not forcing a full file copy. We need an
     * open cursor on the file. Open the backup checkpoint, confirming it exists.
     */
    if (!F_ISSET(cb, WT_CURBACKUP_FORCE_FULL) && !WT_PREFIX_MATCH(cb->incr_file, "WiredTiger")) {
        WT_ERR(__wt_scr_alloc(session, 0, &open_uri));
        WT_ERR(__wt_buf_fmt(session, open_uri, "file:%s", cb->incr_file));
        __wt_free(session, cb->incr_file);
        WT_ERR(__wt_strdup(session, open_uri->data, &cb->incr_file));

        WT_ERR(__wt_curfile_open(session, cb->incr_file, NULL, cfg, &cb->incr_cursor));
        WT_ERR(__wt_cursor_init(cursor, uri, NULL, cfg, cursorp));
        WT_ERR(__wt_strdup(session, cb->incr_cursor->internal_uri, &cb->incr_cursor->internal_uri));
    } else
        WT_ERR(__wt_cursor_init(cursor, uri, NULL, cfg, cursorp));

err:
    __wt_scr_free(session, &open_checkpoint);
    __wt_scr_free(session, &open_uri);
    __wt_free(session, new_cfg);
    return (ret);
}
