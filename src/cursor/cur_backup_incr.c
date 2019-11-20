/*-
 * Copyright (c) 2014-2019 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __curbackup_incr_get_key --
 *     WT_CURSOR->get_key for incremental hot backup cursors.
 */
static int
__curbackup_incr_get_key(WT_CURSOR *cursor, ...)
{
    WT_CURSOR_BACKUP *cb;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;
    uint64_t offset, size, type;
    va_list ap;

    va_start(ap, cursor);
    cb = (WT_CURSOR_BACKUP *)cursor;
    CURSOR_API_CALL(cursor, session, get_key, NULL);

    WT_ERR(__cursor_needkey(cursor));

    offset = cb->incr_list[cb->incr_list_offset];
    size = WT_MIN(cb->incr_list[cb->incr_list_offset + 1], cb->incr_granularity);
    /* XXX fix */
    type = WT_BACKUP_FILE;

    *va_arg(ap, uint64_t *) = offset;
    *va_arg(ap, uint64_t *) = size;
    *va_arg(ap, uint64_t *) = type;

err:
    va_end(ap);
    API_END_RET(session, ret);
}

/*
 * __alloc_merge --
 *     Merge two allocation lists.
 */
static void
__alloc_merge(
  uint64_t *a, uint64_t a_cnt, uint64_t *b, uint64_t b_cnt, uint64_t *res, uint64_t *res_cnt)
{
    uint64_t total;

    for (total = 0; a_cnt > 0 || b_cnt > 0; ++total, res += 2) {
        if (a_cnt > 0 && b_cnt > 0) {
            if (a[0] <= b[0]) {
                res[0] = a[0];
                if (a[0] + a[1] < b[0])
                    res[1] = a[1];
                else {
                    res[1] = (b[0] + b[1]) - a[0];
                    b += 2;
                    --b_cnt;
                }
                a += 2;
                --a_cnt;
            } else if (b[0] <= a[0]) {
                res[0] = b[0];
                if (b[0] + b[1] < a[0])
                    res[1] = b[1];
                else {
                    res[1] = (a[0] + a[1]) - b[0];
                    a += 2;
                    --a_cnt;
                }
                b += 2;
                --b_cnt;
            }
        } else if (a_cnt > 0) {
            res[0] = a[0];
            res[1] = a[1];
            a += 2;
            --a_cnt;
        } else if (b_cnt > 0) {
            res[0] = b[0];
            res[1] = b[1];
            b += 2;
            --b_cnt;
        }
    }
    *res_cnt = total;
}

/*
 * __curbackup_incr_next --
 *     WT_CURSOR->next method for the btree cursor type when configured with incremental_backup.
 */
static int
__curbackup_incr_next(WT_CURSOR *cursor)
{
    WT_BTREE *btree;
    WT_CKPT *ckpt, *ckptbase;
    WT_CURSOR_BACKUP *cb;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;
    wt_off_t size;
    uint64_t *a, *b, *current, *next;
    uint64_t entries, total;
    bool start, stop;

    ckptbase = NULL;
    a = b = NULL;

    cb = (WT_CURSOR_BACKUP *)cursor;
    btree = cb->incr_cursor == NULL ? NULL : ((WT_CURSOR_BTREE *)cb->incr_cursor)->btree;
    CURSOR_API_CALL(cursor, session, get_value, btree);

    if (cb->incr_init) {
        /* We have this object's incremental information, Check if we're done. */
        if (cb->incr_list_offset >= cb->incr_list_count - 2)
            return (WT_NOTFOUND);

        /*
         * If we returned all of the data, step to the next block, otherwise return the next chunk
         * of the current block.
         */
        if (cb->incr_list[cb->incr_list_offset + 1] <= cb->incr_granularity)
            cb->incr_list_offset += 2;
        else {
            cb->incr_list[cb->incr_list_offset] += cb->incr_granularity;
            cb->incr_list[cb->incr_list_offset + 1] -= cb->incr_granularity;
        }
    } else if (btree == NULL) {
        /* We don't have this objects incremental information, and it's a full file copy. */
        WT_ERR(__wt_fs_size(session, cb->incr_file, &size));

        WT_ERR(__wt_calloc_def(session, 2, &cb->incr_list));
        cb->incr_list[0] = 0;
        cb->incr_list[1] = (uint64_t)size;
        cb->incr_list_count = 2;
        cb->incr_list_offset = 0;
        WT_ERR(__wt_scr_alloc(session, 0, &cb->incr_block));
        cb->incr_init = true;

        F_SET(cursor, WT_CURSTD_KEY_EXT | WT_CURSTD_VALUE_EXT);
    } else {
        /*
         * We don't have this objects incremental information, and it's not a full file copy. Get a
         * list of the checkpoints available for the file and flag the starting/stopping ones. It
         * shouldn't be possible to specify checkpoints that no longer exist, but check anyway.
         */
        ret = __wt_meta_ckptlist_get(session, cb->incr_file, false, &ckptbase);
        WT_ERR(ret == WT_NOTFOUND ? ENOENT : ret);

        /*
         * Count up the maximum number of block entries we might have to merge, and allocate a pair
         * of temporary arrays in which to do the merge.
         */
        entries = 0;
        WT_CKPT_FOREACH (ckptbase, ckpt)
            entries += ckpt->alloc_list_entries;
        WT_ERR(__wt_calloc_def(session, entries * 2, &a));
        WT_ERR(__wt_calloc_def(session, entries * 2, &b));

        /* Merge the block lists into a final list of blocks to copy. */
        start = stop = false;
        total = 0;
        current = NULL;
        next = a;
        WT_CKPT_FOREACH (ckptbase, ckpt) {
            if (strcmp(ckpt->name, cb->incr_checkpoint_start) == 0) {
                start = true;
                WT_ERR_ASSERT(session, ckpt->alloc_list_entries == 0, __wt_panic(session),
                  "incremental backup start checkpoint has allocation list blocks");
                continue;
            }
            if (start == true) {
                if (strcmp(ckpt->name, cb->incr_checkpoint_stop) == 0)
                    stop = true;

                __alloc_merge(
                  current, total, ckpt->alloc_list, ckpt->alloc_list_entries, next, &total);
                current = next;
                next = next == a ? b : a;
            }

            if (stop == true)
                break;
        }

        if (!start)
            WT_ERR_MSG(session, ENOENT, "incremental backup start checkpoint %s not found",
              cb->incr_checkpoint_start);
        if (!stop)
            WT_ERR_MSG(session, ENOENT, "incremental backup stop checkpoint %s not found",
              cb->incr_checkpoint_stop);

        /* There may be nothing that needs copying. */
        if (total == 0)
            WT_ERR(WT_NOTFOUND);

        if (next == a) {
            cb->incr_list = b;
            b = NULL;
        } else {
            cb->incr_list = a;
            a = NULL;
        }
        cb->incr_list_count = total;
        cb->incr_list_offset = 0;
        WT_ERR(__wt_scr_alloc(session, 0, &cb->incr_block));
        cb->incr_init = true;

        F_SET(cursor, WT_CURSTD_KEY_EXT | WT_CURSTD_VALUE_EXT);
    }

err:
    __wt_free(session, a);
    __wt_free(session, b);
    __wt_meta_ckptlist_free(session, &ckptbase);
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
    __wt_cursor_close(cb->incr_cursor);
    __wt_free(session, cb->incr_checkpoint_start);
    __wt_free(session, cb->incr_checkpoint_stop);
    __wt_free(session, cb->incr_list);
    __wt_scr_free(session, &cb->incr_block);
}

/*
 * __wt_curbackup_open_incr --
 *     Initialize the duplicate backup cursor for a file-based incremental backup.
 */
int
__wt_curbackup_open_incr(WT_SESSION_IMPL *session, const char *uri, WT_CURSOR *other,
  WT_CURSOR *cursor, const char *cfg[], WT_CURSOR **cursorp)
{
    static const char *copy_entire[] = {WT_BASECONFIG, WT_METADATA_BACKUP, WT_WIREDTIGER, NULL};
    WT_CURSOR_BACKUP *cb, *other_cb;
    WT_DECL_ITEM(open_checkpoint);
    WT_DECL_ITEM(open_uri);
    WT_DECL_RET;
    size_t i;
    const char **p, **new_cfg;

    cb = (WT_CURSOR_BACKUP *)cursor;
    other_cb = (WT_CURSOR_BACKUP *)other;
    new_cfg = NULL;

    cursor->key_format = "qqq";
    cursor->value_format = "u";

    cursor->next = __curbackup_incr_next;
    cursor->get_key = __curbackup_incr_get_key;
    cursor->get_value = __wt_cursor_get_value_notsup;

    /* We need a starting checkpoint. */
    if (other_cb->incr_checkpoint_start == NULL)
        WT_ERR_MSG(session, EINVAL,
          "a starting checkpoint must be specified to open a hot backup cursor for file-based "
          "incremental backups");

    /* The two checkpoints aren't supposed to be the same. */
    if (strcmp(other_cb->incr_checkpoint_start, other->checkpoint) == 0)
        WT_ERR_MSG(session, EINVAL,
          "incremental backup start and stop checkpoints are the same: %s",
          other_cb->incr_checkpoint_start);

    /* Copy information from the primary cursor to the current file. */
    WT_ERR(__wt_strdup(session, other_cb->incr_checkpoint_start, &cb->incr_checkpoint_start));
    WT_ERR(__wt_strdup(session, other->checkpoint, &cb->incr_checkpoint_stop));
    cb->incr_granularity = other_cb->incr_granularity;

    /*
     * Files that aren't underlying block-manager files have to be copied in their entirety. Catch
     * them up front and don't try and read them.
     */
    for (p = copy_entire; *p != NULL; ++p)
        if (strcmp(*p, cb->incr_file) == 0)
            return (__wt_cursor_init(cursor, uri, NULL, cfg, cursorp));

    /*
     * If doing a file-based incremental backup, we need an open cursor on the file. Open the backup
     * checkpoint, confirming it exists.
     */
    WT_ERR(__wt_scr_alloc(session, 0, &open_uri));
    WT_ERR(__wt_buf_fmt(session, open_uri, "file:%s", cb->incr_file));
    __wt_free(session, cb->incr_file);
    WT_ERR(__wt_strdup(session, open_uri->data, &cb->incr_file));

    WT_ERR(__wt_scr_alloc(session, 0, &open_checkpoint));
    WT_ERR(__wt_buf_fmt(session, open_checkpoint, "checkpoint=%s", cb->incr_checkpoint_start));
    for (i = 0; cfg[i] != NULL; ++i)
        ;
    WT_ERR(__wt_calloc_def(session, i + 2, &new_cfg));
    for (i = 0; cfg[i] != NULL; ++i)
        new_cfg[i] = cfg[i];
    new_cfg[i++] = open_checkpoint->data;
    new_cfg[i] = NULL;

    WT_ERR(__wt_curfile_open(session, cb->incr_file, NULL, new_cfg, &cb->incr_cursor));

    WT_ERR(__wt_cursor_init(cursor, uri, NULL, cfg, cursorp));

    /* XXX KEITH */
    WT_ERR(__wt_strdup(session, cb->incr_cursor->internal_uri, &cb->incr_cursor->internal_uri));

err:
    __wt_scr_free(session, &open_checkpoint);
    __wt_scr_free(session, &open_uri);
    __wt_free(session, new_cfg);
    return (ret);
}
