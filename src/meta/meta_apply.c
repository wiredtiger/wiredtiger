/*-
 * Copyright (c) 2014-2019 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __meta_btree_apply --
 *     Conditionally apply a function to the given file URI.
 */
static inline int
__wt_meta_btree_apply(WT_SESSION_IMPL *session, const char* uri,
  int (*file_func)(WT_SESSION_IMPL *, const char *[]),
  int (*name_func)(WT_SESSION_IMPL *, const char *, bool *), const char *cfg[])
{
    WT_DECL_RET;
    int t_ret;
    bool skip;

    skip = false;
    if (name_func != NULL)
        ret = name_func(session, uri, &skip);

    if (file_func == NULL || skip || !WT_PREFIX_MATCH(uri, "file:"))
        return (ret);

    /*
     * We need to pull the handle into the session handle cache and make sure it's referenced to
     * stop other internal code dropping the handle (e.g in LSM when cleaning up obsolete
     * chunks). Holding the schema lock isn't enough.
     *
     * Handles that are busy are skipped without the whole operation failing. This deals among
     * other cases with checkpoint encountering handles that are locked (e.g., for bulk loads or
     * verify operations).
     */
    if ((t_ret = __wt_session_get_dhandle(session, uri, NULL, NULL, 0)) != 0) {
        WT_TRET_BUSY_OK(t_ret);
    }

    WT_SAVE_DHANDLE(session, WT_TRET(file_func(session, cfg)));
    WT_TRET(__wt_session_release_dhandle(session));

    return (ret);
}
/*
 * __meta_btree_apply --
 *     Walk all files listed in the metadata, apart from the metadata file and LAS file. Applying
 *     a given function to each file. At the end apply this function to the LAS file.
 */
static inline int
__meta_btree_walk_and_apply(WT_SESSION_IMPL *session, WT_CURSOR *cursor,
  int (*file_func)(WT_SESSION_IMPL *, const char *[]),
  int (*name_func)(WT_SESSION_IMPL *, const char *, bool *), const char *cfg[])
{
    WT_DECL_RET;
    int t_ret;
    const char *uri;

    /*
     * Accumulate errors but continue through to the end of the metadata.
     */
    while ((t_ret = cursor->next(cursor)) == 0) {
        if ((t_ret = cursor->get_key(cursor, &uri)) != 0 || strcmp(uri, WT_METAFILE_URI) == 0 || strcmp(uri, WT_LAS_URI) == 0) {
            WT_TRET(t_ret);
            continue;
        }
        WT_TRET(__wt_meta_btree_apply(session, uri, file_func, name_func, cfg));
    }
    WT_TRET_NOTFOUND_OK(t_ret);

    WT_TRET(__wt_meta_btree_apply(session, WT_LAS_URI, file_func, name_func, cfg));

    return (ret);
}

/*
 * __wt_meta_apply_all --
 *     Apply a function to all files listed in the metadata, apart from the metadata file.
 */
int
__wt_meta_apply_all(WT_SESSION_IMPL *session, int (*file_func)(WT_SESSION_IMPL *, const char *[]),
  int (*name_func)(WT_SESSION_IMPL *, const char *, bool *), const char *cfg[])
{
    WT_CURSOR *cursor;
    WT_DECL_RET;

    WT_ASSERT(session, F_ISSET(session, WT_SESSION_LOCKED_SCHEMA));
    WT_RET(__wt_metadata_cursor(session, &cursor));
    WT_SAVE_DHANDLE(session, ret = __meta_btree_walk_and_apply(session, cursor, file_func, name_func, cfg));
    WT_TRET(__wt_metadata_cursor_release(session, &cursor));

    return (ret);
}
