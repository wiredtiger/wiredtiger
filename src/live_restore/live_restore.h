/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

struct __wt_live_restore_work_item {
    char *uri;
    TAILQ_ENTRY(__wt_live_restore_work_item) q; /* List of pages queued for pre-fetch. */
};

/*
 * WT_LIVE_RESTORE_SERVER --
 *     Metadata kept along side a file handle to track holes in the destination file.
 */
struct __wt_live_restore_server {
    WT_THREAD_GROUP threads;
    WT_SPINLOCK queue_lock;

    TAILQ_HEAD(__wt_live_restore_work_queue, __wt_live_restore_work_item) work_queue;
};

/* DO NOT EDIT: automatically built by prototypes.py: BEGIN */

extern int __wt_live_restore_server_destroy(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_live_restore_server_init(WT_SESSION_IMPL *session, const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_os_live_restore_fs(WT_SESSION_IMPL *session, const char *cfg[],
  const char *destination, WT_FILE_SYSTEM **fsp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#ifdef HAVE_UNITTEST

#endif

/* DO NOT EDIT: automatically built by prototypes.py: END */
