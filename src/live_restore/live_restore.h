/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

// TODO - Intentionally separate from the state file states. Think about this.
#define WT_LIVE_RESTORE_INIT 0x0
#define WT_LIVE_RESTORE_IN_PROGRESS 0x1
#define WT_LIVE_RESTORE_COMPLETE 0x2

/* DO NOT EDIT: automatically built by prototypes.py: BEGIN */

extern int __wt_live_restore_fh_extent_to_metadata(WT_SESSION_IMPL *session, WT_FILE_HANDLE *fh,
  WT_ITEM *extent_string) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_live_restore_fh_import_extents_from_string(WT_SESSION_IMPL *session,
  WT_FILE_HANDLE *fh, const char *extent_str) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_live_restore_server_create(WT_SESSION_IMPL *session, const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_live_restore_server_destroy(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_live_restore_setup_recovery(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_os_live_restore_fs(WT_SESSION_IMPL *session, const char *cfg[],
  const char *destination, WT_FILE_SYSTEM **fsp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#ifdef HAVE_UNITTEST

#endif

/* DO NOT EDIT: automatically built by prototypes.py: END */
