/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

#define WT_LIVE_RESTORE_STATE_STRING_MAX 128

/* DO NOT EDIT: automatically built by prototypes.py: BEGIN */

extern int __wt_live_restore_fh_extent_to_metadata(WT_SESSION_IMPL *session, WT_FILE_HANDLE *fh,
  WT_ITEM *extent_string) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_live_restore_fh_import_extents_from_string(WT_SESSION_IMPL *session,
  WT_FILE_HANDLE *fh, const char *extent_str) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_live_restore_get_state_string(WT_SESSION_IMPL *session, WT_ITEM *lr_state_str)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_live_restore_server_create(WT_SESSION_IMPL *session, const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_live_restore_server_destroy(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_live_restore_validate_non_lr_system(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_os_live_restore_fs(WT_SESSION_IMPL *session, const char *cfg[],
  const char *destination, WT_FILE_SYSTEM **fsp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern void __wt_live_restore_init_stats(WT_SESSION_IMPL *session);

#ifdef HAVE_UNITTEST

#endif

/* DO NOT EDIT: automatically built by prototypes.py: END */
