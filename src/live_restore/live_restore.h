/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

/*
 * Live restore state reported to the application so it knows when to terminate live restore.
 *
 * !!! MongoDB doesn't have access to these macros and instead checks them by value. i.e. to
 * know live restore has completed the server layer reads the stat and checks for the value 2. Do
 * not change these values without updating the relevant code in the server layer.
 */
#define WT_LIVE_RESTORE_INIT 0x0
#define WT_LIVE_RESTORE_IN_PROGRESS 0x1
#define WT_LIVE_RESTORE_COMPLETE 0x2

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
extern int __wt_live_restore_setup_recovery(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_os_live_restore_fs(WT_SESSION_IMPL *session, const char *cfg[],
  const char *destination, WT_FILE_SYSTEM **fsp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern void __wt_live_restore_init_stats(WT_SESSION_IMPL *session);

#ifdef HAVE_UNITTEST

#endif

/* DO NOT EDIT: automatically built by prototypes.py: END */
