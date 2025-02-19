/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

#define WT_LIVE_RESTORE_INIT 0x0
#define WT_LIVE_RESTORE_IN_PROGRESS 0x1
#define WT_LIVE_RESTORE_COMPLETE 0x2

/*
 * __wt_live_restore_fh_meta --
 *     File handle metadata persisted to the WiredTiger metadata file.
 */
struct __wt_live_restore_fh_meta {
    char *bitmap_str;
    uint64_t nbits;
    uint32_t allocsize;
};

/* DO NOT EDIT: automatically built by prototypes.py: BEGIN */

extern int __wt_live_restore_fh_import_bitmap(WT_SESSION_IMPL *session, WT_FILE_HANDLE *fh,
  WT_LIVE_RESTORE_FH_META *lr_fh_meta) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_live_restore_fh_to_metadata(WT_SESSION_IMPL *session, WT_FILE_HANDLE *fh,
  WT_ITEM *meta_string) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_live_restore_server_create(WT_SESSION_IMPL *session, const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_live_restore_server_destroy(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_os_live_restore_fs(WT_SESSION_IMPL *session, const char *cfg[],
  const char *destination, WT_FILE_SYSTEM **fsp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#ifdef HAVE_UNITTEST
extern int __ut_live_restore_decode_bitmap(WT_SESSION_IMPL *session, const char *bitmap_str,
  uint64_t nbits, WTI_LIVE_RESTORE_FILE_HANDLE *lr_fh)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __ut_live_restore_encode_bitmap(
  WT_SESSION_IMPL *session, WTI_LIVE_RESTORE_FILE_HANDLE *lr_fh, WT_ITEM *buf)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#endif

/* DO NOT EDIT: automatically built by prototypes.py: END */
