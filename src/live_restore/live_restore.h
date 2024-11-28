/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

typedef enum {
    WT_LIVE_RESTORE_FS_LAYER_DESTINATION,
    WT_LIVE_RESTORE_FS_LAYER_SOURCE
} WT_LIVE_RESTORE_FS_LAYER_TYPE;

/*
 * __wt_live_restore_fs_layer --
 *     A layer in the live restore file system.
 */
struct __wt_live_restore_fs_layer {
    const char *home;
    WT_LIVE_RESTORE_FS_LAYER_TYPE which;
};

/*
 * __wt_live_restore_fs --
 *     A live restore file system in the user space, which consists of a source and destination 
 *     layer.
 */
struct __wt_live_restore_fs {
    WT_FILE_SYSTEM iface;
    WT_FILE_SYSTEM *os_file_system; /* The storage file system. */
    WT_LIVE_RESTORE_FS_LAYER destination;
    WT_LIVE_RESTORE_FS_LAYER source;
};

/* DO NOT EDIT: automatically built by prototypes.py: BEGIN */

extern int __wt_os_live_restore_fs(WT_SESSION_IMPL *session, WT_CONFIG_ITEM *source_cfg,
  const char *destination, WT_FILE_SYSTEM **fsp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#ifdef HAVE_UNITTEST

#endif

/* DO NOT EDIT: automatically built by prototypes.py: END */
