/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

typedef enum { WT_UNION_FS_LAYER_DESTINATION, WT_UNION_FS_LAYER_SOURCE } WT_UNION_FS_LAYER_TYPE;

/*
 * __wt_union_fs_layer --
 *     A layer in a union file system.
 */
struct __wt_union_fs_layer {
    const char *home;
    WT_UNION_FS_LAYER_TYPE which;
};

/*
 * __wt_union_fs --
 *     A union file system in the user space, which consists of one or more actual FS layers.
 */
struct __wt_union_fs {
    WT_FILE_SYSTEM iface;
    WT_FILE_SYSTEM *os_file_system; /* The storage file system. */
    WT_UNION_FS_LAYER destination;
    WT_UNION_FS_LAYER source;
};

/* DO NOT EDIT: automatically built by prototypes.py: BEGIN */

extern int __wt_os_union_fs(WT_SESSION_IMPL *session, WT_CONFIG_ITEM *source_cfg,
  const char *destination, WT_FILE_SYSTEM **fsp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#ifdef HAVE_UNITTEST

#endif

/* DO NOT EDIT: automatically built by prototypes.py: END */
