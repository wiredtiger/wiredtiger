/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

#include "os.h"

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