/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

#define WT_UNION_FS_TOMBSTONE_SUFFIX ".deleted"

/*
 * __wt_union_hole_list
 */
struct __wt_union_hole_list {
    wt_off_t off;
    size_t len;

    WT_UNION_HOLE_LIST *next;
};

/*
 * WT_DESTINATION_METADATA --
 *     A file handle in a union file system - one layer.
 */
typedef struct {
    WT_FILE_HANDLE *fh;
    bool complete;

    /* We need to get back to the file system when checking for tombstone files. */
    WT_UNION_FS *back_pointer;

    /*
     * allocation_list tracks which ranges in the destination file shouldn't be brought up from the
     * source layer. Holes in these extents should only shrink and never grow.
     */
    WT_UNION_HOLE_LIST *hole_list;
} WT_DESTINATION_METADATA;

/*
 * __wt_union_file_handle --
 *     A file handle in a union file system.
 */
struct __wt_union_file_handle {
    WT_FILE_HANDLE iface;
    WT_FILE_HANDLE *source;
    WT_DESTINATION_METADATA destination; /* 0 is the most recent layer. */

    WT_FS_OPEN_FILE_TYPE file_type;
};
