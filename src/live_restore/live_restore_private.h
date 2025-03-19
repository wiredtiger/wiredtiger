/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

#define WT_LIVE_RESTORE_FS_TOMBSTONE_SUFFIX ".deleted"

/*
 * WT_OFFSET_END returns the last byte used by a range (inclusive). i.e. if we have an offset=0 and
 * length=1024 WT_OFFSET_END returns 1023
 */
#define WT_OFFSET_END(offset, len) (offset + (wt_off_t)len - 1)
#define WT_EXTENT_END(ext) WT_OFFSET_END((ext)->off, (ext)->len)
/* As extent ranges are inclusive we want >= and <= on both ends of the range. */
#define WT_OFFSET_IN_EXTENT(addr, ext) ((addr) >= (ext)->off && (addr) <= WT_EXTENT_END(ext))

/*
 * __wt_live_restore_hole_list --
 *     A linked list of extents. Each extent represents a hole in the destination file that needs to
 *     be read from the source file.
 */
struct __wt_live_restore_hole_list {
    wt_off_t off;
    size_t len;

    WT_LIVE_RESTORE_HOLE_LIST *next;
};

/*
 * WT_DESTINATION_METADATA --
 *     Metadata kept along side a file handle to track holes in the destination file.
 */
typedef struct {
    WT_FILE_HANDLE *fh;
    bool complete;

    /* We need to get back to the file system when checking for tombstone files. */
    WT_LIVE_RESTORE_FS *back_pointer;

    /*
     * hole_list tracks which ranges in the destination file are holes. As the migration continues
     * the holes will be gradually filled by either data from the source or new writes. Holes in
     * these extents should only shrink and never grow.
     */
    WT_LIVE_RESTORE_HOLE_LIST *hole_list;
} WT_DESTINATION_METADATA;

/*
 * __wt_live_restore_file_handle --
 *     A file handle in a live restore file system.
 */
struct __wt_live_restore_file_handle {
    WT_FILE_HANDLE iface;
    WT_FILE_HANDLE *source;
    WT_DESTINATION_METADATA destination;

    WT_FS_OPEN_FILE_TYPE file_type;
};

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

#ifdef HAVE_UNITTEST

#endif

/* DO NOT EDIT: automatically built by prototypes.py: END */
