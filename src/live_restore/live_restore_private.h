/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

/*
 * Stop files are created in the file system to indicate that the source directory should never be
 * used for the filename indicated.
 *
 * For example "foo.wt" would have a stop file "foo.wt.stop", this could mean a number of things:
 *  - The file foo.wt may have completed migration.
 *  - It may have been removed, in this case we create a stop file in case the same name "foo.wt" is
 *    recreated.
 *  - It may have been renamed, again we create a stop file in case it is recreated.
 */
#define WTI_LIVE_RESTORE_STOP_FILE_SUFFIX ".stop"
#define WTI_LIVE_RESTORE_TEMP_FILE_SUFFIX ".lr_tmp"
/*
 * WTI_OFFSET_END returns the last byte used by a range (inclusive). i.e. if we have an offset=0 and
 * length=1024 WTI_OFFSET_END returns 1023
 */
#define WTI_OFFSET_END(offset, len) (offset + (wt_off_t)len)
#define WTI_OFFSET_TO_BIT(offset) (uint64_t)((offset) / (wt_off_t)lr_fh->allocsize)
#define WTI_BIT_TO_OFFSET(bit) (wt_off_t)((bit)*lr_fh->allocsize)

/*
 * __wti_live_restore_file_handle --
 *     A file handle in a live restore file system.
 */
struct __wti_live_restore_file_handle {
    WT_FILE_HANDLE iface;
    WT_FILE_HANDLE *source;
    size_t source_size;
    /* Metadata kept along side a file handle to track holes in the destination file. */
    struct {
        WT_FILE_HANDLE *fh;
        bool complete;

        /* We need to get back to the file system when checking for stop files. */
        WTI_LIVE_RESTORE_FS *back_pointer;

        /* Number of bits in the bitmap, should be equivalent to source file size / alloc_size. */
        uint64_t nbits;
        uint8_t *bitmap;
        bool newly_created;
    } destination;

    uint32_t allocsize;
    WT_FS_OPEN_FILE_TYPE file_type;
    WT_RWLOCK bitmap_lock;
};

/*
 * WTI_WITH_LIVE_RESTORE_BITMAP_WRITE_LOCK --
 *     Acquire the bitmap list write lock and perform an operation.
 */
#define WTI_WITH_LIVE_RESTORE_BITMAP_WRITE_LOCK(session, lr_fh, op) \
    do {                                                            \
        __wt_writelock((session), &(lr_fh)->bitmap_lock);           \
        op;                                                         \
        __wt_writeunlock((session), &(lr_fh)->bitmap_lock);         \
    } while (0)

typedef enum {
    WTI_LIVE_RESTORE_FS_LAYER_DESTINATION,
    WTI_LIVE_RESTORE_FS_LAYER_SOURCE
} WTI_LIVE_RESTORE_FS_LAYER_TYPE;

/*
 * __wti_live_restore_fs_layer --
 *     A layer in the live restore file system.
 */
struct __wti_live_restore_fs_layer {
    const char *home;
    WTI_LIVE_RESTORE_FS_LAYER_TYPE which;
};

/*
 * __wti_live_restore_fs --
 *     A live restore file system in the user space, which consists of a source and destination
 *     layer.
 */
struct __wti_live_restore_fs {
    WT_FILE_SYSTEM iface;
    WT_FILE_SYSTEM *os_file_system; /* The storage file system. */
    WTI_LIVE_RESTORE_FS_LAYER destination;
    WTI_LIVE_RESTORE_FS_LAYER source;
    bool finished;

    uint8_t background_threads_max;
    size_t read_size;
};

/*
 * WTI_LIVE_RESTORE_WORK_ITEM --
 *     A single item of work to be worked on by a thread.
 */
struct __wti_live_restore_work_item {
    char *uri;
    TAILQ_ENTRY(__wti_live_restore_work_item) q; /* List of URIs queued for background migration. */
};

/*
 * WTI_LIVE_RESTORE_SERVER --
 *     The live restore server object that is kept on the connection. Holds a thread group and the
 *     work queue, with some additional info.
 */
struct __wti_live_restore_server {
    WT_THREAD_GROUP threads;
    wt_shared uint32_t threads_working;
    WT_SPINLOCK queue_lock;
    WT_TIMER msg_timer;
    WT_TIMER start_timer;
    uint64_t msg_count;
    uint64_t work_count;
    uint64_t work_items_remaining;

    TAILQ_HEAD(__wti_live_restore_work_queue, __wti_live_restore_work_item) work_queue;
};

/* DO NOT EDIT: automatically built by prototypes.py: BEGIN */

extern int __wti_live_restore_cleanup_stop_files(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_live_restore_fs_fill_holes(WT_FILE_HANDLE *fh, WT_SESSION *wt_session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#ifdef HAVE_UNITTEST

#endif

/* DO NOT EDIT: automatically built by prototypes.py: END */
