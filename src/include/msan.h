/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 * 	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */
#include <sys/stat.h>
#include <sanitizer/msan_interface.h>
#include <string.h>

static inline int
__wt_stat(const char *path, struct stat *buf)
{
    // memset(buf, 0, sizeof(*buf));
    WT_RET(stat(path, buf));

    __msan_unpoison(&buf, sizeof(buf));
    return (0);
}

static inline int
__wt_fstat(int fd, struct stat *buf)
{
    // memset(buf, 0, sizeof(*buf));
    WT_RET(fstat(fd, buf));

    __msan_unpoison(&buf, sizeof(buf));
    return (0);
}

