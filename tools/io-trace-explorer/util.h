/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#ifndef BLOCK_TRACE_EXPLORER_UTIL_H
#define BLOCK_TRACE_EXPLORER_UTIL_H

#include <sys/time.h>

/*
 * current_time --
 *     Return the current time in seconds.
 */
inline double
current_time(void)
{
    struct timeval tv;
    gettimeofday(&tv, NULL);

    return tv.tv_sec + tv.tv_usec / 1.0e6;
}

#endif /* BLOCK_TRACE_EXPLORER_UTIL_H */