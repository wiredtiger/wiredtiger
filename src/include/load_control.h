/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

struct __wt_connection_load_control {
    uint8_t load_control_threshold; /* threshold when the load management starts */
    uint64_t read_load_max;         /* cache in use bytes for max read load */
    uint64_t read_load_threshold;   /* cache in use bytes equivalent to control threshold */
    uint64_t write_load_max;        /* cache dirty bytes for max write load */
    uint64_t write_load_threshold;  /* cache dirty bytes equivalent to control threshold */

    /* cache eviction controls bit positions */
#define WT_CONNECTION_LOAD_CONTROL_CONTROL 0x1u
    wt_shared uint32_t flags;
};
