/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

/* This file defines __wt_cursor_version and related definitions. */

struct __wt_cursor_version {
    WT_CURSOR iface;

    WT_CURSOR *hs_cursor;   /* Queries of history cursor. */
    WT_CURSOR *file_cursor; /* Queries of regular file cursor. */
    WT_UPDATE *next_upd;

    /*
     * While we are iterating through updates on the update list, we need to remember information
     * about the previous update we have just traversed so that we can record this as part of the
     * debug metadata in the version cursor's key.
     */
    uint64_t upd_stop_txnid;
    /* The previous traversed update's durable_ts will become the durable_stop_ts. */
    wt_timestamp_t upd_durable_stop_ts;
    /* The previous traversed update's start_ts will become the stop_ts. */
    wt_timestamp_t upd_stop_ts;

#define WT_CURVERSION_UPDATE_CHAIN 0
#define WT_CURVERSION_DISK_IMAGE 1
#define WT_CURVERSION_HISTORY_STORE 2

/* AUTOMATIC FLAG VALUE GENERATION START 0 */
#define WT_CURVERSION_HS_EXHAUSTED 0x1u
#define WT_CURVERSION_ON_DISK_EXHAUSTED 0x2u
#define WT_CURVERSION_UPDATE_EXHAUSTED 0x4u
    /* AUTOMATIC FLAG VALUE GENERATION STOP 8 */
    uint8_t flags;
};
