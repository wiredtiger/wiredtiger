/*-
 * Copyright (c) 2014-2020 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/* AUTOMATIC FLAG VALUE GENERATION START */
#define WT_TXN_TS_ALREADY_LOCKED 0x1u
#define WT_TXN_TS_INCLUDE_CKPT 0x2u
#define WT_TXN_TS_INCLUDE_OLDEST 0x4u
/* AUTOMATIC FLAG VALUE GENERATION STOP */

#define WT_TS_NONE 0         /* Beginning of time */
#define WT_TS_MAX UINT64_MAX /* End of time */

/*
 * We format timestamps in a couple of ways, declare appropriate sized buffers. Hexadecimal is 2x
 * the size of the value. MongoDB format (high/low pairs of 4B unsigned integers, with surrounding
 * parenthesis and separating comma and space), is 2x the maximum digits from a 4B unsigned integer
 * plus 4. Both sizes include a trailing null byte as well.
 */
#define WT_TS_HEX_STRING_SIZE (2 * sizeof(wt_timestamp_t) + 1)
#define WT_TS_INT_STRING_SIZE (2 * 10 + 4 + 1)

/*
 * We need an appropriately sized buffer for formatted time pairs, aggregates and windows. This is
 * for time windows with 4 timestamps, 2 transaction IDs, prepare state and formatting. The
 * formatting is currently about 32 characters - enough space that we don't need to think about it.
 */
#define WT_TP_STRING_SIZE (WT_TS_INT_STRING_SIZE + 1 + 20 + 1)
#define WT_TIME_STRING_SIZE (WT_TS_INT_STRING_SIZE * 4 + 20 * 2 + 64)

/* The set of time pairs that define a time window and some associated metadata */
struct __wt_time_window {
    wt_timestamp_t start_durable_ts;
    wt_timestamp_t start_ts;
    uint64_t start_txn;

    wt_timestamp_t stop_durable_ts;
    wt_timestamp_t stop_ts;
    uint64_t stop_txn;
    bool prepare;
};

/* The set of time pairs that define an aggregated time window */
struct __wt_time_aggregate {

    wt_timestamp_t newest_stop_durable_ts;
    wt_timestamp_t newest_stop_ts;
    uint64_t newest_stop_txn;

    wt_timestamp_t oldest_start_ts;
    uint64_t oldest_start_txn;
    bool prepare;
};
