/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

/* This file defines __wt_cursor_log and related definitions. */

struct __wt_cursor_log {
    WT_CURSOR iface;

    WT_LSN *cur_lsn;                  /* LSN of current record */
    WT_LSN *next_lsn;                 /* LSN of next record */
    WT_ITEM *logrec;                  /* Copy of record for cursor */
    WT_ITEM *opkey, *opvalue;         /* Op key/value copy */
    const uint8_t *stepp, *stepp_end; /* Pointer within record */
    uint8_t *packed_key;              /* Packed key for 'raw' interface */
    uint8_t *packed_value;            /* Packed value for 'raw' interface */
    uint32_t step_count;              /* Intra-record count */
    uint32_t rectype;                 /* Record type */
    uint64_t txnid;                   /* Record txnid */

/* AUTOMATIC FLAG VALUE GENERATION START 0 */
#define WT_CURLOG_REMOVE_LOCK 0x1u /* Remove lock held */
                                   /* AUTOMATIC FLAG VALUE GENERATION STOP 8 */
    uint8_t flags;
};
