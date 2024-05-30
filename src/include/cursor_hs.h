/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

/* This file defines __wt_cursor_hs and related definitions. */

struct __wt_cursor_hs {
    WT_CURSOR iface;

    WT_CURSOR *file_cursor; /* Queries of regular history store data */
    WT_TIME_WINDOW time_window;
    uint32_t btree_id;
    WT_ITEM *datastore_key;

/* AUTOMATIC FLAG VALUE GENERATION START 0 */
#define WT_HS_CUR_BTREE_ID_SET 0x1u
#define WT_HS_CUR_COUNTER_SET 0x2u
#define WT_HS_CUR_KEY_SET 0x4u
#define WT_HS_CUR_TS_SET 0x8u
    /* AUTOMATIC FLAG VALUE GENERATION STOP 8 */
    uint8_t flags;
};
