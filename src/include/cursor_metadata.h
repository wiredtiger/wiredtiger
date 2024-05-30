/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

/* This file defines __wt_cursor_metadata and related definitions. */

struct __wt_cursor_metadata {
    WT_CURSOR iface;

    WT_CURSOR *file_cursor;   /* Queries of regular metadata */
    WT_CURSOR *create_cursor; /* Extra cursor for create option */

/* AUTOMATIC FLAG VALUE GENERATION START 0 */
#define WT_MDC_CREATEONLY 0x1u
#define WT_MDC_ONMETADATA 0x2u
#define WT_MDC_POSITIONED 0x4u
    /* AUTOMATIC FLAG VALUE GENERATION STOP 8 */
    uint8_t flags;
};
