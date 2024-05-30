/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

/* Define opaque types */

#define	WT_CURSOR_BTREE_SIZE	1192 /* sizeof(WT_CURSOR_BTREE) */
typedef struct {
        uint64_t opaque[WT_CURSOR_BTREE_SIZE/8];
} WT_CURSOR_BTREE_OPAQUE;
#define WT_CURSOR_BTREE_CAST(OPAQUEP) ((WT_CURSOR_BTREE *) (OPAQUEP))
