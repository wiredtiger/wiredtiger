/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

/*
 * WT_BLOCK_INMEM --
 *	Block manager handle for per-file in_memory objects (no backing data file).
 */
struct __wt_block_inmem {
    /*
     * The fields up to and including hashq must match the layout of WT_BLOCK exactly. The
     * shared-prefix invariant is enforced by static_asserts in verify_build.h.
     */
    const char *name;  /* Name */
    uint32_t objectid; /* Object id */
    uint32_t ref;      /* References */

    TAILQ_ENTRY(__wt_block) q;     /* Linked list of handles */
    TAILQ_ENTRY(__wt_block) hashq; /* Hashed list of handles */

    uint64_t tableid;
};
