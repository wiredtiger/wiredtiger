/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * WT_VERIFY_INFO -- A structure to hold all the information related to a verify operation.
 */
struct __wt_verify_info {
    WT_SESSION_IMPL *session;

/* AUTOMATIC FLAG VALUE GENERATION START 12 */
#define WT_VRFY_DISK_CONTINUE_ON_FAILURE 0x1000u
#define WT_VRFY_EMPTY_PAGE_OK 0x2000u
    /* AUTOMATIC FLAG VALUE GENERATION STOP 32 */
    uint32_t flags;

    const char *tag;
    uint32_t cell_num;
    size_t item_size;
    WT_ADDR *item_addr;
    const WT_PAGE_HEADER *dsk;
    uint64_t recno;
};
