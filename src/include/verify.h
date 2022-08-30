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

    const char *tag;           /* Identifier included in error messages */
    const WT_PAGE_HEADER *dsk; /* The disk header for the page being verified */
    WT_ADDR *page_addr;        /* An item representing a page entry being verified */
    size_t page_size;
    uint32_t cell_num; /* The current cell offset being verified */
    uint64_t recno;    /* The current record number in a column store page */

/* AUTOMATIC FLAG VALUE GENERATION START 0 */
#define WT_VRFY_DISK_CONTINUE_ON_FAILURE 0x1u
#define WT_VRFY_DISK_EMPTY_PAGE_OK 0x2u
    /* AUTOMATIC FLAG VALUE GENERATION STOP 32 */
    uint32_t flags;
};
