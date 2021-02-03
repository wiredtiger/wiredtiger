/*-
 * Copyright (c) 2014-2020 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * WT_STORAGE_MANAGER --
 *	A structure that holds resources used to manage any shared storage
 *	for the whole database.
 */
struct __wt_storage_manager {
    uint64_t object_size; /* Ideal object size */
    uint64_t wait_usecs;  /* Wait time period */
    uint32_t workers;     /* Current number of LSM workers */
    uint32_t workers_max;
    uint32_t workers_min;

#define WT_STORAGE_MAX_WORKERS 20
#define WT_STORAGE_MIN_WORKERS 1

/* AUTOMATIC FLAG VALUE GENERATION START */
#define WT_STORAGE_MANAGER_SHUTDOWN 0x1u /* Manager has shut down */
                                         /* AUTOMATIC FLAG VALUE GENERATION STOP */
    uint32_t flags;
};
