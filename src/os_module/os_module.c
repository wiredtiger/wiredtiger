/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "os_module.h"
#include "error_module.h"

#include <stdlib.h>

// For now HAVE_DIAGNOSTIC compile flag is not passed to unit tests, so they always build
// modules without diagnostic - need to look into future to test modules diag also.

/*
 * test_mod --
 *     A module test function
 */
int
test_mod(void)
{
    printf("Testing.. \n");
    return (0);
}

/*
 * __wt_calloc_mod --
 *     ANSI calloc function.
 */
int
__wt_calloc_mod(/*WT_SESSION_IMPL *session, */size_t number, size_t size, void *retp)
  WT_GCC_FUNC_ATTRIBUTE((visibility("default")))
{
    void *p;

    /*
     * Defensive: if our caller doesn't handle errors correctly, ensure a free won't fail.
     */
    *(void **)retp = NULL;

    /*
     * !!!
     * This function MUST handle a NULL WT_SESSION_IMPL handle.
     */
    WT_ASSERT(NULL, number != 0 && size != 0);

    //if (session != NULL)
    //    WT_STAT_CONN_INCR(session, memory_allocation);

    if ((p = calloc(number, size)) == NULL)
        //WT_RET_MSG(session, __wt_errno(), "memory allocation of %" WT_SIZET_FMT " bytes failed",
        //  size * number);

    *(void **)retp = p;
    return (0);
}
