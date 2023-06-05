/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

#include <threads.h>

static thread_local __wt_ctx_cmp __ctx_cmp;
static thread_local void *__ctx_arg;

/*
 * Redirection function has compatible signature for qsort but 
 * acts a thunker for __wt_qsort_r().
 */
static int __ctx_qsort_cmp(const void *lhs, const void *rhs)
{
    return __ctx_cmp(lhs, rhs, __ctx_arg);
}

/*
 * Wrapper for qsort(3) that can take an optional context argument, accessible
 * in all comparator invocations. Sorting and comparator evaluation is 
 * consistent with qsort.
 * 
 * \param base Must not be NULL if count > 0.
 * \param count If zero: no arguments will be evaluated.
 * \param esize Must be greater than zero: else behavior is undefined.
 * \param cmp Must be a valid function pointer: else behavior is undefined.
 * \param ctx May be NULL.
 * 
 * \note May be called recursively, in that the comparator function may itself
 * __wt_qsort_r.
 * 
 * \warning Cannot be used in signal handlers or for use with setjmp.
 */
void
__wt_qsort_r(void *base, size_t count, size_t esize, __wt_ctx_cmp cmp, void *ctx)
{
    __wt_ctx_cmp cmp_entry;
    void *arg_entry;

    WT_ASSERT(NULL, esize > 0);
    WT_ASSERT(NULL, cmp != NULL);
 
    if (count == 0) 
        return;

    WT_ASSERT(NULL, base != NULL);

    cmp_entry = __ctx_cmp;
    arg_entry = __ctx_arg;
    __ctx_cmp = cmp;
    __ctx_arg = ctx;
    qsort(base, count, esize, __ctx_qsort_cmp);
    __ctx_cmp = cmp_entry;
    __ctx_arg = arg_entry;
}