/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#ifndef __WIREDTIGER_ERROR_MODULE_H_
#define __WIREDTIGER_ERROR_MODULE_H_

#include "misc_module.h"

/*
 * Branch prediction hints. If an expression is likely to return true/false we can use this
 * information to improve performance at runtime. This is not supported for MSVC compilers.
 */
#if !defined(_MSC_VER)
#define LIKELY(x) __builtin_expect(!!(x), 1)
#define UNLIKELY(x) __builtin_expect(!!(x), 0)
#else
#define LIKELY(x) (x)
#define UNLIKELY(x) (x)
#endif

/*
 * WT_ASSERT --
 *  Assert an expression and abort if it fails.
 *  Only enabled when compiled with HAVE_DIAGNOSTIC=1.
 */
#ifdef HAVE_DIAGNOSTIC
#define WT_ASSERT(session, exp)                                       \
    do {                                                              \
        if (UNLIKELY(!(exp)))                                         \
            TRIGGER_ABORT(session, exp, "Expression returned false"); \
    } while (0)
#else
#define WT_ASSERT(session, exp) WT_UNUSED(session)
#endif

#endif /* __WIREDTIGER_ERROR_MODULE_H_ */
