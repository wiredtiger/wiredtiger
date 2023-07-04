/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#ifndef __WIREDTIGER_MISC_MODULE_H_
#define __WIREDTIGER_MISC_MODULE_H_

/*
 * Quiet compiler warnings about unused function parameters and variables, and unused function
 * return values.
 */
#define WT_UNUSED(var) (void)(var)
#define WT_IGNORE_RET(call)                \
    do {                                   \
        uintmax_t __ignored_ret;           \
        __ignored_ret = (uintmax_t)(call); \
        WT_UNUSED(__ignored_ret);          \
    } while (0)

#define WT_DECL_RET int ret = 0

#endif /* __WIREDTIGER_MISC_MODULE_H_ */
