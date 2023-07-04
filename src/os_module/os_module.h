/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#ifndef __WIREDTIGER_OS_MODULE_H_
#define __WIREDTIGER_OS_MODULE_H_

#if defined(__cplusplus)
extern "C" {
#endif

#include "gcc_decl.h"
#include <stdio.h>

typedef struct __wt_session_impl WT_SESSION_IMPL;

extern int test_mod(void);
extern int __wt_calloc_mod(WT_SESSION_IMPL *session, size_t number, size_t size, void *retp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
    WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#if defined(__cplusplus)
}
#endif
#endif /* __WIREDTIGER_OS_MODULE_H_ */
