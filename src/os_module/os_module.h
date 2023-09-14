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

#include "../include/gcc_decl.h"
#include <stdio.h>

typedef struct __wt_session_impl WT_SESSION_IMPL;

extern int test_mod(void);
extern int __wt_calloc(WT_SESSION_IMPL *session, size_t number, size_t size, void *retp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
    WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern void __wt_free_int(WT_SESSION_IMPL *session, const void *p_arg)
  WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")));
extern int __wt_malloc(WT_SESSION_IMPL *session, size_t bytes_to_allocate, void *retp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_memdup(WT_SESSION_IMPL *session, const void *str, size_t len, void *retp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_realloc(WT_SESSION_IMPL *session, size_t *bytes_allocated_ret,
  size_t bytes_to_allocate, void *retp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_realloc_aligned(WT_SESSION_IMPL *session, size_t *bytes_allocated_ret,
  size_t bytes_to_allocate, void *retp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_realloc_noclear(WT_SESSION_IMPL *session, size_t *bytes_allocated_ret,
  size_t bytes_to_allocate, void *retp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_strndup(WT_SESSION_IMPL *session, const void *str, size_t len, void *retp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#if defined(__cplusplus)
}
#endif
#endif /* __WIREDTIGER_OS_MODULE_H_ */
