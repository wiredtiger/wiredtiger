#pragma once

/* DO NOT EDIT: automatically built by prototypes.py: BEGIN */

extern int __wt_futex_wait(volatile WT_FUTEX_WORD *addr, WT_FUTEX_WORD expected, time_t usec,
  WT_FUTEX_WORD *wake_valp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_futex_wake(volatile WT_FUTEX_WORD *addr, WT_FUTEX_WAKE wake, WT_FUTEX_WORD wake_val)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_semaphore_destroy(WT_SESSION_IMPL *session, WT_SEMAPHORE *sem)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_semaphore_init(WT_SESSION_IMPL *session, WT_SEMAPHORE *sem, uint32_t count,
  const char *name) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_semaphore_post(WT_SESSION_IMPL *session, WT_SEMAPHORE *sem)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_semaphore_wait(WT_SESSION_IMPL *session, WT_SEMAPHORE *sem)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#ifdef HAVE_UNITTEST

#endif

/* DO NOT EDIT: automatically built by prototypes.py: END */
