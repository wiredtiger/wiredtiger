/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 * All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

/* DO NOT EDIT: automatically built by prototypes.py: BEGIN */

extern int __wti_log_acquire(WT_SESSION_IMPL *session, uint64_t recsize, WT_LOGSLOT *slot)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_log_allocfile(WT_SESSION_IMPL *session, uint32_t lognum, const char *dest)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_log_close(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_log_extract_lognum(WT_SESSION_IMPL *session, const char *name, uint32_t *id)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_log_fill(WT_SESSION_IMPL *session, WT_MYSLOT *myslot, bool force, WT_ITEM *record,
  WT_LSN *lsnp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_log_force_write(WT_SESSION_IMPL *session, bool retry, bool *did_work)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_log_open(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_log_recover_prevlsn(WT_SESSION_IMPL *session, const uint8_t **pp,
  const uint8_t *end, WT_LSN *lsnp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_log_release(WT_SESSION_IMPL *session, WT_LOGSLOT *slot, bool *freep)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_log_remove(WT_SESSION_IMPL *session, const char *file_prefix, uint32_t lognum)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_log_set_version(WT_SESSION_IMPL *session, uint16_t version, uint32_t first_rec,
  bool downgrade, bool live_chg, uint32_t *lognump)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_log_slot_destroy(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_log_slot_init(WT_SESSION_IMPL *session, bool alloc)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_log_slot_switch(WT_SESSION_IMPL *session, WT_MYSLOT *myslot, bool retry,
  bool forced, bool *did_work) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_log_system_prevlsn(WT_SESSION_IMPL *session, WT_FH *log_fh, WT_LSN *lsn)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int64_t __wti_log_slot_release(WT_MYSLOT *myslot, int64_t size)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern void __wti_log_slot_activate(WT_SESSION_IMPL *session, WT_LOGSLOT *slot);
extern void __wti_log_slot_free(WT_SESSION_IMPL *session, WT_LOGSLOT *slot);
extern void __wti_log_slot_join(
  WT_SESSION_IMPL *session, uint64_t mysize, uint32_t flags, WT_MYSLOT *myslot);
extern void __wti_log_wrlsn(WT_SESSION_IMPL *session, int *yield);

#ifdef HAVE_UNITTEST

#endif

/* DO NOT EDIT: automatically built by prototypes.py: END */
