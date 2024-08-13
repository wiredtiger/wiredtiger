#pragma once

extern bool __wt_compact_check_eligibility(WT_SESSION_IMPL *session, const char *uri)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern bool __wt_session_prefetch_check(WT_SESSION_IMPL *session, WT_REF *ref)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern const char *__wt_session_strerror(WT_SESSION *wt_session, int error)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_open_cursor(WT_SESSION_IMPL *session, const char *uri, WT_CURSOR *owner,
  const char *cfg[], WT_CURSOR **cursorp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_open_internal_session(WT_CONNECTION_IMPL *conn, const char *name,
  bool open_metadata, uint32_t session_flags, uint32_t session_lock_flags,
  WT_SESSION_IMPL **sessionp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_open_session(WT_CONNECTION_IMPL *conn, WT_EVENT_HANDLER *event_handler,
  const char *config, bool open_metadata, WT_SESSION_IMPL **sessionp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_session_array_walk(WT_SESSION_IMPL *session,
  int (*walk_func)(WT_SESSION_IMPL *, WT_SESSION_IMPL *, bool *exit_walkp, void *cookiep),
  bool skip_internal, void *cookiep) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_session_breakpoint(WT_SESSION *wt_session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_session_close_internal(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_session_compact_check_interrupted(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_session_copy_values(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_session_create(WT_SESSION_IMPL *session, const char *uri, const char *config)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_session_cursor_cache_sweep(WT_SESSION_IMPL *session, bool big_sweep)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_session_dhandle_try_writelock(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_session_dump(WT_SESSION_IMPL *session, WT_SESSION_IMPL *dump_session,
  bool show_cursors) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_session_get_btree_ckpt(WT_SESSION_IMPL *session, const char *uri, const char *cfg[],
  uint32_t flags, WT_DATA_HANDLE **hs_dhandlep, WT_CKPT_SNAPSHOT *ckpt_snapshot)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_session_get_dhandle(WT_SESSION_IMPL *session, const char *uri,
  const char *checkpoint, const char *cfg[], uint32_t flags)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_session_lock_checkpoint(WT_SESSION_IMPL *session, const char *checkpoint)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_session_lock_dhandle(WT_SESSION_IMPL *session, uint32_t flags, bool *is_deadp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_session_range_truncate(WT_SESSION_IMPL *session, const char *uri, WT_CURSOR *start,
  WT_CURSOR *stop) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_session_release_dhandle(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_session_release_dhandle_v2(WT_SESSION_IMPL *session, bool check_visibility)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_session_release_resources(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_session_reset_cursors(WT_SESSION_IMPL *session, bool free_buffers)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern void __wt_session_close_cache(WT_SESSION_IMPL *session);
extern void __wt_session_dhandle_sweep(WT_SESSION_IMPL *session);
extern void __wt_session_dhandle_writeunlock(WT_SESSION_IMPL *session);

#ifdef HAVE_UNITTEST

#endif
