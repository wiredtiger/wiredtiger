#pragma once

/*
 * __wt_compact_check_eligibility --
 *     Function to check whether the specified URI is eligible for compaction.
 */
extern bool __wt_compact_check_eligibility(WT_SESSION_IMPL *session, const char *uri)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_open_cursor --
 *     Internal version of WT_SESSION::open_cursor.
 */
extern int __wt_open_cursor(WT_SESSION_IMPL *session, const char *uri, WT_CURSOR *owner,
  const char *cfg[], WT_CURSOR **cursorp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_open_internal_session --
 *     Allocate a session for WiredTiger's use.
 */
extern int __wt_open_internal_session(WT_CONNECTION_IMPL *conn, const char *name,
  bool open_metadata, uint32_t session_flags, uint32_t session_lock_flags,
  WT_SESSION_IMPL **sessionp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_open_session --
 *     Allocate a session handle.
 */
extern int __wt_open_session(WT_CONNECTION_IMPL *conn, WT_EVENT_HANDLER *event_handler,
  const char *config, bool open_metadata, WT_SESSION_IMPL **sessionp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_session_array_walk --
 *     Walk the connections session array, calling a function for every active session in the array.
 *     Callers can exit the walk early if desired. Arguments to the walk function are provided by a
 *     customizable cookie.
 *
 * The walk itself cannot fail, if the callback function can't error out then the call to this
 *     function should be wrapped in an ignore return macro.
 */
extern int __wt_session_array_walk(WT_SESSION_IMPL *session,
  int (*walk_func)(WT_SESSION_IMPL *, WT_SESSION_IMPL *, bool *exit_walkp, void *cookiep),
  bool skip_internal, void *cookiep) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_session_breakpoint --
 *     A place to put a breakpoint, if you need one, or call some check code.
 */
extern int __wt_session_breakpoint(WT_SESSION *wt_session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_session_close_cache --
 *     Close any cached handles in a session.
 */
extern void __wt_session_close_cache(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_session_close_internal --
 *     Internal function of WT_SESSION->close method.
 */
extern int __wt_session_close_internal(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_session_compact_check_interrupted --
 *     Check if compaction has been interrupted. Foreground compaction can be interrupted through an
 *     event handler while background compaction can be disabled at any time using the compact API.
 */
extern int __wt_session_compact_check_interrupted(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_session_copy_values --
 *     Copy values into all positioned cursors, so that they don't keep transaction IDs pinned.
 */
extern int __wt_session_copy_values(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_session_create --
 *     Internal version of WT_SESSION::create.
 */
extern int __wt_session_create(WT_SESSION_IMPL *session, const char *uri, const char *config)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_session_cursor_cache_sweep --
 *     Sweep the cursor cache.
 */
extern int __wt_session_cursor_cache_sweep(WT_SESSION_IMPL *session, bool big_sweep)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_session_dhandle_sweep --
 *     Discard any session dhandles that are not open.
 */
extern void __wt_session_dhandle_sweep(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_session_dhandle_try_writelock --
 *     Try to acquire write lock for the session's current dhandle.
 */
extern int __wt_session_dhandle_try_writelock(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_session_dhandle_writeunlock --
 *     Release write lock for the session's current dhandle.
 */
extern void __wt_session_dhandle_writeunlock(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_session_dump --
 *     Given a session dump information about that session. The caller session's scratch memory and
 *     event handler is used.
 */
extern int __wt_session_dump(WT_SESSION_IMPL *session, WT_SESSION_IMPL *dump_session,
  bool show_cursors) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_session_get_btree_ckpt --
 *     Check the configuration strings for a checkpoint name. If opening a checkpoint, resolve the
 *     checkpoint name, get a btree handle for it, load that into the session, and if requested with
 *     non-null pointers, also resolve a matching history store checkpoint, open a handle for it,
 *     return that, and also find and return the corresponding snapshot/timestamp metadata. The
 *     transactions array in the snapshot info is allocated and must be freed by the caller on
 *     success. If not opening a checkpoint, the history store dhandle and snapshot info is
 *     immaterial; if the return pointers are not null, send back nulls and in particular never
 *     allocate or open anything.
 */
extern int __wt_session_get_btree_ckpt(WT_SESSION_IMPL *session, const char *uri, const char *cfg[],
  uint32_t flags, WT_DATA_HANDLE **hs_dhandlep, WT_CKPT_SNAPSHOT *ckpt_snapshot)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_session_get_dhandle --
 *     Get a data handle for the given name, set session->dhandle. Optionally if we opened a
 *     checkpoint return its checkpoint order number.
 */
extern int __wt_session_get_dhandle(WT_SESSION_IMPL *session, const char *uri,
  const char *checkpoint, const char *cfg[], uint32_t flags)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_session_lock_checkpoint --
 *     Lock the btree handle for the given checkpoint name.
 */
extern int __wt_session_lock_checkpoint(WT_SESSION_IMPL *session, const char *checkpoint)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_session_lock_dhandle --
 *     Return when the current data handle is either (a) open with the requested lock mode; or (b)
 *     closed and write locked. If exclusive access is requested and cannot be granted immediately
 *     because the handle is in use, fail with EBUSY. Here is a brief summary of how different
 *     operations synchronize using either the schema lock, handle locks or handle flags: open --
 *     one thread gets the handle exclusive, reverts to a shared handle lock once the handle is
 *     open; bulk load --
 *     sets bulk and exclusive; salvage, truncate, update, verify --
 *     hold the schema lock, get the handle exclusive, set a "special" flag; sweep --
 *     gets a write lock on the handle, doesn't set exclusive The principle is that some application
 *     operations can cause other application operations to fail (so attempting to open a cursor on
 *     a file while it is being bulk-loaded will fail), but internal or database-wide operations
 *     should not prevent application-initiated operations. For example, attempting to verify a file
 *     should not fail because the sweep server happens to be in the process of closing that file.
 */
extern int __wt_session_lock_dhandle(WT_SESSION_IMPL *session, uint32_t flags, bool *is_deadp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_session_prefetch_check --
 *     Check if pre-fetching work should be performed for a given ref.
 */
extern bool __wt_session_prefetch_check(WT_SESSION_IMPL *session, WT_REF *ref)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_session_range_truncate --
 *     Session handling of a range truncate.
 */
extern int __wt_session_range_truncate(WT_SESSION_IMPL *session, const char *uri, WT_CURSOR *start,
  WT_CURSOR *stop) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_session_release_dhandle --
 *     Unlock a data handle.
 */
extern int __wt_session_release_dhandle(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_session_release_dhandle_v2 --
 *     Unlock a data handle.
 */
extern int __wt_session_release_dhandle_v2(WT_SESSION_IMPL *session, bool check_visibility)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_session_release_resources --
 *     Release common session resources.
 */
extern int __wt_session_release_resources(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_session_reset_cursors --
 *     Reset all open cursors.
 */
extern int __wt_session_reset_cursors(WT_SESSION_IMPL *session, bool free_buffers)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_session_strerror --
 *     WT_SESSION->strerror method.
 */
extern const char *__wt_session_strerror(WT_SESSION *wt_session, int error)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#ifdef HAVE_UNITTEST

#endif
