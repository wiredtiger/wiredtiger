#pragma once

/*
 * __wt_abort --
 *     Abort the process, dropping core.
 */
extern void __wt_abort(WT_SESSION_IMPL *session) WT_GCC_FUNC_DECL_ATTRIBUTE((noreturn))
  WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
    WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_calloc --
 *     ANSI calloc function.
 */
extern int __wt_calloc(WT_SESSION_IMPL *session, size_t number, size_t size, void *retp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
    WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_close --
 *     Close a file handle.
 */
extern int __wt_close(WT_SESSION_IMPL *session, WT_FH **fhp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_close_connection_close --
 *     Close any open file handles at connection close.
 */
extern int __wt_close_connection_close(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_copy_and_sync --
 *     Copy a file safely.
 */
extern int __wt_copy_and_sync(WT_SESSION *wt_session, const char *from, const char *to)
  WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
    WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_errno --
 *     Return errno, or WT_ERROR if errno not set.
 */
extern int __wt_errno(void) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_ext_map_windows_error --
 *     Extension API call to map a Windows system error to a POSIX/ANSI error.
 */
extern int __wt_ext_map_windows_error(WT_EXTENSION_API *wt_api, WT_SESSION *wt_session,
  uint32_t windows_error) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_file_zero --
 *     Zero out the file from offset for size bytes.
 */
extern int __wt_file_zero(WT_SESSION_IMPL *session, WT_FH *fh, wt_off_t start_off, wt_off_t size,
  WT_THROTTLE_TYPE type) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_filename --
 *     Build a file name in a scratch buffer, automatically calculate the length of the file name.
 */
extern int __wt_filename(WT_SESSION_IMPL *session, const char *name, char **path)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_filename_construct --
 *     Given unique identifiers, return a WT_ITEM of a generated file name of the given prefix type.
 *     Any identifier that is 0 will be skipped.
 */
extern int __wt_filename_construct(WT_SESSION_IMPL *session, const char *path,
  const char *file_prefix, uintmax_t id_1, uint32_t id_2, WT_ITEM *buf)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_fopen --
 *     Open a stream handle.
 */
extern int __wt_fopen(WT_SESSION_IMPL *session, const char *name, uint32_t open_flags,
  uint32_t flags, WT_FSTREAM **fstrp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_free_int --
 *     ANSI free function.
 */
extern void __wt_free_int(WT_SESSION_IMPL *session, const void *p_arg)
  WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
    WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_fsync_background --
 *     Background fsync for all dirty file handles.
 */
extern int __wt_fsync_background(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_fsync_background_chk --
 *     Return if background fsync is supported.
 */
extern bool __wt_fsync_background_chk(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_getopt --
 *     Parse argc/argv argument vector.
 */
extern int __wt_getopt(const char *progname, int nargc, char *const *nargv, const char *ostr)
  WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
    WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_handle_is_open --
 *     Return if there's an open handle matching a name.
 */
extern bool __wt_handle_is_open(WT_SESSION_IMPL *session, const char *name, bool locked)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_malloc --
 *     ANSI malloc function.
 */
extern int __wt_malloc(WT_SESSION_IMPL *session, size_t bytes_to_allocate, void *retp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_memdup --
 *     Duplicate a byte string of a given length.
 */
extern int __wt_memdup(WT_SESSION_IMPL *session, const void *str, size_t len, void *retp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_open --
 *     Open a file handle.
 */
extern int __wt_open(WT_SESSION_IMPL *session, const char *name, WT_FS_OPEN_FILE_TYPE file_type,
  u_int flags, WT_FH **fhp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_os_inmemory --
 *     Initialize an in-memory configuration.
 */
extern int __wt_os_inmemory(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_os_stdio --
 *     Initialize the stdio configuration.
 */
extern void __wt_os_stdio(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_realloc --
 *     WiredTiger's realloc API.
 */
extern int __wt_realloc(WT_SESSION_IMPL *session, size_t *bytes_allocated_ret,
  size_t bytes_to_allocate, void *retp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_realloc_aligned --
 *     ANSI realloc function that aligns to buffer boundaries, configured with the
 *     "buffer_alignment" key to wiredtiger_open.
 */
extern int __wt_realloc_aligned(WT_SESSION_IMPL *session, size_t *bytes_allocated_ret,
  size_t bytes_to_allocate, void *retp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_realloc_noclear --
 *     WiredTiger's realloc API, not clearing allocated memory.
 */
extern int __wt_realloc_noclear(WT_SESSION_IMPL *session, size_t *bytes_allocated_ret,
  size_t bytes_to_allocate, void *retp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_remove_if_exists --
 *     Remove a file if it exists and return error if WT_CONN_READONLY is set.
 */
extern int __wt_remove_if_exists(WT_SESSION_IMPL *session, const char *name, bool durable)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_remove_locked --
 *     While locked, if the handle is not open, remove the local file.
 */
extern int __wt_remove_locked(WT_SESSION_IMPL *session, const char *name, bool *removed)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_strerror --
 *     WT_SESSION.strerror and wiredtiger_strerror.
 */
extern const char *__wt_strerror(WT_SESSION_IMPL *session, int error, char *errbuf, size_t errlen)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_strndup --
 *     ANSI strndup function.
 */
extern int __wt_strndup(WT_SESSION_IMPL *session, const void *str, size_t len, void *retp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_strtouq --
 *     Convert a string to an unsigned quad integer.
 */
extern uint64_t __wt_strtouq(const char *nptr, char **endptr, int base)
  WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
    WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#ifdef HAVE_UNITTEST

#endif
