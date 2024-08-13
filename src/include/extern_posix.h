#pragma once

/*
 * __wt_absolute_path --
 *     Return if a filename is an absolute path.
 */
extern bool __wt_absolute_path(const char *path) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cond_alloc --
 *     Allocate and initialize a condition variable.
 */
extern int __wt_cond_alloc(WT_SESSION_IMPL *session, const char *name, WT_CONDVAR **condp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cond_destroy --
 *     Destroy a condition variable.
 */
extern void __wt_cond_destroy(WT_SESSION_IMPL *session, WT_CONDVAR **condp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cond_signal --
 *     Signal a waiting thread.
 */
extern void __wt_cond_signal(WT_SESSION_IMPL *session, WT_CONDVAR *cond)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cond_wait_signal --
 *     Wait on a mutex, optionally timing out. If we get it before the time out period expires, let
 *     the caller know.
 */
extern void __wt_cond_wait_signal(WT_SESSION_IMPL *session, WT_CONDVAR *cond, uint64_t usecs,
  bool (*run_func)(WT_SESSION_IMPL *), bool *signalled)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_dlclose --
 *     Close a dynamic library
 */
extern int __wt_dlclose(WT_SESSION_IMPL *session, WT_DLH *dlh)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_dlopen --
 *     Open a dynamic library.
 */
extern int __wt_dlopen(WT_SESSION_IMPL *session, const char *path, WT_DLH **dlhp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_dlsym --
 *     Lookup a symbol in a dynamic library.
 */
extern int __wt_dlsym(WT_SESSION_IMPL *session, WT_DLH *dlh, const char *name, bool fail,
  void *sym_ret) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_epoch_raw --
 *     Return the time since the Epoch as reported by a system call.
 */
extern void __wt_epoch_raw(WT_SESSION_IMPL *session, struct timespec *tsp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
    WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_get_vm_pagesize --
 *     Return the default page size of a virtual memory page.
 */
extern int __wt_get_vm_pagesize(void) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_getenv --
 *     Get a non-NULL, greater than zero-length environment variable.
 */
extern int __wt_getenv(WT_SESSION_IMPL *session, const char *variable, const char **envp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
    WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_has_priv --
 *     Return if the process has special privileges, defined as having different effective and read
 *     UIDs or GIDs.
 */
extern bool __wt_has_priv(void) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_localtime --
 *     Return the current local broken-down time.
 */
extern int __wt_localtime(WT_SESSION_IMPL *session, const time_t *timep, struct tm *result)
  WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
    WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_once --
 *     One-time initialization per process.
 */
extern int __wt_once(void (*init_routine)(void)) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_os_posix --
 *     Initialize a POSIX configuration.
 */
extern int __wt_os_posix(WT_SESSION_IMPL *session) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_path_separator --
 *     Return the path separator string.
 */
extern const char *__wt_path_separator(void) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_process_id --
 *     Return the process ID assigned by the operating system.
 */
extern uintmax_t __wt_process_id(void) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_sleep --
 *     Pause the thread of control.
 */
extern void __wt_sleep(uint64_t seconds, uint64_t micro_seconds)
  WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
    WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_stream_set_line_buffer --
 *     Set line buffering on a stream.
 */
extern void __wt_stream_set_line_buffer(FILE *fp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
    WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_stream_set_no_buffer --
 *     Turn off buffering on a stream.
 */
extern void __wt_stream_set_no_buffer(FILE *fp) WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_thread_create --
 *     Create a new thread of control.
 */
extern int __wt_thread_create(WT_SESSION_IMPL *session, wt_thread_t *tidret,
  WT_THREAD_CALLBACK (*func)(void *), void *arg) WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_thread_id --
 *     Return an arithmetic representation of a thread ID on POSIX.
 */
extern void __wt_thread_id(uintmax_t *id) WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_thread_join --
 *     Wait for a thread of control to exit.
 */
extern int __wt_thread_join(WT_SESSION_IMPL *session, wt_thread_t *tid)
  WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
    WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_thread_str --
 *     Fill in a printable version of the process and thread IDs.
 */
extern int __wt_thread_str(char *buf, size_t buflen)
  WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
    WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_vsnprintf_len_incr --
 *     POSIX vsnprintf convenience function, incrementing the returned size.
 */
extern int __wt_vsnprintf_len_incr(char *buf, size_t size, size_t *retsizep, const char *fmt,
  va_list ap) WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_yield --
 *     Yield the thread of control.
 */
extern void __wt_yield(void) WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_yield_no_barrier --
 *     Yield the thread of control. Don't set any memory barriers as this may hide memory
 *     synchronization errors in the surrounding code. It's not explicitly documented that yielding
 *     without a memory barrier is safe, so this function should only be used for testing in
 *     diagnostic mode.
 */
extern void __wt_yield_no_barrier(void) WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

extern int __wti_posix_directory_list(WT_FILE_SYSTEM *file_system, WT_SESSION *wt_session,
  const char *directory, const char *prefix, char ***dirlistp, uint32_t *countp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_posix_directory_list_free(WT_FILE_SYSTEM *file_system, WT_SESSION *wt_session,
  char **dirlist, uint32_t count) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_posix_directory_list_single(WT_FILE_SYSTEM *file_system, WT_SESSION *wt_session,
  const char *directory, const char *prefix, char ***dirlistp, uint32_t *countp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_posix_file_extend(WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session,
  wt_off_t offset) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_posix_map(WT_FILE_HANDLE *fh, WT_SESSION *wt_session, void **mapped_regionp,
  size_t *lenp, void **mapped_cookiep) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_posix_map_discard(WT_FILE_HANDLE *fh, WT_SESSION *wt_session, void *map,
  size_t length, void *mapped_cookie) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_posix_map_preload(WT_FILE_HANDLE *fh, WT_SESSION *wt_session, const void *map,
  size_t length, void *mapped_cookie) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_posix_unmap(WT_FILE_HANDLE *fh, WT_SESSION *wt_session, void *mapped_region,
  size_t len, void *mapped_cookie) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern void __wti_posix_prepare_remap_resize_file(
  WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session, wt_off_t len, bool *remap);
extern void __wti_posix_release_without_remap(WT_FILE_HANDLE *file_handle);
extern void __wti_posix_remap_resize_file(WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session);

#ifdef HAVE_UNITTEST

#endif
