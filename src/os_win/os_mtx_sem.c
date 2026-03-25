/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_semaphore_init --
 *     Initialize a semaphore.
 */
int
__wt_semaphore_init(WT_SESSION_IMPL *session, WT_SEMAPHORE *sem, uint32_t count, const char *name)
{
    DWORD windows_error;
    HANDLE handle;

    WT_CLEAR(*sem);

    handle = CreateSemaphore(NULL, (LONG)count, (LONG)INT32_MAX, NULL);
    if (handle == NULL) {
        windows_error = __wt_getlasterror();
        __wt_errx(
          session, "%s: CreateSemaphore: %s", name, __wt_formatmessage(session, windows_error));
        return (__wt_map_windows_error(windows_error));
    }

    sem->name = name;
    sem->sem = handle;
    return (0);
}

/*
 * __wt_semaphore_destroy --
 *     Destroy a semaphore.
 */
int
__wt_semaphore_destroy(WT_SESSION_IMPL *session, WT_SEMAPHORE *sem)
{
    DWORD windows_error;

    if (CloseHandle(sem->sem) == 0) {
        windows_error = __wt_getlasterror();
        __wt_errx(
          session, "%s: CloseHandle: %s", sem->name, __wt_formatmessage(session, windows_error));
        return (__wt_map_windows_error(windows_error));
    }

    WT_CLEAR(*sem);
    return (0);
}

/*
 * __wt_semaphore_post --
 *     Post a semaphore.
 */
int
__wt_semaphore_post(WT_SESSION_IMPL *session, WT_SEMAPHORE *sem)
{
    DWORD windows_error;

    if (ReleaseSemaphore(sem->sem, 1, NULL) == 0) {
        windows_error = __wt_getlasterror();
        __wt_errx(session, "%s: ReleaseSemaphore: %s", sem->name,
          __wt_formatmessage(session, windows_error));
        return (__wt_map_windows_error(windows_error));
    }

    return (0);
}

/*
 * __wt_semaphore_wait --
 *     Wait on a semaphore.
 */
int
__wt_semaphore_wait(WT_SESSION_IMPL *session, WT_SEMAPHORE *sem)
{
    DWORD wait_result, windows_error;

    wait_result = WaitForSingleObject(sem->sem, INFINITE);
    if (wait_result == WAIT_FAILED) {
        windows_error = __wt_getlasterror();
        __wt_errx(session, "%s: WaitForSingleObject: %s", sem->name,
          __wt_formatmessage(session, windows_error));
        return (__wt_map_windows_error(windows_error));
    }
    if (wait_result != WAIT_OBJECT_0) {
        /* This can happen if the wait was abandoned, or if it timed out. */
        windows_error = __wt_getlasterror();
        __wt_errx(session, "%s: WaitForSingleObject: Unexpected wait result %" PRIu32, sem->name,
          (uint32_t)wait_result);
        return (ETIMEDOUT);
    }

    return (0);
}
