/*-
 * Public Domain 2014-present MongoDB, Inc.
 * Public Domain 2008-2014 WiredTiger, Inc.
 *
 * This is free and unencumbered software released into the public domain.
 *
 * Anyone is free to copy, modify, publish, use, compile, sell, or
 * distribute this software, either in source code form or as a compiled
 * binary, for any purpose, commercial or non-commercial, and by any
 * means.
 *
 * In jurisdictions that recognize copyright laws, the author or authors
 * of this software dedicate any and all copyright interest in the
 * software to the public domain. We make this dedication for the benefit
 * of the public at large and to the detriment of our heirs and
 * successors. We intend this dedication to be an overt act of
 * relinquishment in perpetuity of all present and future rights to this
 * software under copyright law.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

/*
 * Minimal child-process layer built on spawn-style creation, chosen so a Windows port only has to
 * substitute the CRT near-equivalents (_spawnv, _pipe, _cwait/TerminateProcess) inside subproc.c
 * without touching the callers.
 */

#pragma once

#include <stdbool.h>
#include <sys/types.h>

typedef struct {
    const char *who; /* Child's role name for diagnostics; NULL marks an unspawned slot. */
    pid_t pid;
    bool reaped;
    int status; /* Raw wait status, valid once reaped. */
} SUBPROC;

/* Fixed slots for the children a test run can have. */
#define SUBPROC_LEADER 0
#define SUBPROC_FOLLOWER 1
#define SUBPROC_SLOTS 2

typedef enum {
    SUBPROC_RUNNING,
    SUBPROC_EXITED, /* Normal exit; code is the exit status. */
    SUBPROC_KILLED  /* Abnormal termination; code is the signal number. */
} SUBPROC_STATUS;

/* Create the leader-to-follower event pipe; fds[0] is the read end. */
void subproc_pipe(int fds[2]);

/* Resolve the running binary's path for re-spawning; argv0 is used as a fallback. */
void subproc_self_path(const char *argv0, char *buf, size_t buf_size);

/* Start a child process running the given binary; the child inherits open descriptors. */
void subproc_spawn(SUBPROC *proc, const char *who, const char *path, char *const argv[]);

/* Terminate a child abruptly, with no chance for cleanup. */
void subproc_kill(SUBPROC *proc);

/* Non-blocking status probe. */
SUBPROC_STATUS subproc_poll(SUBPROC *proc, int *codep);

/* Blocking wait for termination. */
SUBPROC_STATUS subproc_wait(SUBPROC *proc, int *codep);
