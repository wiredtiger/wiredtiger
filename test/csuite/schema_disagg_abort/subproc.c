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

#include "test_util.h"

#include "subproc.h"

#include <signal.h>
#include <spawn.h>
#include <sys/wait.h>

#if !defined(__GLIBC__)
/* POSIX specifies the symbol but not a header declaration; GLIBC declares it in <unistd.h>. */
extern char **environ;
#endif

/*
 * subproc_pipe --
 *     Create the leader-to-follower event pipe.
 */
void
subproc_pipe(int fds[2])
{
    testutil_assert_errno(pipe(fds) == 0);
}

/*
 * subproc_self_path --
 *     Resolve the running binary's path for re-spawning.
 */
void
subproc_self_path(const char *argv0, char *buf, size_t buf_size)
{
    char resolved[PATH_MAX];

    /* argv0 works as-is when realpath fails (e.g., the binary was found via PATH). */
    const char *path = realpath(argv0, resolved) != NULL ? resolved : argv0;
    testutil_assert(strlen(path) < buf_size);
    strcpy(buf, path);
}

/*
 * subproc_spawn --
 *     Start a child process running the given binary.
 */
void
subproc_spawn(SUBPROC *proc, const char *who, const char *path, char *const argv[])
{
    memset(proc, 0, sizeof(*proc));
    proc->who = who;

    const int ret = posix_spawn(&proc->pid, path, NULL, NULL, argv, environ);
    if (ret != 0)
        testutil_die(ret, "posix_spawn %s: %s", who, path);
}

/*
 * subproc_kill --
 *     Terminate a child abruptly, with no chance for cleanup.
 */
void
subproc_kill(SUBPROC *proc)
{
    testutil_assertfmt(!proc->reaped, "%s: killing an already reaped child", proc->who);
    if (kill(proc->pid, SIGKILL) != 0)
        testutil_die(errno, "kill %s", proc->who);
}

/*
 * subproc_decode --
 *     Translate a raw wait status into the portable status/code pair.
 */
static SUBPROC_STATUS
subproc_decode(int status, int *codep)
{
    if (WIFEXITED(status)) {
        *codep = WEXITSTATUS(status);
        return (SUBPROC_EXITED);
    }
    testutil_assert(WIFSIGNALED(status));
    *codep = WTERMSIG(status);
    return (SUBPROC_KILLED);
}

/*
 * subproc_poll --
 *     Non-blocking status probe.
 */
SUBPROC_STATUS
subproc_poll(SUBPROC *proc, int *codep)
{
    if (!proc->reaped) {
        int status;
        const pid_t got = waitpid(proc->pid, &status, WNOHANG);
        testutil_assert_errno(got != -1);
        if (got == 0)
            return (SUBPROC_RUNNING);
        proc->reaped = true;
        proc->status = status;
    }
    return (subproc_decode(proc->status, codep));
}

/*
 * subproc_wait --
 *     Blocking wait for termination.
 */
SUBPROC_STATUS
subproc_wait(SUBPROC *proc, int *codep)
{
    if (!proc->reaped) {
        int status;
        testutil_assert_errno(waitpid(proc->pid, &status, 0) != -1);
        proc->reaped = true;
        proc->status = status;
    }
    return (subproc_decode(proc->status, codep));
}
