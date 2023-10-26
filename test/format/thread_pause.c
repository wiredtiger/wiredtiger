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

#include "format.h"

/*
 * FIXME-thread-pause There's a lot of white boxing here where we reach into the SESSION_IMPL and
 * CONNECTION_IMPL structs. The alternative is a handful of new API functions but I don't like that
 * idea.
 */

/*
 * thread_pause --
 *     Randomly pick active threads as tracked in the connections thread_registry and pause them for
 *     a brief period of time. The OS won't let me play with the scheduler so we use pthread signals
 *     instead.
 */
WT_THREAD_RET
thread_pause(void *arg)
{
    WT_CONNECTION_IMPL *conn;
    WT_SESSION_IMPL *session;
    int ret;

    WT_RAND_STATE rnd;
    uint32_t rand_index;
    wt_thread_t *rand_thread;
    char thread_name[WT_THREAD_NAME_MAX_LEN];

    session = (WT_SESSION_IMPL *)arg;
    conn = S2C(session);

    __wt_random_init_seed(NULL, &rnd);

    while (!g.workers_finished) {
        __wt_readlock(session, &conn->internal_thread_registry_lock);

        /* The thread registry array can have holes. Keep trying until we find something */
        rand_thread = NULL;
        while (rand_thread == NULL && !g.workers_finished) {
            rand_index = __wt_random(&rnd) % conn->internal_thread_registry_size;
            rand_thread = conn->internal_thread_registry[rand_index];
        }

        pthread_getname_np(rand_thread->id, thread_name, WT_THREAD_NAME_MAX_LEN);

        /* FIXME-thread-pause - Need to drop these ANSI escape codes. */

        /*
         * Printing out the paused thread interferes with the usual trace output that shows the
         * current number of ops performed. Begin with an ANSI escape code to clear the current line
         */
        printf("\33[2K\r Pausing thread %s \n", thread_name);

        /*
         * pthread_kill doesn't actually kill the thread, it just sends a signal of our choice to
         * the target thread. This is the only way with pthreads to signal a specific thread and not
         * the process, which will select a random thread to handle the signal.
         */
        ret = pthread_kill(rand_thread->id, SIGUSR1);
        if (ret != 0)
            printf("\33[2K\r    pthread failed to signal!!");

        __wt_readunlock(session, &conn->internal_thread_registry_lock);

        /*
         * Sleep a bit longer than the paused threads (0.05s). Make sure we're pausing at most one
         * thread at a time.
         */
        usleep(WT_THREAD_PAUSE_DURATION + 50000);
    }

    return (WT_THREAD_RET_VALUE);
}

/*
 * dump_active_threads --
 *     Dump all active threads and their names as listed in the conn->internal_thread_registry.
 */
void
dump_active_threads(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *wt_conn;

    wt_conn = S2C(session);
    __wt_readlock((WT_SESSION_IMPL *)session, &wt_conn->internal_thread_registry_lock);

    printf("Threads available for pausing:\n");
    for (uint32_t j = 0; j < wt_conn->internal_thread_registry_size; j++) {
        printf("index %3u: %p\t", j, (void *)wt_conn->internal_thread_registry[j]);
#ifdef __linux__
        if (wt_conn->internal_thread_registry[j] != NULL) {
            char buf[WT_THREAD_NAME_MAX_LEN];
            pthread_getname_np(
              wt_conn->internal_thread_registry[j]->id, buf, WT_THREAD_NAME_MAX_LEN);
            printf("    name: %s", buf);
        }
#endif
        printf("\n");
    }
    printf("\n");
    __wt_readunlock((WT_SESSION_IMPL *)session, &wt_conn->internal_thread_registry_lock);
    return;
}
