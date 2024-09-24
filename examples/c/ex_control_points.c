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
 *
 * ex_control_points.c
 *	This is an example demonstrating how to define and wait for a control point.
 */
#include "test_util.h"

static const char *home;

#define NUM_THREADS 10

struct thread_arguments {
    WT_CONNECTION *conn;
    int thread_num;
    WT_CONTROL_POINT_ID wait_for_id;
    WT_CONTROL_POINT_ID my_id;
};

/*! Thread that prints */
static WT_THREAD_RET
print_thread(void *thread_arg)
{
    struct thread_arguments *args = thread_arg;
    WT_CONNECTION *conn = args->conn;
    WT_SESSION *session;
    WT_SESSION_IMPL *session_impl;
    WT_RAND_STATE rnd_state;
    uint32_t rnd_num1;
    uint32_t seconds;
    uint32_t rnd_num2;
    uint32_t microseconds;
    bool enabled;

    /* Initialize */
    error_check(conn->open_session(conn, NULL, NULL, &session));
    session_impl = (WT_SESSION_IMPL *)session;
    __wt_random_init_seed(session_impl, &rnd_state);

    /* Wait for main or the previous thread. */
    CONNECTION_CONTROL_POINT_WAIT_FOR_TRIGGER(session_impl, args->wait_for_id, enabled);

    /* Sleep a random time. */
    rnd_num1 = __wt_random(&rnd_state);
    seconds = rnd_num1 % 5;
    rnd_num2 = __wt_random(&rnd_state);
    microseconds = rnd_num2 % WT_MILLION;
    __wt_sleep(seconds, microseconds);

    printf("Thread %d, wait_for_id %" PRId32 ", my_id %" PRId32 ", enabled %c. Slept %" PRIu32
           " seconds, %" PRIu32 " microseconds\n",
      args->thread_num, args->wait_for_id, args->my_id, enabled ? '1' : '0', seconds, microseconds);
    fflush(stdout);

    /* Finished. Signal the next thread which waits for this thread to get here. */
    CONNECTION_CONTROL_POINT_DEFINE_WAIT_FOR_TRIGGER(session_impl, args->my_id);

    /* Cleanup */
    error_check(session->close(session, NULL));

    return (0);
}

int
main(int argc, char *argv[])
{
    WT_CONNECTION *conn;
    WT_SESSION *session;
    WT_SESSION_IMPL *session_impl;
    wt_thread_t threads[NUM_THREADS];
    struct thread_arguments thread_args[NUM_THREADS];
    int idx;
    const WT_CONTROL_POINT_ID thread_control_point_ids[NUM_THREADS] = {
      WT_CONN_CONTROL_POINT_ID_THREAD0,
      WT_CONN_CONTROL_POINT_ID_THREAD1,
      WT_CONN_CONTROL_POINT_ID_THREAD2,
      WT_CONN_CONTROL_POINT_ID_THREAD3,
      WT_CONN_CONTROL_POINT_ID_THREAD4,
      WT_CONN_CONTROL_POINT_ID_THREAD5,
      WT_CONN_CONTROL_POINT_ID_THREAD6,
      WT_CONN_CONTROL_POINT_ID_THREAD7,
      WT_CONN_CONTROL_POINT_ID_THREAD8,
      WT_CONN_CONTROL_POINT_ID_THREAD9,
    };
    bool enabled;

    /* Setup */
    home = example_setup(argc, argv);

    error_check(wiredtiger_open(home, NULL, "create", &conn));
    error_check(conn->open_session(conn, NULL, NULL, &session));
    session_impl = (WT_SESSION_IMPL *)session;

    /* Enable all control points. */
    error_check(
      __wt_conn_control_point_enable(session, WT_CONN_CONTROL_POINT_ID_MainStartPrinting));
    for (idx = 0; idx < NUM_THREADS; ++idx)
        error_check(__wt_conn_control_point_enable(session, thread_control_point_ids[idx]));

    /* Start all threads */
    for (idx = 0; idx < NUM_THREADS; ++idx) {
        struct thread_arguments *my_args = &(thread_args[idx]);
        my_args->conn = conn;
        my_args->thread_num = idx;
        my_args->wait_for_id = ((idx == 0) ? WT_CONN_CONTROL_POINT_ID_MainStartPrinting :
                                             thread_control_point_ids[idx - 1]);
        my_args->my_id = thread_control_point_ids[idx];

        error_check(__wt_thread_create(NULL, &threads[idx], print_thread, &(thread_args[idx])));
    }

    /* Signal threads[0] which waits for this thread to get here. */
    CONNECTION_CONTROL_POINT_DEFINE_WAIT_FOR_TRIGGER(
      session_impl, WT_CONN_CONTROL_POINT_ID_MainStartPrinting);

    /* This thread waits for threads[NUM_THREADS - 1] to finish. */
    CONNECTION_CONTROL_POINT_WAIT_FOR_TRIGGER(
      session_impl, thread_control_point_ids[NUM_THREADS - 1], enabled);
    WT_UNUSED(enabled);

    /* Join all threads */
    for (idx = 0; idx < NUM_THREADS; ++idx)
        error_check(__wt_thread_join(NULL, &threads[idx]));

    /*
     * Cleanup
     */
    /* Disable all control points. */
    error_check(
      __wt_conn_control_point_disable(session, WT_CONN_CONTROL_POINT_ID_MainStartPrinting));
    for (idx = 0; idx < NUM_THREADS; ++idx)
        error_check(__wt_conn_control_point_disable(session, thread_control_point_ids[idx]));

    /* Close session and connection. */
    error_check(session->close(session, NULL));
    error_check(conn->close(conn, NULL));

    return (EXIT_SUCCESS);
}
