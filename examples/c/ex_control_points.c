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

#ifdef HAVE_CONTROL_POINT

static const char *home;

#define NUM_THREADS 5

static const char *const session_open_config = "";

struct thread_arguments {
    WT_CONNECTION *conn;
    WT_SESSION *session;
    int thread_num;
    wt_control_point_id_t wait_for_id;
    wt_control_point_id_t my_id;
};
/*
 * __construct_configuration_control_point_string --
 *     Construct the configuration string for the control point.
 *
 * @param session The session. @param cp_config_name The control point's configuration name. @param
 *     cfg Configuration string.
 */
static int
__construct_configuration_control_point_string(
  WT_SESSION_IMPL *session, const char *cp_config_name, char **buf)
{
    size_t len;

    len = strlen("per_connection_control_points") + strlen(cp_config_name) + 10;

    WT_RET(__wt_calloc_def(session, len, buf));
    WT_RET(__wt_snprintf(*buf, len, "per_connection_control_points.%s", cp_config_name));
    return (0);
}

/*!
 * get_and_print_config --
 *
 * Get and print a config value.
 */
static int
get_and_print_config(
  WT_SESSION *wt_session, const char *cp_name, const char *parameter_name, const char *extra_config)
{
    WT_CONFIG_ITEM cp_cval;    /* Control point node */
    WT_CONFIG_ITEM param_cval; /* Parameter node */
    WT_DECL_RET;
    char *config_path;
    const char *cfg[3];
    WT_SESSION_IMPL *session;
    WT_CONNECTION_IMPL *conn;

    /* Get the path of the control point config node */
    config_path = NULL;
    session = (WT_SESSION_IMPL *)wt_session;
    WT_RET(__construct_configuration_control_point_string(session, cp_name, &config_path));

    /* Get the control point config node */
    conn = S2C(session);
    cfg[0] = conn->cfg;
    cfg[1] = extra_config;
    cfg[2] = NULL;
    WT_ERR(__wt_config_gets(session, cfg, config_path, &cp_cval));

    /* Get the parameter value */
    WT_ERR(__wt_config_subgets(session, &cp_cval, parameter_name, &param_cval));

    printf("Config value: Control point %s, parameter %s is %" PRIu64 "\n", cp_name, parameter_name,
      (uint64_t)param_cval.val);
err:
    __wt_free(session, config_path);
    return (ret);
}

/*! Thread that prints */
static WT_THREAD_RET
print_thread(void *thread_arg)
{
    struct thread_arguments *args;
    WT_CONNECTION *wt_conn;
    WT_SESSION *wt_session;
    WT_SESSION_IMPL *session;
    WT_RAND_STATE rnd_state;
    uint32_t rnd_num1;
    uint32_t seconds;
    uint32_t rnd_num2;
    uint32_t microseconds;

    args = thread_arg;
    wt_conn = args->conn;
    /* Initialize */
    error_check(wt_conn->open_session(wt_conn, NULL, session_open_config, &wt_session));
    session = (WT_SESSION_IMPL *)wt_session;
    __wt_random_init_seed(session, &rnd_state);

    /* Wait for main or the previous thread. */
    CONNECTION_CONTROL_POINT_WAIT_THREAD_BARRIER(session, args->wait_for_id);

    /* Sleep a random time. */
    rnd_num1 = __wt_random(&rnd_state);
    seconds = rnd_num1 % 5;
    rnd_num2 = __wt_random(&rnd_state);
    microseconds = rnd_num2 % WT_MILLION;
    __wt_sleep(seconds, microseconds);

    printf("Thread %d, wait_for_id %" PRId32 ", my_id %" PRId32 ". Slept %" PRIu32
           " seconds, %" PRIu32 " microseconds\n",
      args->thread_num, args->wait_for_id, args->my_id, seconds, microseconds);
    fflush(stdout);

    /* Finished. Both this thread and the next thread wait for the other to get to this control
     * point. */
    CONNECTION_CONTROL_POINT_DEFINE_THREAD_BARRIER(session, args->my_id);

    /* Cleanup */
    error_check(wt_session->close(wt_session, NULL));

    return (0);
}
#endif /* HAVE_CONTROL_POINT */

int
main(int argc, char *argv[])
{
#ifdef HAVE_CONTROL_POINT
    WT_CONNECTION *wt_conn;
    WT_SESSION *wt_session;
    WT_SESSION_IMPL *session;
    wt_thread_t threads[NUM_THREADS];
    struct thread_arguments thread_args[NUM_THREADS];
    int idx;
    const wt_control_point_id_t thread_control_point_ids[NUM_THREADS] = {
      WT_CONN_CONTROL_POINT_ID_THREAD_0,
      WT_CONN_CONTROL_POINT_ID_THREAD_1,
      WT_CONN_CONTROL_POINT_ID_THREAD_2,
      WT_CONN_CONTROL_POINT_ID_THREAD_3,
      WT_CONN_CONTROL_POINT_ID_THREAD_4,
    };
    const char *cfg;
    const char *wiredtiger_open_config = "create,"
#if 0 /* Include to test over riding wiredtiger_open() */
      "per_connection_control_points=["
      "thread_0=["
      "enable_count=3]],"
#endif
#if 0 /* Include if needed */
      "verbose=["
      "control_point=5,"
      "],"
#endif
      ;

    cfg = "";
    /* Setup */
    home = example_setup(argc, argv);

    error_check(wiredtiger_open(home, NULL, wiredtiger_open_config, &wt_conn));
    error_check(wt_conn->open_session(wt_conn, NULL, session_open_config, &wt_session));
    session = (WT_SESSION_IMPL *)wt_session;

    /* Enable all control points. */
    testutil_check_error_ok(
      wt_conn->enable_control_point(wt_conn, WT_CONN_CONTROL_POINT_ID_MAIN_START_PRINTING, cfg),
      EEXIST);
    for (idx = 0; idx < NUM_THREADS; ++idx)
        error_check(wt_conn->enable_control_point(wt_conn, thread_control_point_ids[idx], cfg));

    /* Demonstrate reading control point parameters. */
    /* With over-ride when reading. */
    testutil_check(get_and_print_config(
      wt_session, "thread_0", "wait_count", "per_connection_control_points.thread_0.wait_count=3"));
    /* Without over-ride when reading. */
    testutil_check(get_and_print_config(wt_session, "thread_0", "wait_count", ""));

    /* Start all threads */
    for (idx = 0; idx < NUM_THREADS; ++idx) {
        struct thread_arguments *my_args = &(thread_args[idx]);
        my_args->conn = wt_conn;
        my_args->thread_num = idx;
        my_args->wait_for_id = ((idx == 0) ? WT_CONN_CONTROL_POINT_ID_MAIN_START_PRINTING :
                                             thread_control_point_ids[idx - 1]);
        my_args->my_id = thread_control_point_ids[idx];

        error_check(__wt_thread_create(NULL, &threads[idx], print_thread, &(thread_args[idx])));
    }

    /* Both this thread and threads[0] wait for the other to get to this control point. */
    CONNECTION_CONTROL_POINT_DEFINE_THREAD_BARRIER(
      session, WT_CONN_CONTROL_POINT_ID_MAIN_START_PRINTING);

    /* Both this thread and threads[NUM_THREADS - 1] wait for the other to get to this control
     * point, i.e. for threads[NUM_THREADS - 1] to finish. */
    CONNECTION_CONTROL_POINT_WAIT_THREAD_BARRIER(
      session, thread_control_point_ids[NUM_THREADS - 1]);

    /* Join all threads */
    for (idx = 0; idx < NUM_THREADS; ++idx)
        error_check(__wt_thread_join(NULL, &threads[idx]));

    /*
     * Cleanup
     */
    /* Disable all control points. */
    error_check(
      wt_conn->disable_control_point(wt_conn, WT_CONN_CONTROL_POINT_ID_MAIN_START_PRINTING));
    for (idx = 0; idx < NUM_THREADS; ++idx)
        error_check(wt_conn->disable_control_point(wt_conn, thread_control_point_ids[idx]));

    /* Close session and connection. */
    error_check(wt_session->close(wt_session, NULL));
    error_check(wt_conn->close(wt_conn, NULL));
#else
    WT_UNUSED(argc);
    WT_UNUSED(argv);
#endif

#ifdef HAVE_CONTROL_POINT
    printf("Yes, HAVE_CONTROL_POINT is defined.\n");
#else
    printf("No, HAVE_CONTROL_POINT is not defined.\n");
    printf("This test does nothing since HAVE_CONTROL_POINT is not defined.\n");
#endif

#ifdef HAVE_DIAGNOSTIC
    printf("Yes, HAVE_DIAGNOSTIC is defined.\n");
#else
    printf("No, HAVE_DIAGNOSTIC is not defined.\n");
#endif

#ifdef HAVE_UNITTEST
    printf("Yes, HAVE_UNITTEST is defined.\n");
#else
    printf("No, HAVE_UNITTEST is not defined.\n");
#endif

    return (EXIT_SUCCESS);
}
