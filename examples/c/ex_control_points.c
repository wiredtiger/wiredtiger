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

#define NUM_CONTROL_POINTS 10

#if 0
# Control points config for api_data.c.
Config('main start printing', 'Wait for trigger', 'Always', '', r'''
       Thread 0 waits for main to get here.''',
       type='category', subconfig= [
	   # Action configuration parameters
	   Config('wait_count', '1', r'''
		  the number of triggers for which to wait''',
		  min='1', max='4294967295'),
       ]),
Config('thread 0', 'Wait for trigger', 'Always', '', r'''
       Thread 1 waits for thread 0 to get here.''',
       type='category', subconfig= [
	   # Action configuration parameters
	   Config('wait_count', '1', r'''
		  the number of triggers for which to wait''',
		  min='1', max='4294967295'),
       ]),
Config('thread 1', 'Wait for trigger', 'Always', '', r'''
       Thread 2 waits for thread 1 to get here.''',
       type='category', subconfig= [
	   # Action configuration parameters
	   Config('wait_count', '1', r'''
		  the number of triggers for which to wait''',
		  min='1', max='4294967295'),
       ]),
Config('thread 2', 'Wait for trigger', 'Always', '', r'''
       Thread 3 waits for thread 2 to get here.''',
       type='category', subconfig= [
	   # Action configuration parameters
	   Config('wait_count', '1', r'''
		  the number of triggers for which to wait''',
		  min='1', max='4294967295'),
       ]),
Config('thread 3', 'Wait for trigger', 'Always', '', r'''
       Thread 4 waits for thread 3 to get here.''',
       type='category', subconfig= [
	   # Action configuration parameters
	   Config('wait_count', '1', r'''
		  the number of triggers for which to wait''',
		  min='1', max='4294967295'),
       ]),
Config('thread 4', 'Wait for trigger', 'Always', '', r'''
       Thread 5 waits for thread 4 to get here.''',
       type='category', subconfig= [
	   # Action configuration parameters
	   Config('wait_count', '1', r'''
		  the number of triggers for which to wait''',
		  min='1', max='4294967295'),
       ]),
Config('thread 5', 'Wait for trigger', 'Always', '', r'''
       Thread 6 waits for thread 5 to get here.''',
       type='category', subconfig= [
	   # Action configuration parameters
	   Config('wait_count', '1', r'''
		  the number of triggers for which to wait''',
		  min='1', max='4294967295'),
       ]),
Config('thread 6', 'Wait for trigger', 'Always', '', r'''
       Thread 7 waits for thread 6 to get here.''',
       type='category', subconfig= [
	   # Action configuration parameters
	   Config('wait_count', '1', r'''
		  the number of triggers for which to wait''',
		  min='1', max='4294967295'),
       ]),
Config('thread 7', 'Wait for trigger', 'Always', '', r'''
       Thread 8 waits for thread 7 to get here.''',
       type='category', subconfig= [
	   # Action configuration parameters
	   Config('wait_count', '1', r'''
		  the number of triggers for which to wait''',
		  min='1', max='4294967295'),
       ]),
Config('thread 8', 'Wait for trigger', 'Always', '', r'''
       Thread 9 waits for thread 8 to get here.''',
       type='category', subconfig= [
	   # Action configuration parameters
	   Config('wait_count', '1', r'''
		  the number of triggers for which to wait''',
		  min='1', max='4294967295'),
       ]),
Config('thread 9', 'Wait for trigger', 'Always', '', r'''
       The thread executing main waits for thread 9 to get here.''',
       type='category', subconfig= [
	   # Action configuration parameters
	   Config('wait_count', '1', r'''
		  the number of triggers for which to wait''',
		  min='1', max='4294967295'),
       ]),
#endif

struct thread_arguments {
    WT_CONNECTION *conn;
    int thread_num;
    WT_CONTROL_POINT_ID wait_for_id;
    WT_CONTROL_POINT_ID my_id;
};

/*! Thread that prints */
static WT_THREAD_RET
print_thread(void *thread_arg) {
    thread_arguments *args = thread_arg;
    WT_CONNECTION *conn = args->conn;
    WT_SESSION *session;
    WT_RAND_STATE rnd_state;
    uint32_t rnd_num1;
    uint32_t seconds;
    uint32_t rnd_num2;
    uint32_t microseconds;
    bool enabled;

    /* Initialize */
    error_check(conn->open_session(conn, NULL, NULL, &session));
    __wt_random_init_seed(session, &rnd_state);

    /* Wait for main or the previous thread. */
    CONNECTION_CONTROL_POINT_WAIT_FOR_TRIGGER(session, args->wait_for_id, enabled);

    /* Sleep a random time. */
    rnd_num1 = __wt_random(&rnd_state);
    seconds = rnd_num1 % 5;
    rnd_num2 = __wt_random(&rnd_state);
    microseconds = rnd_num2 % WT_MILLION;
    __wt_sleep(seconds, microseconds);
    
    printf("Thread %d, wait_for_id %" PRIu32 ", my_id %" PRIu32 ", enabled %c. Slept %" PRIu32 " seconds, %" PRIu32 " microseconds\n",
	   arguments->thread_num, args->wait_for_id, args->my_id, enabled ? '1' : '0', seconds, microseconds);
    fflush(stdout);

    /* Finished. Signal the next thread which waits for this thread to get here. */
    CONNECTION_CONTROL_POINT_DEFINE_WAIT_FOR_TRIGGER(session, args->my_id);

    /* Cleanup */
    error_check(session->close(session, NULL));

    return (0);
}

int
main(int argc, char *argv[]) {
    WT_CONNECTION *conn;
    WT_SESSION *session;
    wt_thread_t threads[NUM_THREADS];
    thread_arguments thread_args[NUM_THREADS];
    int idx;
    const WT_CONTROL_POINT_ID thread_control_point_ids[NUM_THREADS] = {
	WT_CONN_CONTROL_POINT_ID_THREAD_0,
	WT_CONN_CONTROL_POINT_ID_THREAD_1,
	WT_CONN_CONTROL_POINT_ID_THREAD_2,
	WT_CONN_CONTROL_POINT_ID_THREAD_3,
	WT_CONN_CONTROL_POINT_ID_THREAD_4,
	WT_CONN_CONTROL_POINT_ID_THREAD_5,
	WT_CONN_CONTROL_POINT_ID_THREAD_6,
	WT_CONN_CONTROL_POINT_ID_THREAD_7,
	WT_CONN_CONTROL_POINT_ID_THREAD_8,
	WT_CONN_CONTROL_POINT_ID_THREAD_9,
    };

    /* Setup */
    home = example_setup(argc, argv);

    error_check(wiredtiger_open(home, NULL, "create", &conn));
    error_check(conn->open_session(conn, NULL, NULL, &session));

    /* Enable all control points. */
    error_check(__wt_conn_control_point_enable(session, WT_CONN_CONTROL_POINT_ID_MAIN_START_PRINTING));
    for (idx = 0; idx < NUM_THREADS; ++idx)
	error_check(__wt_conn_control_point_enable(session, thread_control_point_ids[idx]));

    /* Start all threads */
    for (idx = 0; idx < NUM_THREADS; ++idx) {
	thread_arguments * my_args = &(thread_args[idx]);
	my_args->conn = conn;
	my_args->thread_num = idx;
	my_args->wait_for_id = ((idx == 0)
				? WT_CONN_CONTROL_POINT_ID_MAIN_START_PRINTING
				: thread_control_point_ids[idx - 1]);
	my_args->my_id = thread_control_point_ids[idx];

        error_check(__wt_thread_create(NULL, & threads[i], print_thread,
				       &(thread_args[idx])));
    }
    
    /* Signal threads[0] which waits for this thread to get here. */
    CONNECTION_CONTROL_POINT_DEFINE_WAIT_FOR_TRIGGER(session,
						  WT_CONN_CONTROL_POINT_ID_MAIN_START_PRINTING);

    /* This thread waits for threads[NUM_THREADS - 1] to finish. */
    CONNECTION_CONTROL_POINT__WAIT_FOR_TRIGGER(session,
					    thread_control_point_ids[NUM_THREADS - 1], enabled);
    
    /* Join all threads */
    for (idx = 0; idx < NUM_THREADS; ++idx)
        error_check(__wt_thread_join(NULL, &threads[idx]));

    /*
     * Cleanup
     */
    /* Disable all control points. */
    error_check(__wt_conn_control_point_enable(session, WT_CONN_CONTROL_POINT_ID_MAIN_START_PRINTING));
    for (idx = 0; idx < NUM_THREADS; ++idx)
	error_check(__wt_conn_control_point_disable(session, thread_control_point_ids[idx]));

    /* Close session and connection. */
    error_check(session->close(session, NULL));
    error_check(conn->close(conn, NULL));

    return (EXIT_SUCCESS);
}
