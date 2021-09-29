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

#include "test_harness/util/logger.h"
#include "test_harness/thread_manager.h"


extern "C" {
#include "wiredtiger.h"
}

void
thread_op2()
{
    test_harness::logger::log_msg(LOG_INFO, "called thread_op2");
}

void
thread_op1()
{
    test_harness::logger::log_msg(LOG_INFO, "called thread_op1");
}

int
main(int argc, char *argv[])
{
    /* Set the program name for error messages. */
    (void)testutil_set_progname(argv);

    /* Set the tracing level for the logger component. */
    test_harness::logger::trace_level = LOG_INFO;
    test_harness::logger::log_msg(LOG_INFO, "Starting test basic_test.cxx");

    /* Create a thread_manager and spawn some threads. */
    test_harness::thread_manager t;

    test_harness::connection_manager

    t.add_thread(thread_op1);
    t.add_thread(thread_op2);

    t.join();

    return (0);
}
