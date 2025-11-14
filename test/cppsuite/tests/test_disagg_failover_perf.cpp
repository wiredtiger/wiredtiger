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
 * [test_disagg_failover_perf]: Measure how long diagg failover takes on a running system.
 */


#include "src/common/constants.h"
#include "src/common/logger.h"
#include "src/common/random_generator.h"
#include "src/common/thread_manager.h"
#include "src/storage/connection_manager.h"
#include "src/storage/scoped_session.h"
#include "src/main/database.h"
#include "src/main/database_operation.h"

extern "C" {
#include "wiredtiger.h"
#include "test_util.h"
}

using namespace test_harness;
/* Declarations to avoid the error raised by -Werror=missing-prototypes. */
void insert_op(WT_CURSOR *cursor, int key_size, int value_size);
void read_op(WT_CURSOR *cursor, int key_size);

bool do_inserts = false;
bool do_reads = false;
static int nkeys = 50000;
static int ncolllections = 10;
static int key_len = 10;
static int val_len = 1000;
static int oldest_lag = 5;
static int stable_lag = 5;
static double crud_ratio[] = {0.1, 0.5, 0.3, 0.1};
test_harness::database *database_model;
test_harness::timestamp_manager *tsm;
test_harness::operation_tracker *op_tracker;

void
insert_op(WT_CURSOR *cursor, int key_size, int value_size)
{
    logger::log_msg(LOG_INFO, "called insert_op");

    /* Insert random data. */
    std::string key, value;
    while (do_inserts) {
        key = random_generator::instance().generate_random_string(key_size);
        value = random_generator::instance().generate_random_string(value_size);
        cursor->set_key(cursor, key.c_str());
        cursor->set_value(cursor, value.c_str());
        testutil_check(cursor->insert(cursor));
    }
}

void
read_op(WT_CURSOR *cursor, int key_size)
{
    logger::log_msg(LOG_INFO, "called read_op");

    /* Read random data. */
    std::string key;
    while (do_reads) {
        key = random_generator::instance().generate_random_string(key_size);
        cursor->set_key(cursor, key.c_str());
        WT_IGNORE_RET(cursor->search(cursor));
    }
}

/*
 * Because we can't use the cppsuite, as we start and stop WiredTiger we setup the necessary 
 * functionality here.
 */
static void
initialize() {
    database_model = new database();
    tsm = new timestamp_manager(oldest_lag, stable_lag);
    op_tracker = new operation_tracker(*tsm);
    database_model->set_timestamp_manager(tsm);
    database_model->set_create_config(false, false);
    database_model->set_operation_tracker(op_tracker);
}

static void
tear_down() {
    delete op_tracker;
    delete tsm;
    delete database_model;
    
    op_tracker = nullptr;
    tsm = nullptr;
    database_model = nullptr;
}

int
main(int argc, char *argv[])
{
    /* Set the program name for error messages. */
    const std::string progname = testutil_set_progname(argv);

    /* Set the tracing level for the logger component. */
    logger::trace_level = LOG_INFO;

    /* Printing some messages. */
    logger::log_msg(LOG_INFO, "Starting " + progname);

    /* 
     * Create a connection, and specify the home directory. We intentionally don't set the cache
     * size here as WiredTiger's 1/2 of system memory default is sufficient. 
     */
    const std::string home_dir = std::string(DEFAULT_DIR) + '_' + progname;

    /* Clean up any artifacts from prior runs. */
    testutil_remove(home_dir.c_str());

    /* Create connection. */
    connection_manager::instance().create(CONNECTION_CREATE + ",disaggregated=(role=\"leader\"),verbose=(disaggregated_storage:2)", home_dir);

    /* Initialize. */
    (void)crud_ratio;
    initialize();

    /* Populate the database. */
    database_operation::populate(*database_model, tsm, op_tracker, ncolllections, nkeys, key_len, val_len, 1, nullptr);

    /* Restart WiredTiger in follower mode. */
    logger::log_msg(LOG_INFO, "Restarting WiredTiger.");
    connection_manager::instance().close();
    connection_manager::instance().reopen(CONNECTION_CREATE + ",disaggregated=(role=\"follower\"),verbose=(disaggregated_storage:2)", home_dir);

    /* Manually delete things before the connection gets destructed to avoid seg fauls on scope_cursor close. */
    tear_down();
    /* Create a thread manager and spawn some threads that will work. */
    // thread_manager t;
    // int key_size = 1, value_size = 2;

    // do_inserts = true;
    // t.add_thread(insert_op, insert_cursor, key_size, value_size);

    // do_reads = true;
    // t.add_thread(read_op, read_cursor, key_size);

    // /* Sleep for the test duration. */
    // std::chrono::seconds test_duration_s(5);
    // std::this_thread::sleep_for(test_duration_s);

    // /* Stop the threads. */
    // do_reads = false;
    // do_inserts = false;
    // t.join();

    // /* Close cursors. */
    // for (auto c : cursors)
    //     testutil_check(c->close(c));

    // /* Another message. */
    // logger::log_msg(LOG_INFO, "End of test.");

    return (0);
}
