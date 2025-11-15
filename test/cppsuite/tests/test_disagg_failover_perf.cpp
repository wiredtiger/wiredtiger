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
#include "src/main/crud.h"

extern "C" {
#include "wiredtiger.h"
#include "test_util.h"
}

#include <sstream>

using namespace test_harness;
/* Declarations to avoid the error raised by -Werror=missing-prototypes. */
void insert_op(WT_CURSOR *cursor, int key_size, int value_size);
void read_op(WT_CURSOR *cursor, int key_size);

bool do_inserts = false;
bool do_reads = false;
static int nkeys = 50000;
static int ncollections = 10;
// static int nkeys = 2000;
// static int ncolllections = 2;
static int key_len = 10;
static int val_len = 1000;
static double crud_ratio[] = {0.1, 0.5, 0.3, 0.1};
wt_timestamp_t ts = 100;
test_harness::database *database_model;

/*
 * Because we can't use the cppsuite, as we start and stop WiredTiger we setup the necessary
 * functionality here.
 */
static void
initialize() {
    database_model = new database();
    database_model->set_create_config(false, false, true);
}

static std::string
generate_key()
{
    return random_generator::instance().generate_random_string(key_len);
}

static std::string
generate_value()
{
    return random_generator::instance().generate_pseudo_random_string(val_len);
}

static void
update_global_timestamps(){
    std::string config;
    config += STABLE_TS + "=" + timestamp_manager::decimal_to_hex(++ts) + ",";
    config += OLDEST_TS + "=" + timestamp_manager::decimal_to_hex(ts - 20);
    connection_manager::instance().set_timestamp(config);

}

static void
populate()
{
    logger::log_msg(
      LOG_INFO, "Populate: creating " + std::to_string(ncollections) + " collections.");

    /* Create n collections as per the configuration. */
    scoped_session session = connection_manager::instance().create_session();
    for (int64_t i = 0; i < ncollections; ++i) {
        /*
         * The database model will call into the API and create the collection, with its own
         * session.
         */
        database_model->add_collection(session, nkeys);
        collection &coll = database_model->get_collection(i);
        scoped_cursor cursor = session.open_scoped_cursor(coll.name);
        transaction txn(20, 20);
        for (int64_t j = 0; j < nkeys; j ++) {
            txn.begin(session);
            wt_timestamp_t commit_ts = ++ts;
            testutil_check(txn.set_commit_timestamp(session, commit_ts));
            testutil_assert(crud::insert(cursor, txn, generate_key(), generate_value()));
            testutil_assert(txn.commit(session));
            if (j % 1000 == 0) {
                /* Advance the stable and oldest timestamps. */
                update_global_timestamps();
                session->checkpoint(session.get(), nullptr);
                logger::log_msg(
                LOG_INFO, "Populate: loaded " + std::to_string(j) + " keys");
            }
        }
        logger::log_msg(
            LOG_INFO, "Populate: loaded collection: " + std::to_string(i) + "");

    }
    logger::log_msg(
      LOG_INFO, "Populate: " + std::to_string(ncollections) + " collections created and loaded with " + std::to_string(nkeys) + " .");
}

// static void
// cache_warming(){

// }

// static void
// crud_operations() {

// }

// static void
// tear_down() {
//     delete op_tracker;
//     delete tsm;
//     delete database_model;

//     op_tracker = nullptr;
//     tsm = nullptr;
//     database_model = nullptr;
// }

/*
 * wt_disagg_pick_up_latest_checkpoint --
 *     Pick up the latest WiredTiger checkpoint.
 */
static uint64_t
wt_disagg_pick_up_latest_checkpoint()
{
    WT_CONNECTION *conn = connection_manager::instance().get_connection();
    scoped_session session = connection_manager::instance().create_session();
    WT_PAGE_LOG *page_log;
    testutil_check(conn->get_page_log(conn, "palite", &page_log));

    WT_ITEM metadata{};
    uint64_t timestamp;
    testutil_check(page_log->pl_get_complete_checkpoint_ext(
      page_log, session.get(), nullptr, nullptr, &timestamp, &metadata));

    page_log->terminate(page_log, NULL); /* dereference */
    page_log = NULL;

    char *checkpoint_meta = strndup((const char *)metadata.data, metadata.size);
    free(metadata.mem);

    std::ostringstream config;
    config << "disaggregated=(checkpoint_meta=\"" << checkpoint_meta << "\")";
    free(checkpoint_meta);

    std::string config_str = config.str();
    testutil_check(conn->reconfigure(conn, config_str.c_str()));
    return timestamp;
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
    //connection_manager::instance().create(CONNECTION_CREATE + ",extensions=[../../ext/page_log/palite/libwiredtiger_palite.so=(config=\"(verbose=1)\")],precise_checkpoint=true,disaggregated=(role=\"leader\",page_log=palite),verbose=(disaggregated_storage:2)", home_dir);
    connection_manager::instance().create(CONNECTION_CREATE + ",extensions=[../../ext/page_log/palite/libwiredtiger_palite.so],precise_checkpoint=true,disaggregated=(role=\"leader\",page_log=palite),verbose=(disaggregated_storage:2)", home_dir);

    /* Initialize. */
    (void)crud_ratio;
    initialize();

    /* Populate the database. */
    populate();
    /* Restart WiredTiger in follower mode. */
    logger::log_msg(LOG_INFO, "############################################################################################################");
    logger::log_msg(LOG_INFO, "######################################## Restarting WiredTiger. ############################################");
    logger::log_msg(LOG_INFO, "############################################################################################################");

    connection_manager::instance().close();
    logger::log_msg(LOG_INFO, "######################################################################################################################");
    logger::log_msg(LOG_INFO, "######################################## Starting WiredTiger as follower. ############################################");
    logger::log_msg(LOG_INFO, "######################################################################################################################");
    connection_manager::instance().reopen(CONNECTION_CREATE + ",extensions=[../../ext/page_log/palite/libwiredtiger_palite.so=(config=\"(verbose=2)\")],precise_checkpoint=true,disaggregated=(role=\"follower\",page_log=palite),verbose=(disaggregated_storage:2)", home_dir);

    // /* TODO: Optionally scan created tables to warm the WT cache. */
    // cache_warming();

    // /* TODO: Perform crud operations. */
    // crud_operations();

    // /* TODO: Measure time of step up. */
    wt_timestamp_t timestamp = wt_disagg_pick_up_latest_checkpoint();
    WT_CONNECTION *conn = connection_manager::instance().get_connection();
    conn->reconfigure(conn, "disaggregated=(role=\"leader\")");
    std::string stable_config = "stable_timestamp=" + timestamp_manager::decimal_to_hex(timestamp);
    conn->set_timestamp(conn, stable_config.c_str());
    /* TODO: Should we measure post step up checkpoint duration here? cc: Keith Smith. */
    /* Peter thinks we should do this. */
    // /* Manually delete things before the connection gets destructed to avoid seg fauls on scope_cursor close. */
    // // tear_down();
    return (0);
}
