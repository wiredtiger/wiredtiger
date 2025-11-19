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
 * [test_disagg_failover_perf]: Measure how long disagg failover takes on a running system.
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
#include <string_view>
#include <vector>
#include <string>
#include <charconv>
#include <iostream>

using namespace test_harness;
struct options {
    int collection_count = 3;
    int key_count = 5000;
    int key_size = 10;
    int value_size = 1000;
    std::string workload_shape = "mixed";
    std::string home_path = DEFAULT_DIR;
    bool warm_cache = false;
    bool load_skip = false;
    bool load_copy = false;
};

options opt;
static double crud_ratio[] = {0.1, 0.5, 0.3, 0.1};
wt_timestamp_t ts = 100;
int last_key = 1;
test_harness::database *database_model;

/*
 * Because we can't use the cppsuite, as we start and stop WiredTiger we setup the necessary
 * functionality here.
 */
static void
initialize()
{
    database_model = new database();
    database_model->set_create_config(false, false, true);
}

static std::string
generate_key(int key)
{
    return thread_worker::pad_string(std::to_string(key), opt.key_size);
}

static std::string
generate_value()
{
    return random_generator::instance().generate_pseudo_random_string(opt.value_size);
}

static bool
parse_int(std::string_view sv, int &out)
{
    const char *b = sv.data();
    const char *e = sv.data() + sv.size();
    auto res = std::from_chars(b, e, out);
    return res.ec == std::errc() && res.ptr == e;
}

static bool
match_opt(std::string_view arg, std::string_view key, std::string_view &val, bool &has_inline_val)
{
    if (arg == key) {
        has_inline_val = false;
        val = {};
        return true;
    }
    if (arg.size() >= key.size() + 1 && arg.substr(0, key.size()) == key) {
        has_inline_val = true;
        if (arg[key.size()] == '=') {
            val = arg.substr(key.size() + 1);
        } else {
            val = arg.substr(key.size());
        }
        return true;
    }
    return false;
}

static bool
parse_options(const std::vector<std::string_view> &args, options &out, std::string &error)
{
    std::size_t i = 1;
    bool end_of_options = false;

    auto require_value = [&](std::string_view current_key, std::string_view inline_val,
                           bool has_inline_val, std::string_view &val) -> bool {
        if (has_inline_val) {
            val = inline_val;
            return true;
        }
        if (i + 1 >= args.size()) {
            error = std::string(current_key) + " requires a value";
            return false;
        }
        if (args[i + 1].size() > 0 && args[i + 1][0] == '-') {
            error = std::string(current_key) + " requires a value";
            return false;
        }
        val = args[++i];
        return true;
    };

    while (i < args.size()) {
        std::string_view arg = args[i];

        if (!end_of_options && arg == "--") {
            end_of_options = true;
            ++i;
            continue;
        }

        if (!end_of_options && !arg.empty() && arg[0] == '-') {
            std::string_view val;
            bool has_inline_val = false;
            if (match_opt(arg, "-cc", val, has_inline_val)) {
                std::string_view v;
                if (!require_value("-cc", val, has_inline_val, v))
                    return false;
                int n = 0;
                if (!parse_int(v, n) || n <= 0) {
                    error = "invalid collection_count: " + std::string(v);
                    return false;
                }
                out.collection_count = n;
                ++i;
                continue;
            }
            if (match_opt(arg, "-kc", val, has_inline_val)) {
                std::string_view v;
                if (!require_value("-kc", val, has_inline_val, v))
                    return false;
                int n = 0;
                if (!parse_int(v, n) || n <= 0) {
                    error = "invalid key_count: " + std::string(v);
                    return false;
                }
                out.key_count = n;
                ++i;
                continue;
            }
            if (match_opt(arg, "-ks", val, has_inline_val)) {
                std::string_view v;
                if (!require_value("-ks", val, has_inline_val, v))
                    return false;
                int n = 0;
                if (!parse_int(v, n) || n <= 0) {
                    error = "invalid key_size: " + std::string(v);
                    return false;
                }
                out.key_size = n;
                ++i;
                continue;
            }
            if (match_opt(arg, "-vs", val, has_inline_val)) {
                std::string_view v;
                if (!require_value("-vs", val, has_inline_val, v))
                    return false;
                int n = 0;
                if (!parse_int(v, n) || n <= 0) {
                    error = "invalid value_size: " + std::string(v);
                    return false;
                }
                out.value_size = n;
                ++i;
                continue;
            }
            if (match_opt(arg, "-shape", val, has_inline_val)) {
                std::string_view v;
                if (!require_value("-shape", val, has_inline_val, v))
                    return false;
                if (v != "append" && v != "mixed") {
                    error =
                      "invalid workload_shape (expected 'append' or 'mixed'): " + std::string(v);
                    return false;
                }
                out.workload_shape = v;
                ++i;
                continue;
            }
            if (match_opt(arg, "-h", val, has_inline_val)) {
                std::string_view v;
                if (!require_value("-h", val, has_inline_val, v))
                    return false;
                out.home_path = v;
                ++i;
                continue;
            }
            if (match_opt(arg, "-wc", val, has_inline_val)) {
                std::string_view v;
                out.warm_cache = true;
                ++i;
                continue;
            }
            if (match_opt(arg, "-lc", val, has_inline_val)) {
                std::string_view v;
                out.load_copy = true;
                ++i;
                continue;
            }
            if (match_opt(arg, "-ls", val, has_inline_val)) {
                std::string_view v;
                out.load_skip = true;
                ++i;
                continue;
            }
            error = "unknown option: " + std::string(arg);
            return false;
        }
        ++i;
    }
    return true;
}

static void
update_global_timestamps()
{
    std::string config;
    config += STABLE_TS + "=" + timestamp_manager::decimal_to_hex(++ts) + ",";
    config += OLDEST_TS + "=" + timestamp_manager::decimal_to_hex(ts - 20);
    connection_manager::instance().set_timestamp(config);
}

static void
populate()
{
    logger::log_msg(
      LOG_INFO, "Populate: creating " + std::to_string(opt.collection_count) + " collections.");

    /* Create n collections as per the configuration. */
    scoped_session session = connection_manager::instance().create_session();
    for (int64_t i = 0; i < opt.collection_count; ++i) {
        /*
         * The database model will call into the API and create the collection, with its own
         * session.
         */
        database_model->add_collection(session, opt.key_count);
        collection &coll = database_model->get_collection(i);
        scoped_cursor cursor = session.open_scoped_cursor(coll.name);
        transaction txn(20, 20);
        for (int64_t j = 0; j < opt.key_count; j++) {
            txn.begin(session);
            wt_timestamp_t commit_ts = ++ts;
            testutil_check(txn.set_commit_timestamp(session, commit_ts));
            testutil_assert(crud::insert(cursor, txn, generate_key(last_key++), generate_value()));
            testutil_assert(txn.commit(session));
            if (j % 1000 == 0)
                /* Advance the stable and oldest timestamps. */
                update_global_timestamps();

            if (j != 0 && j % 10000 == 0)
                logger::log_msg(LOG_INFO, "Populate: loaded " + std::to_string(j) + " keys");
        }
        update_global_timestamps();
        session->checkpoint(session.get(), nullptr);
        logger::log_msg(LOG_INFO, "Populate: loaded collection: " + std::to_string(i) + "");
    }
    logger::log_msg(LOG_INFO,
      "Populate: " + std::to_string(opt.collection_count) +
        " collections created and loaded with " + std::to_string(opt.key_count) + " keys.");
}

/*
 * Walk a cursor for a specified number of records, if less than the total number it will bias
 * towards lower collection numbers.
 */
static void
cache_warming(int64_t records)
{
    scoped_session session = connection_manager::instance().create_session();
    int64_t record_count = 0;
    for (int64_t i = 0; i < opt.collection_count; ++i) {
        collection &coll = database_model->get_collection(i);
        scoped_cursor cursor = session.open_scoped_cursor(coll.name);
        for (int64_t j = 0; j < opt.key_count && record_count < records; j++, record_count++)
            /*
             * We should always be within the record count of the collection, therefore this should
             * never return an error.
             */
            testutil_check(cursor->next(cursor.get()));
    }
}

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
    std::vector<std::string_view> args(argv, argv + argc);
    /* Set the program name for error messages. */
    const std::string progname = testutil_set_progname(argv);

    /* Set the tracing level for the logger component. */
    logger::trace_level = LOG_INFO;
    logger::log_msg(LOG_INFO, "Starting " + progname);

    /* Parse options. */
    std::string err;
    if (!parse_options(args, opt, err)) {
        std::cerr << "error: " << err << "\n";
        std::cerr << "usage: " << args[0] << " [options]\n"
                  << "  -cc N        collection_count (int > 0)\n"
                  << "  -kc N        key_count (int > 0)\n"
                  << "  -ks N        key_size (int > 0)\n"
                  << "  -vs N        value_size (int > 0)\n"
                  << "  -shape S     workload_shape ('append' or 'mixed')\n"
                  << "  -h PATH      home_path\n"
                  << "  --           end of options\n";
        return 1;
    }

    logger::log_msg(LOG_INFO,
      "Running with configuration: collection_count=" + std::to_string(opt.collection_count) +
        ", key_count=" + std::to_string(opt.key_count) + ", key_size=" +
        std::to_string(opt.key_size) + ", value_size=" + std::to_string(opt.value_size) +
        ", workload_shape=" + opt.workload_shape + ", home_path=" + opt.home_path +
        ", warm_cache=" + (opt.warm_cache ? "true" : "false") + ", load_copy=" +
        (opt.load_copy ? "true" : "false") + ", load_skip=" + (opt.load_skip ? "true" : "false"));

    logger::log_msg(LOG_INFO,
      "Data size is: " +
        std::to_string(((opt.collection_count * opt.key_count) * (opt.key_size + opt.value_size)) /
          1000 / 1000) +
        "MB");
    /*
     * Create a connection, and specify the home directory. We intentionally don't set the cache
     * size here as WiredTiger's 1/2 of system memory default is sufficient.
     */
    /* Clean up any artifacts from prior runs. */
    testutil_remove(opt.home_path.c_str());

    /* Create connection. */
    // connection_manager::instance().create(CONNECTION_CREATE +
    // ",extensions=[../../ext/page_log/palite/libwiredtiger_palite.so=(config=\"(verbose=1)\")],precise_checkpoint=true,disaggregated=(role=\"leader\",page_log=palite),verbose=(disaggregated_storage:2)",
    // home_dir);

    /* Initialize. */
    (void)crud_ratio;
    initialize();

    /* Populate the database. */
    if (opt.load_skip) {
        logger::log_msg(LOG_INFO, "Using existing database.");
        logger::log_msg(
          LOG_INFO, "Copying \"" + opt.home_path + ".back\" to \"" + opt.home_path + "\"");
        testutil_copy(std::string(opt.home_path + ".back").c_str(), opt.home_path.c_str());
        database_model->add_existing_collections(opt.collection_count, opt.key_count);
    } else {
        connection_manager::instance().create(CONNECTION_CREATE +
            ",extensions=[../../ext/page_log/palite/"
            "libwiredtiger_palite.so],precise_checkpoint=true,disaggregated=(role=\"leader\",page_"
            "log="
            "palite)",
          opt.home_path);
        populate();
    }
    /* Restart WiredTiger in follower mode. */
    logger::log_msg(LOG_INFO, "##########################################################");
    logger::log_msg(LOG_INFO, "################ Restarting WiredTiger. ##################");
    logger::log_msg(LOG_INFO, "##########################################################");

    connection_manager::instance().close();
    if (opt.load_copy) {
        logger::log_msg(LOG_INFO, "Copying the home directory to \"" + opt.home_path + ".back\"");
        logger::log_msg(LOG_INFO, "This will delete the existing directory!");
        testutil_remove(std::string(opt.home_path + ".back").c_str());
        testutil_copy(opt.home_path.c_str(), std::string(opt.home_path + ".back").c_str());
    }

    logger::log_msg(LOG_INFO, "##########################################################");
    logger::log_msg(LOG_INFO, "############ Starting WiredTiger as follower. ############");
    logger::log_msg(LOG_INFO, "##########################################################");

    // connection_manager::instance().reopen(CONNECTION_CREATE +
    // ",extensions=[../../ext/page_log/palite/libwiredtiger_palite.so=(config=\"(verbose=1)\")],precise_checkpoint=true,disaggregated=(role=\"follower\",page_log=palite),verbose=(disaggregated_storage:1)",
    // home_dir);
    connection_manager::instance().reopen(CONNECTION_CREATE +
        ",extensions=[../../ext/page_log/palite/"
        "libwiredtiger_palite.so],precise_checkpoint=true,disaggregated=(role=\"follower\",page_"
        "log=palite),verbose=(disaggregated_storage:1)",
      opt.home_path);

    // TODO: Do we need to pickup the checkpoint as soon as we start in follower mode?
    wt_timestamp_t timestamp = wt_disagg_pick_up_latest_checkpoint();

    /* TODO: */
    /* TODO: Optionally scan created tables to warm the WT cache. */
    if (opt.warm_cache)
        cache_warming(opt.collection_count * opt.key_count);

    /* TODO: Perform crud operations. */
    // crud_operations();

    /* TODO: Measure time of step up. */
    WT_CONNECTION *conn = connection_manager::instance().get_connection();
    conn->reconfigure(conn, "disaggregated=(role=\"leader\")");
    std::string stable_config = "stable_timestamp=" + timestamp_manager::decimal_to_hex(timestamp);
    conn->set_timestamp(conn, stable_config.c_str());
    /* TODO: Should we measure post step up checkpoint duration here? cc: Keith Smith. */
    /* Peter thinks we should do this. */
    // /* Manually delete things before the connection gets destructed to avoid segfaults on
    // scope_cursor close. */
    // // tear_down();
    return (0);
}
