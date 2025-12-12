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
#include <iostream>
enum class workload_type { append, update };

using namespace test_harness;
struct options {
    int collection_count = 3;
    int cache_size_gb = 16;
    int key_count = 5000;
    int key_size = 10;
    int value_size = 1000;
    int ingest_size_mb = 1;
    int verbose_level = 0;
    workload_type type = workload_type::update;
    std::string home_path = DEFAULT_DIR;
    int warm_cache_pct = 0;
    bool load_skip = false;
    bool load_copy = false;
};

options opt;
wt_timestamp_t ts = 100;
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
            if (match_opt(arg, "-cs_gb", val, has_inline_val)) {
                std::string_view v;
                if (!require_value("-cs_gb", val, has_inline_val, v))
                    return false;
                int n = 0;
                if (!parse_int(v, n) || n <= 0) {
                    error = "invalid cache_size_gb: " + std::string(v);
                    return false;
                }
                out.cache_size_gb = n;
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
            if (match_opt(arg, "-ingest_size_mb", val, has_inline_val)) {
                std::string_view v;
                if (!require_value("-ingest_size_mb", val, has_inline_val, v))
                    return false;
                int n = 0;
                if (!parse_int(v, n) || n <= 0) {
                    error = "invalid ingest_size_mb: " + std::string(v);
                    return false;
                }
                out.ingest_size_mb = n;
                ++i;
                continue;
            }
            if (match_opt(arg, "-wc_pct", val, has_inline_val)) {
                std::string_view v;
                if (!require_value("-wc_pct ", val, has_inline_val, v))
                    return false;
                int n = 0;
                if (!parse_int(v, n) || n <= 0) {
                    error = "invalid wc_pct: " + std::string(v);
                    return false;
                }
                out.warm_cache_pct = n;
                ++i;
                continue;
            }
            if (match_opt(arg, "-ve", val, has_inline_val)) {
                std::string_view v;
                if (!require_value("-ve", val, has_inline_val, v))
                    return false;
                int n = 0;
                if (!parse_int(v, n) || n <= 0) {
                    error = "invalid verbose level: " + std::string(v);
                    return false;
                }
                out.verbose_level = n;
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
                if (v == "append")
                    out.type = workload_type::append;
                else if (v == "updates")
                    out.type = workload_type::update;
                else {
                    error =
                      "invalid workload_shape (expected 'append' or 'updates'): " + std::string(v);
                    return false;
                }
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
    logger::log_msg(LOG_TRACE, "Updating global timestamps " + config);
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
        transaction txn;
        for (int64_t j = 0; j < opt.key_count; j++) {
            txn.begin(session);
            wt_timestamp_t commit_ts = ++ts;
            testutil_check(session->timestamp_transaction(session.get(),
              ("commit_timestamp=" + timestamp_manager::decimal_to_hex(commit_ts)).c_str()));
            testutil_assert(crud::insert(cursor, txn, generate_key(j), generate_value()));
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
    logger::log_msg(
      LOG_INFO, "Warming cache by loading in " + std::to_string(records) + " records.");
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

static void
append(collection &coll, scoped_session &session, scoped_cursor &cursor, uint64_t &ingested_data)
{
    uint64_t start_key_count = coll.get_key_count();
    for (int j = 0; j < 10; j++) {
        transaction txn;
        txn.begin(session);
        testutil_check(session->timestamp_transaction(
          session.get(), ("commit_timestamp=" + timestamp_manager::decimal_to_hex(++ts)).c_str()));
        testutil_assert(
          crud::insert(cursor, txn, generate_key(j + start_key_count), generate_value()));
        testutil_assert(txn.commit(session));
        ingested_data += opt.key_size + opt.value_size;
    }
    coll.increase_key_count(10);
}

static void
update(collection &coll, scoped_session &session, scoped_cursor &cursor, uint64_t &ingested_data)
{
    uint64_t key_count = coll.get_key_count();
    testutil_assert(key_count != 0);
    for (int j = 0; j < 10; j++) {
        transaction txn;
        uint64_t key = random_generator::instance().generate_integer(0UL, key_count - 1);
        std::string k = generate_key(key);

        txn.begin(session);
        testutil_check(session->timestamp_transaction(
          session.get(), ("commit_timestamp=" + timestamp_manager::decimal_to_hex(++ts)).c_str()));

        // Read the current value (optional, but as per prompt)
        cursor->set_key(cursor.get(), k.c_str());
        /* All keys must exist. */
        testutil_check(cursor->search(cursor.get()));
        // Overwrite with a new value
        testutil_assert(crud::update(cursor, txn, k, generate_value()));
        testutil_assert(txn.commit(session));
        ingested_data += opt.key_size + opt.value_size;
    }
}

static void
crud_worker(workload_type type)
{
    scoped_session session = connection_manager::instance().create_session();
    struct collection_cursor {
        collection_cursor(collection &coll, scoped_cursor &&cursor)
            : coll(coll), cursor(std::move(cursor))
        {
        }
        scoped_cursor cursor;
        collection &coll;
    };
    std::map<int, collection_cursor> cursor_map;
    uint64_t ingested_data = 0;
    uint64_t last_logged_mb = 0;
    while (ingested_data < opt.ingest_size_mb * 1000ULL * 1000ULL) {
        /* Generate a random int between 0 and collection count. */
        int collection_num =
          random_generator::instance().generate_integer(0, opt.collection_count - 1);
        if (cursor_map.find(collection_num) == cursor_map.end()) {
            collection &coll = database_model->get_collection(collection_num);
            /*
             * Construct the mapped value in-place. Using operator[] and then assigning
             * requires the mapped_type to be assignable which is not true here because
             * `collection_cursor` holds a reference member (and a move-only cursor). That
             * deletes the implicit assignment operator. Emplace avoids assignment by
             * constructing the value directly in the map.
             */
            cursor_map.emplace(
              collection_num, collection_cursor(coll, session.open_scoped_cursor(coll.name)));
        }
        /*
         * Access the stored scoped_cursor member from the mapped value. Use `at` to avoid
         * accidental default-construction if the key were missing.
         */
        collection_cursor &cc = cursor_map.at(collection_num);
        scoped_cursor &cursor = cc.cursor;
        // Workload logic
        if (type == workload_type::append)
            append(cc.coll, session, cursor, ingested_data);
        else if (type == workload_type::update)
            update(cc.coll, session, cursor, ingested_data);
        /* Log every 100MB ingested. */
        uint64_t current_mb = ingested_data / 1000 / 1000;
        if (current_mb >= last_logged_mb + 100) {
            logger::log_msg(LOG_INFO,
              (type == workload_type::append ? "Appended " : "Updated ") +
                std::to_string(current_mb) + "MB");
            last_logged_mb = current_mb;
        }
    }
}

static void
crud_operations()
{
    if (opt.type == workload_type::append)
        logger::log_msg(LOG_INFO, "Performing ingest appends.");
    else
        logger::log_msg(LOG_INFO, "Performing ingest updates.");
    crud_worker(opt.type);
    logger::log_msg(LOG_INFO, "Ingest phase complete.");
}

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
        std::cerr
          << "usage: " << args[0] << " [options]\n"
          << "  -cc N                   collection_count (int > 0)\n"
          << "  -cs_gb N                cache_size_gb (int > 0)\n"
          << "  -kc N                   key_count (int > 0)\n"
          << "  -ks N                   key_size (int > 0)\n"
          << "  -vs N                   value_size (int > 0)\n"
          << "  -ve N                   verbosity level; 1 turns on WT_VERB_DISAGG:1, 2 "
          << " will enable the palite module to begin logging with verbosity level 1, 3 "
          << " will increase the verbosity level of WT_VERB_DISAGG and so on.\n"
          << "  -wc_pct N               warm the cache as a percentage of initial data set size\n"
          << "  -lc                     create a copy of the loaded data\n"
          << "  -ls                     use data in WT_TEST.back instead of loading\n"
          << "  -ingest_size_mb N       amount of data to insert into ingest tables."
          << " note: this will only make sense with workload shape append.\n"
          << "  -shape S                workload_shape ('append' or 'update')\n"
          << "  -h PATH                 home_path\n"
          << "  --                      end of options\n";
        return 1;
    }

    logger::log_msg(LOG_INFO,
      "Running with configuration: collection_count=" + std::to_string(opt.collection_count) +
        ", key_count=" + std::to_string(opt.key_count) + ", key_size=" +
        std::to_string(opt.key_size) + ", value_size    =" + std::to_string(opt.value_size) +
        ", workload_shape=" + (opt.type == workload_type::append ? "append" : "update") +
        ", home_path=" + opt.home_path + ", warm_cache_pct=" + std::to_string(opt.warm_cache_pct) +
        "%, load_copy=" + (opt.load_copy ? "true" : "false") +
        ", load_skip=" + (opt.load_skip ? "true" : "false"));

    logger::log_msg(LOG_INFO,
      "Data size is: " +
        std::to_string(
          ((1ULL * opt.collection_count * opt.key_count) * (opt.key_size + opt.value_size)) / 1000 /
          1000) +
        "MB");

    /* Clean up any artifacts from prior runs. */
    testutil_remove(opt.home_path.c_str());

    /* Initialize. */
    initialize();

    std::string shared_open_config = CONNECTION_CREATE +
      ",cache_size=" + std::to_string(opt.cache_size_gb) + "GB,precise_checkpoint=true";
    std::string extension_config = ",extensions=[../../ext/page_log/palite/libwiredtiger_palite.so";
    std::string shared_disagg_config = ",disaggregated=(page_log=palite";

    /* Populate the database. */
    if (opt.load_skip) {
        logger::log_msg(LOG_INFO, "Using existing database.");
        logger::log_msg(
          LOG_INFO, "Copying \"" + opt.home_path + ".back\" to \"" + opt.home_path + "\"");
        testutil_copy(std::string(opt.home_path + ".back").c_str(), opt.home_path.c_str());
        database_model->add_existing_collections(opt.collection_count, opt.key_count);
    } else {
        connection_manager::instance().create(
          shared_open_config + extension_config + "]" + shared_disagg_config + ",role=\"leader\",)",
          opt.home_path);
        /*
         * We take a checkpoint as the very last stop of populate, this means we don't need to
         * abandon any work. Abandoning a checkpoint is very slow and makes the perf tests results
         * relatively meaningless.*
         */
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

    std::string other_config =
      ",statistics_log=(json,wait=1,on_close),statistics=(all),file_manager=(close_idle_time=600,"
      "close_handle_minimum=2000)";
    connection_manager::instance().reopen(shared_open_config + shared_disagg_config +
        ",role=\"follower\",)" + other_config + extension_config +
        (opt.verbose_level > 1 ?
            "=(config=\"(verbose=" + std::to_string(opt.verbose_level - 1) + ")\")" :
            "") +
        "]," +
        (opt.verbose_level >= 1 ?
            "verbose=(disaggregated_storage:" + std::to_string(opt.verbose_level) + ")" :
            ""),
      opt.home_path);
    WT_CONNECTION *conn = connection_manager::instance().get_connection();

    /* If we loaded an existing database, query the stable timestamp. */
    if (opt.load_skip) {
        char timestamp[256];
        conn->query_timestamp(conn, timestamp, "get=stable");
        uint64_t stable_timestamp = timestamp_manager::hex_to_decimal(timestamp);
        logger::log_msg(LOG_INFO,
          "Queried stable timestamp from WiredTiger: " + std::to_string(stable_timestamp));
        ts = stable_timestamp + 1;
    }

    /* Pickup the latest checkpoint after starting in follower mode. */
    wt_timestamp_t timestamp = wt_disagg_pick_up_latest_checkpoint();

    /* Optionally scan created tables to warm the WT cache. */
    if (opt.warm_cache_pct > 0)
        cache_warming(opt.collection_count * opt.key_count * opt.warm_cache_pct / 100);

    crud_operations();

    conn->reconfigure(conn, "disaggregated=(role=\"leader\")");
    std::string stable_config = "stable_timestamp=" + timestamp_manager::decimal_to_hex(timestamp);
    conn->set_timestamp(conn, stable_config.c_str());
    /* Sleep for 10 seconds, hopefully this will help with FTDC files. */
    std::this_thread::sleep_for(std::chrono::seconds(10));
    return (0);
}
