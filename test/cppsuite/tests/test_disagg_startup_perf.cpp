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
 * [test_disagg_startup_perf]: Create N layered tables, checkpoint them into palite, then restart
 * three times. The perf file reports the average of those three runs, in milliseconds:
 * - startup_open_avg_ms: wiredtiger_open.
 * - startup_pickup_avg_ms: checkpoint pickup.
 * - startup_avg_ms: both stages.
 */

#include "src/common/constants.h"
#include "src/common/logger.h"
#include "src/component/timestamp_manager.h"
#include "src/storage/connection_manager.h"
#include "src/storage/scoped_session.h"
#include "src/main/database.h"
#include "src/component/metrics_writer.h"

extern "C" {
#include "wiredtiger.h"
#include "test_util.h"
}

extern "C" {
extern int __wt_optind;
extern char *__wt_optarg;
}

#include <chrono>
#include <iostream>
#include <string>

using namespace test_harness;

static const int RESTARTS = 3;

struct options {
    int collection_count = 100;
    int cache_size_gb = 4;
    int verbose_level = 0;
    std::string home_path = DEFAULT_DIR;
};

static options opt;
static database *database_model;

static int64_t
elapsed_ms(std::chrono::steady_clock::time_point start, std::chrono::steady_clock::time_point end)
{
    return (std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count());
}

static bool
parse_options(int argc, char *argv[], options &out, std::string &error)
{
    int ch;

    while ((ch = __wt_getopt("test_disagg_startup_perf", argc, argv, "c:g:h:V:")) != EOF) {
        switch (ch) {
        case 'c':
            out.collection_count = atoi(__wt_optarg);
            if (out.collection_count <= 0) {
                error = "invalid collection_count: " + std::string(__wt_optarg);
                return (false);
            }
            break;
        case 'g':
            out.cache_size_gb = atoi(__wt_optarg);
            if (out.cache_size_gb <= 0) {
                error = "invalid cache_size_gb: " + std::string(__wt_optarg);
                return (false);
            }
            break;
        case 'h':
            out.home_path = __wt_optarg;
            break;
        case 'V':
            out.verbose_level = atoi(__wt_optarg);
            if (out.verbose_level < 0) {
                error = "invalid verbose_level: " + std::string(__wt_optarg);
                return (false);
            }
            break;
        case '?':
        default:
            error = "unknown option";
            return (false);
        }
    }
    return (true);
}

/*
 * wt_disagg_pick_up_latest_checkpoint --
 *     Install the latest complete palite checkpoint.
 */
static void
wt_disagg_pick_up_latest_checkpoint()
{
    WT_CONNECTION *conn = connection_manager::instance().get_connection();
    scoped_session session = connection_manager::instance().create_session();
    WT_PAGE_LOG *page_log;
    testutil_check(conn->get_page_log(conn, "palite", &page_log));

    WT_PAGE_LOG_GET_COMPLETE_CHECKPOINT_ARGS args{};
    testutil_check(page_log->pl_get_complete_checkpoint(page_log, session.get(), &args));

    page_log->terminate(page_log, NULL);
    page_log = NULL;

    char *checkpoint_meta =
      strndup((const char *)args.checkpoint_metadata.data, args.checkpoint_metadata.size);
    free(args.checkpoint_metadata.mem);

    std::string config = "disaggregated=(checkpoint_meta=\"" + std::string(checkpoint_meta) + "\")";
    free(checkpoint_meta);
    testutil_check(conn->reconfigure(conn, config.c_str()));
}

static std::string
extension_config()
{
    std::string ext = ",extensions=[../../ext/page_log/palite/libwiredtiger_palite.so";
    if (opt.verbose_level > 1)
        ext += "=(config=\"(verbose=" + std::to_string(opt.verbose_level - 1) + ")\")";
    ext += "]";
    if (opt.verbose_level >= 1)
        ext += ",verbose=(disaggregated_storage:1)";
    return (ext);
}

static std::string
base_config()
{
    return (CONNECTION_CREATE + ",cache_size=" + std::to_string(opt.cache_size_gb) +
      "GB,precise_checkpoint=true,disaggregated=(page_log=palite");
}

static void
setup()
{
    logger::log_msg(
      LOG_INFO, "Setup: creating " + std::to_string(opt.collection_count) + " layered tables.");
    connection_manager::instance().create(
      base_config() + ",role=\"leader\")" + extension_config(), opt.home_path);

    database_model = new database();
    database_model->set_create_config(false, false, true);
    {
        scoped_session session = connection_manager::instance().create_session();
        for (int i = 0; i < opt.collection_count; ++i) {
            database_model->add_collection(session, 0);
            if ((i + 1) % 1000 == 0)
                logger::log_msg(LOG_INFO,
                  "Setup: created " + std::to_string(i + 1) + "/" +
                    std::to_string(opt.collection_count) + " tables.");
        }

        connection_manager::instance().set_timestamp(STABLE_TS + "=1");
        testutil_check(session->checkpoint(session.get(), nullptr));
    }
    logger::log_msg(LOG_INFO, "Setup: checkpoint stored in palite.");
    connection_manager::instance().close();
}

static void
restart_once(int n, int64_t *open_msp, int64_t *pickup_msp)
{
    logger::log_msg(LOG_INFO, "Restart " + std::to_string(n) + ": opening as follower.");
    auto open_start = std::chrono::steady_clock::now();
    connection_manager::instance().reopen(base_config() +
        ",role=\"follower\",lose_all_my_data=true),statistics=(all)" + extension_config(),
      opt.home_path);
    auto open_end = std::chrono::steady_clock::now();

    logger::log_msg(LOG_INFO, "Restart " + std::to_string(n) + ": picking up checkpoint.");
    auto pickup_start = std::chrono::steady_clock::now();
    wt_disagg_pick_up_latest_checkpoint();
    auto pickup_end = std::chrono::steady_clock::now();

    *open_msp = elapsed_ms(open_start, open_end);
    *pickup_msp = elapsed_ms(pickup_start, pickup_end);
    logger::log_msg(LOG_INFO,
      "Restart " + std::to_string(n) + ": open " + std::to_string(*open_msp) + " ms, pickup " +
        std::to_string(*pickup_msp) + " ms.");
    connection_manager::instance().close();
}

int
main(int argc, char *argv[])
{
    const std::string progname = testutil_set_progname(argv);
    logger::trace_level = LOG_INFO;

    std::string err;
    if (!parse_options(argc, argv, opt, err)) {
        std::cerr << "error: " << err << "\n";
        std::cerr << "usage: " << argv[0] << " [options]\n"
                  << "  -c N     collection_count (int > 0)\n"
                  << "  -g N     cache_size_gb (int > 0)\n"
                  << "  -h PATH  home_path\n"
                  << "  -V N     verbosity level\n";
        return (1);
    }

    logger::log_msg(LOG_INFO, "collection_count = " + std::to_string(opt.collection_count));
    logger::log_msg(LOG_INFO, "cache_size_gb = " + std::to_string(opt.cache_size_gb));

    testutil_remove(opt.home_path.c_str());
    setup();

    int64_t open_sum = 0, pickup_sum = 0, total_sum = 0;
    for (int i = 1; i <= RESTARTS; ++i) {
        int64_t open_ms, pickup_ms;
        restart_once(i, &open_ms, &pickup_ms);
        open_sum += open_ms;
        pickup_sum += pickup_ms;
        total_sum += open_ms + pickup_ms;
    }

    metrics_writer::instance().add_stat("startup_open_avg_ms", open_sum / RESTARTS);
    metrics_writer::instance().add_stat("startup_pickup_avg_ms", pickup_sum / RESTARTS);
    metrics_writer::instance().add_stat("startup_avg_ms", total_sum / RESTARTS);
    metrics_writer::instance().output_perf_file(progname);

    delete database_model;
    logger::log_msg(LOG_INFO, "Test completed successfully.");
    return (0);
}
