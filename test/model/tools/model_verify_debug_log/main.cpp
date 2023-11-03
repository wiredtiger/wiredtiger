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

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <sstream>

#include "wiredtiger.h"
extern "C" {
#include "wt_internal.h"
}

#include "model/driver/debug_log_parser.h"
#include "model/kv_database.h"

/*
 * Command-line arguments.
 */
extern int __wt_optind, __wt_optwt;
extern char *__wt_optarg;

/*
 * Configuration.
 */
/*#define ENV_CONFIG                                            \
    "cache_size=20M,create,"                                  \
    "debug_mode=(table_logging=true,checkpoint_retention=5)," \
    "eviction_updates_target=20,eviction_updates_trigger=90," \
    "log=(enabled,file_max=10M,remove=true),session_max=100," \
    "statistics=(all),statistics_log=(wait=1,json,on_close)"*/

/*
 * usage --
 *     Print usage help for the program. (Don't exit.)
 */
static void
usage(const char *progname)
{
    fprintf(stderr, "usage: %s DEBUG_LOG_JSON\n", progname);
}

/*
 * main --
 *     The main entry point for the test.
 */
int
main(int argc, char *argv[])
{
    const char *progname;
    int ch, ret;

    progname = argv[0];

    /*
     * Parse the command-line arguments.
     */
    __wt_optwt = 1;
    while ((ch = __wt_getopt(progname, argc, argv, "?")) != EOF)
        switch (ch) {
        case '?':
            usage(progname);
            return EXIT_SUCCESS;
        default:
            usage(progname);
            return EXIT_FAILURE;
        }
    argc -= __wt_optind;
    if (argc != 1) {
        usage(progname);
        return EXIT_FAILURE;
    }

    const char *debug_log_json = argv[__wt_optind];

    /*
     * Verify.
     */
    try {
        ret = EXIT_SUCCESS;

        std::cout << "Loading " << debug_log_json << std::endl;
        model::kv_database db;
        model::debug_log_parser::parse_json(db, debug_log_json);
    } catch (std::exception &e) {
        std::cerr << "Verification failed: " << e.what() << std::endl;
        ret = EXIT_FAILURE;
    }

    return ret;
}
