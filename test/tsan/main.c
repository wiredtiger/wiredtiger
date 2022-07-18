/*
 * Built with:
 * cmake -DCMAKE_TOOLCHAIN_FILE=../cmake/toolchains/mongodbtoolchain_v3_clang.cmake
 * -DCMAKE_BUILD_TYPE=TSan -DENABLE_PYTHON=1 -DENABLE_LZ4=1 -DENABLE_SNAPPY=1
 * -DENABLE_ZLIB=1 -DENABLE_ZSTD=1 -DHAVE_DIAGNOSTIC=1 -DENABLE_STRICT=1
 * -DCMAKE_EXPORT_COMPILE_COMMANDS=ON . -G Ninja ../.
 *
 * Then:
 * ninja
 */

#include <stdlib.h>

#include "test_util.h"

extern char *__wt_optarg;
extern int __wt_optind;

static char* home;

static int
log_print_err_worker(const char *func, int line, const char *m, int e)
{
    fprintf(stderr, "%s: %s,%d: %s: %s\n", progname, func, line, m, wiredtiger_strerror(e));
    fflush(stderr);
    return (e);
}

#define log_print_err(m, e) log_print_err_worker(__func__, __LINE__, m, e)

static int
usage(void)
{
    fprintf(stderr, "usage: %s [-h home]\n", progname);
    return (EXIT_FAILURE);
}

int main(int argc, char** argv) {
    int ch, ret;
    WT_CONNECTION* conn;
    char* working_dir;

    (void)testutil_set_progname(argv);

    working_dir = NULL;
    while ((ch = __wt_getopt(progname, argc, argv, "h:")) != EOF)
        switch (ch) {
        case 'h':
            working_dir = __wt_optarg;
            break;
        default:
            return (usage());
        }

    argc -= __wt_optind;
    if (argc != 0)
        return (usage());

    home = dmalloc(512);
    testutil_work_dir_from_path(home, 512, working_dir);

    if ((ret = wiredtiger_open(home, NULL, "create", &conn)) != 0) {
        return log_print_err("wiredtiger_open", ret);
    }

    return (EXIT_SUCCESS);
}
