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
#include <test_util.h>

#define MAX_ITERATIONS 5
#define MAX_KEYS 100000

static const char *const conn_config =
  "create,cache_size=100MB,log=(archive=false,enabled=true,file_max=100K)";
static const char *const home_live = "WT_HOME_LOG";
static const char *const home_full = "WT_HOME_LOG_FULL";
static const char *const home_incr = "WT_HOME_LOG_INCR";
static const char *const home_incr_copy = "WT_HOME_LOG_INCR_COPY";
static const char *const uri = "table:logtest";
static const char *const full_out = "./backup_full";
static const char *const incr_out = "./backup_incr";

static WT_CONNECTION *conn = NULL;
static WT_SESSION *session = NULL;

/*
 * dump_table --
 *     Dumps the table content in to the file in human-readable format.
 */
static void
dump_table(const char *home, const char *table, const char *out_file)
{
    char buf[1024];

    testutil_check(
      __wt_snprintf(buf, sizeof(buf), "./wt -R -h %s dump %s > %s", home, table, out_file));
    testutil_check(system(buf));
}

/*
 * reset_dir --
 *     Recreates the directory.
 */
static void
reset_dir(const char *dir)
{
    char buf[1024];

    testutil_check(__wt_snprintf(buf, sizeof(buf), "rm -rf %s && mkdir %s", dir, dir));
    testutil_check(system(buf));
}

/*
 * remove_dir --
 *     Removes the directory.
 */
static void
remove_dir(const char *dir)
{
    char buf[1024];

    testutil_check(__wt_snprintf(buf, sizeof(buf), "rm -rf %s", dir));
    testutil_check(system(buf));
}

/*
 * compare_backups --
 *     Compares the full and the incremental backups.
 */
static int
compare_backups(void)
{
    int ret;
    char buf[1024];

    /*
     * We have to copy incremental backup to keep the original database intact. Otherwise we'll get
     * "Incremental backup after running recovery is not allowed".
     */
    testutil_check(__wt_snprintf(buf, sizeof(buf), "cp %s/* %s", home_incr, home_incr_copy));
    testutil_check(system(buf));

    /* Dump both backups. */
    dump_table(home_full, uri, full_out);
    dump_table(home_incr_copy, uri, incr_out);

    reset_dir(home_incr_copy);

    /* Compare the files. */
    testutil_check(__wt_snprintf(buf, sizeof(buf), "cmp %s %s", full_out, incr_out));
    if ((ret = system(buf)) != 0) {
        printf(
          "Tables \"%s\" don't match in \"%s\" and \"%s\"!\n See \"%s\" and \"%s\" for details.\n",
          uri, home_full, home_incr_copy, full_out, incr_out);
        exit(1);
    } else {
        /* If they compare successfully, clean up. */
        testutil_check(__wt_snprintf(buf, sizeof(buf), "rm %s %s", full_out, incr_out));
        testutil_check(system(buf));
        printf("\t Table \"%s\": OK\n", uri);
    }

    return (ret);
}

/*
 * add_work --
 *     Inserts some data into the database.
 */
static void
add_work(int iter)
{
    WT_CURSOR *cursor;
    int i;
    char k[32], v[128];

    testutil_check(session->open_cursor(session, uri, NULL, NULL, &cursor));

    /* Perform some operations with individual auto-commit transactions. */
    for (i = 0; i < MAX_KEYS; i++) {
        testutil_check(__wt_snprintf(k, sizeof(k), "key.%d.%d", iter, i));
        testutil_check(__wt_snprintf(v, sizeof(v), "value.%d.%d", iter, i));
        cursor->set_key(cursor, k);
        cursor->set_value(cursor, v);
        testutil_check(cursor->insert(cursor));
    }
    testutil_check(cursor->close(cursor));
}

/*
 * take_full_backup --
 *     Takes full backup of the database.
 */
static void
take_full_backup(const char *home, const char *backup_home)
{
    WT_CURSOR *cursor;
    int ret;
    char buf[1024];
    const char *filename;

    testutil_check(session->open_cursor(session, "backup:", NULL, NULL, &cursor));

    while ((ret = cursor->next(cursor)) == 0) {
        testutil_check(cursor->get_key(cursor, &filename));
        testutil_check(
          __wt_snprintf(buf, sizeof(buf), "cp %s/%s %s/%s", home, filename, backup_home, filename));
        testutil_check(system(buf));
    }

    scan_end_check(ret == WT_NOTFOUND);
    testutil_check(cursor->close(cursor));
}

/*
 * take_incr_backup --
 *     Takes incremental log-based backup of the database.
 */
static void
take_incr_backup(const char *backup_home, bool truncate_logs)
{
    WT_CURSOR *cursor;
    int ret;
    char buf[1024];
    const char *filename;

    testutil_check(session->open_cursor(session, "backup:", NULL, "target=(\"log:\")", &cursor));

    while ((ret = cursor->next(cursor)) == 0) {
        testutil_check(cursor->get_key(cursor, &filename));

        testutil_check(__wt_snprintf(
          buf, sizeof(buf), "cp %s/%s %s/%s", home_live, filename, backup_home, filename));
        testutil_check(system(buf));
    }
    scan_end_check(ret == WT_NOTFOUND);

    if (truncate_logs) {
        /*
         * With an incremental cursor, we want to truncate on the backup cursor to archive the logs.
         * Only do this if the copy process was entirely successful.
         */
        testutil_check(session->truncate(session, "log:", cursor, NULL, NULL));
    }

    testutil_check(cursor->close(cursor));
}

/*
 * prepare_folders --
 *     Prepares all working folders required for the test.
 */
static void
prepare_folders(void)
{
    reset_dir(home_live);
    reset_dir(home_full);
    reset_dir(home_incr);
    reset_dir(home_incr_copy);
}

/*
 * cleanup --
 *     Test's cleanup.
 */
static void
cleanup(void)
{
    testutil_check(conn->close(conn, NULL));

    remove_dir(home_full);
    remove_dir(home_incr);
    remove_dir(home_live);
    remove_dir(home_incr_copy);
}

/*
 * reopen_conn --
 *     Close and reopen connection to the database.
 */
static void
reopen_conn(void)
{
    if (conn != NULL) {
        printf("Reopening connection\n");
        testutil_check(conn->close(conn, NULL));
        conn = NULL;
        session = NULL;
    }

    testutil_check(wiredtiger_open(home_live, NULL, conn_config, &conn));
    testutil_check(conn->open_session(conn, NULL, NULL, &session));
}

/*
 * validate --
 *     Validate the database against incremental backup. To do that we need to take a full backup of
 *     the database. Also we have to make a copy of the incremental backup to avoid "Incremental
 *     backup after running recovery is not allowed" error.
 */
static void
validate(bool after_reconnect)
{
    /*
     * The full backup here is only needed for testing and comparison purposes. A normal incremental
     * backup procedure would not include this.
     */
    printf("Taking full backup\n");
    take_full_backup(home_live, home_full);

    /*
     * Taking the incremental backup also calls truncate to archive the log files, if the copies
     * were successful. See that function for details on that call. The truncation only happens
     * after we reconnected to the database.
     */
    printf("Taking incremental backup\n");
    take_incr_backup(home_incr, after_reconnect);

    /*
     * Dump tables from the full backup and incremental backup databases, and compare the dumps.
     */
    printf("Dumping and comparing data\n");
    testutil_check(compare_backups());
    reset_dir(home_full);
}

/*
 * main --
 *     Test's entry point.
 */
int
main(int argc, char *argv[])
{
    /* Original code was taken from examples/c/ex_backup.c */
    int i;

    (void)argc; /* Unused variable */
    (void)testutil_set_progname(argv);

    prepare_folders();

    reopen_conn();
    testutil_check(session->create(session, uri, "key_format=S,value_format=S"));

    printf("Taking initial backup into incremental backup folder\n");
    take_full_backup(home_live, home_incr);

    for (i = 1; i <= MAX_ITERATIONS; i++) {
        printf("==================================\n");
        printf("Iteration %d:\n", i);
        printf("==================================\n");

        printf("Adding data\n");
        add_work(i);
        testutil_check(session->checkpoint(session, NULL));

        /* Validate database against incremental backup. */
        validate(false);

        /* Reopen connection. */
        reopen_conn();

        /* Validate database again. */
        validate(true);
    }

    cleanup();

    return (EXIT_SUCCESS);
}
