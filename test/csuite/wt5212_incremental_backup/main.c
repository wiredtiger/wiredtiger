/*-
 * Public Domain 2014-2020 MongoDB, Inc.
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
 * JIRA ticket reference: WT-5212 Incremental Backup data validation tests
 */

#include "test_util.h"

static const char *const home = "WT_BLOCK";
static const char *const home_full = "WT_BLOCK_LOG_FULL";
static const char *const home_incr = "WT_BLOCK_LOG_INCR";
static const char *const logpath = "logpath";

#define WT_UTIL "../../../wt"
#define WTLOG "WiredTigerLog"
#define WTLOGLEN strlen(WTLOG)

static const char *const full_out = "./backup_block_full";
static const char *const incr_out = "./backup_block_incr";

static const char *const uri = "table:main";
static const char *const uri2 = "table:extra";
static const char *const uri3 = "table:new_table";
static const char *const uri4 = "table:logged_table";
static const char *const uri5 = "table:not_logged_table";

typedef struct __filelist {
    const char *name;
    bool exist;
} FILELIST;

static FILELIST *last_flist = NULL;
static size_t filelist_count = 0;

#define FLIST_INIT 16

#define CONN_CONFIG "create,cache_size=100MB,log=(enabled=true,path=logpath,file_max=100K)"
// Max iterations are (ITERATIONS_MULTIPLIER * 3) + 2 = 17
#define ITERATIONS_MULTIPLIER 5
#define MAX_ITERATIONS 17
#define MAX_KEYS 10000

// The variable is to copy full file instead of copying blocks for new created objects
static bool new_object = false;

static int
compare_backups(int i, const char *t_uri)
{
    int ret;
    char buf[1024], msg[32];

    /*
     * We run 'wt dump' on both the full backup directory and the
     * incremental backup directory for this iteration.  Since running
     * 'wt' runs recovery and makes both directories "live", we need
     * a new directory for each iteration.
     *
     * If i == 0, we're comparing against the main, original directory
     * with the final incremental directory.
     */
    if (i == 0)
        testutil_check(__wt_snprintf(
          buf, sizeof(buf), "%s -R -h %s dump %s > %s.%d", WT_UTIL, home, t_uri, full_out, i));
    else
        testutil_check(__wt_snprintf(buf, sizeof(buf), "%s -R -h %s.%d dump %s > %s.%d", WT_UTIL,
          home_full, i, t_uri, full_out, i));
    testutil_check(system(buf));
    /*
     * Now run dump on the incremental directory.
     */
    testutil_check(__wt_snprintf(buf, sizeof(buf), "%s -R -h %s.%d dump %s > %s.%d", WT_UTIL,
      home_incr, i, t_uri, incr_out, i));
    testutil_check(system(buf));

    /*
     * Compare the files.
     */
    testutil_check(__wt_snprintf(buf, sizeof(buf), "cmp %s.%d %s.%d", full_out, i, incr_out, i));
    ret = system(buf);
    if (i == 0)
        testutil_check(__wt_snprintf(msg, sizeof(msg), "%s", "MAIN"));
    else
        testutil_check(__wt_snprintf(msg, sizeof(msg), "%d", i));
    printf("Iteration %s: Tables %s.%d and %s.%d %s\n", msg, full_out, i, incr_out, i,
      ret == 0 ? "identical" : "differ");
    if (ret != 0)
        exit(1);

    /*
     * If they compare successfully, clean up.
     */
    if (i != 0) {
        testutil_check(__wt_snprintf(buf, sizeof(buf), "rm -rf %s.%d %s.%d %s.%d %s.%d", home_full,
          i, home_incr, i, full_out, i, incr_out, i));
        testutil_check(system(buf));
    }
    return (ret);
}

/*
 * Set up all the directories needed for the test. We have a full backup directory for each
 * iteration and an incremental backup for each iteration. That way we can compare the full and
 * incremental each time through.
 */
static void
setup_directories(void)
{
    int i;
    char buf[1024];

    for (i = 0; i <= MAX_ITERATIONS; i++) {
        /*
         * For incremental backups we need 0-N. The 0 incremental directory will compare with the
         * original at the end.
         */
        testutil_check(__wt_snprintf(buf, sizeof(buf), "rm -rf %s.%d && mkdir -p %s.%d/%s",
          home_incr, i, home_incr, i, logpath));
        testutil_check(system(buf));
        if (i == 0)
            continue;
        /*
         * For full backups we need 1-N.
         */
        testutil_check(__wt_snprintf(buf, sizeof(buf), "rm -rf %s.%d && mkdir -p %s.%d/%s",
          home_full, i, home_full, i, logpath));
        testutil_check(system(buf));
    }
}

static void
add_work(WT_SESSION *session, int iter, int iterj, const char *t_uri1, const char *t_uri2)
{
    WT_CURSOR *cursor, *cursor2;
    int i;
    char k[64], v[64];

    testutil_check(session->open_cursor(session, t_uri1, NULL, NULL, &cursor));

    /*
     * Only on even iterations add content to the extra table. This illustrates and shows that
     * sometimes only some tables will be updated.
     */
    cursor2 = NULL;
    if (iter % 2 == 0) {
        testutil_check(session->open_cursor(session, t_uri2, NULL, NULL, &cursor2));
    }
    /*
     * Perform some operations with individual auto-commit transactions.
     */
    for (i = 0; i < MAX_KEYS; i++) {
        testutil_check(__wt_snprintf(k, sizeof(k), "key.%d.%d.%d", iter, iterj, i));
        testutil_check(__wt_snprintf(v, sizeof(v), "value.%d.%d.%d", iter, iterj, i));
        cursor->set_key(cursor, k);
        cursor->set_value(cursor, v);
        testutil_check(cursor->insert(cursor));
        if (cursor2 != NULL) {
            cursor2->set_key(cursor2, k);
            cursor2->set_value(cursor2, v);
            testutil_check(cursor2->insert(cursor2));
        }
    }
    testutil_check(cursor->close(cursor));
    if (cursor2 != NULL)
        testutil_check(cursor2->close(cursor2));
}

static void
add_bulk_load(WT_SESSION *session, int iter_i, int iter_j, const char *t_uri)
{
    WT_CURSOR *cursor;
    int i;
    int k;
    char v[64];

    testutil_check(session->open_cursor(session, t_uri, NULL, "bulk", &cursor));

    for (i = 0; i < MAX_KEYS; i++) {
        k = iter_i + iter_j + i;
        testutil_check(__wt_snprintf(v, sizeof(v), "value.%d.%d.%d", iter_i, iter_j, i));

        cursor->set_key(cursor, k);
        cursor->set_value(cursor, v);
        testutil_check(cursor->insert(cursor));
    }
    testutil_check(cursor->close(cursor));
}

static void
remove_work(WT_SESSION *session, int iter_i, int iter_j)
{
    WT_CURSOR *cursor;
    int i;
    char k[64], v[64];

    testutil_check(session->open_cursor(session, uri, NULL, NULL, &cursor));

    /*
     * Remove records from the main table.
     */
    for (i = 0; i < MAX_KEYS; i++) {
        testutil_check(__wt_snprintf(k, sizeof(k), "key.%d.%d.%d", iter_i, iter_j, i));
        testutil_check(__wt_snprintf(v, sizeof(v), "value.%d.%d.%d", iter_i, iter_j, i));
        cursor->set_key(cursor, k);

        testutil_check(cursor->remove(cursor));
    }
    testutil_check(cursor->close(cursor));
}

static void
finalize_files(FILELIST *flistp, size_t count)
{
    size_t i;
    char buf[512];

    /*
     * Process files that were removed. Any file that is not marked in the previous list as existing
     * in this iteration should be removed. Free all previous filenames as we go along. Then free
     * the overall list.
     */
    for (i = 0; i < filelist_count; ++i) {
        if (last_flist[i].name == NULL)
            break;
        if (!last_flist[i].exist) {
            testutil_check(__wt_snprintf(buf, sizeof(buf), "rm WT_BLOCK_LOG_*/%s%s",
              strncmp(last_flist[i].name, WTLOG, WTLOGLEN) == 0 ? "logpath/" : "",
              last_flist[i].name));
            testutil_check(system(buf));
        }
        free((void *)last_flist[i].name);
    }
    free(last_flist);

    /* Set up the current list as the new previous list. */
    last_flist = flistp;
    filelist_count = count;
}

/*
 * Process a file name. Build up a list of current file names. But also process the file names from
 * the previous iteration. Mark any name we see as existing so that the finalize function can remove
 * any that don't exist. We walk the list each time. This is slow.
 */
static void
process_file(FILELIST **flistp, size_t *countp, size_t *allocp, const char *filename)
{
    FILELIST *flist;
    size_t alloc, i, new, orig;

    /* Build up the current list, growing as needed. */
    i = *countp;
    alloc = *allocp;
    flist = *flistp;
    if (i == alloc) {
        orig = alloc * sizeof(FILELIST);
        new = orig * 2;
        flist = realloc(flist, new);
        testutil_assert(flist != NULL);
        memset(flist + alloc, 0, new - orig);
        *allocp = alloc * 2;
        *flistp = flist;
    }

    flist[i].name = strdup(filename);
    flist[i].exist = false;
    ++(*countp);

    /* Check against the previous list. */
    for (i = 0; i < filelist_count; ++i) {
        /* If name is NULL, we've reached the end of the list. */
        if (last_flist[i].name == NULL)
            break;
        if (strcmp(filename, last_flist[i].name) == 0) {
            last_flist[i].exist = true;
            break;
        }
    }
}

static void
take_full_backup(WT_SESSION *session, int i)
{
    FILELIST *flist;
    WT_CURSOR *cursor;
    size_t alloc, count;
    int j, ret;
    char buf[1024], f[256], h[256];
    const char *filename, *hdir;

    /*
     * First time through we take a full backup into the incremental directories. Otherwise only
     * into the appropriate full directory.
     */
    if (i != 0) {
        testutil_check(__wt_snprintf(h, sizeof(h), "%s.%d", home_full, i));
        hdir = h;
    } else
        hdir = home_incr;
    if (i == 0) {
        testutil_check(__wt_snprintf(
          buf, sizeof(buf), "incremental=(granularity=1M,enabled=true,this_id=ID%d)", i));
        testutil_check(session->open_cursor(session, "backup:", NULL, buf, &cursor));
    } else
        testutil_check(session->open_cursor(session, "backup:", NULL, NULL, &cursor));

    count = 0;
    alloc = FLIST_INIT;
    flist = calloc(alloc, sizeof(FILELIST));
    testutil_assert(flist != NULL);
    while ((ret = cursor->next(cursor)) == 0) {
        testutil_check(cursor->get_key(cursor, &filename));
        process_file(&flist, &count, &alloc, filename);

        /*
         * If it is a log file, prepend the path for cp.
         */
        if (strncmp(filename, WTLOG, WTLOGLEN) == 0)
            testutil_check(__wt_snprintf(f, sizeof(f), "%s/%s", logpath, filename));
        else
            testutil_check(__wt_snprintf(f, sizeof(f), "%s", filename));

        if (i == 0)
            /*
             * Take a full backup into each incremental directory.
             */
            for (j = 0; j < ITERATIONS_MULTIPLIER * 3; j++) {
                testutil_check(__wt_snprintf(h, sizeof(h), "%s.%d", home_incr, j));
                testutil_check(__wt_snprintf(buf, sizeof(buf), "cp %s/%s %s/%s", home, f, h, f));
#if 0
                printf("FULL: Copy: %s\n", buf);
#endif
                testutil_check(system(buf));
            }
        else {
#if 0
            testutil_check(__wt_snprintf(h, sizeof(h), "%s.%d", home_full, i);
#endif
            testutil_check(__wt_snprintf(buf, sizeof(buf), "cp %s/%s %s/%s", home, f, hdir, f));
#if 0
            printf("FULL %d: Copy: %s\n", i, buf);
#endif
            testutil_check(system(buf));
        }
    }
    scan_end_check(ret == WT_NOTFOUND);
    testutil_check(cursor->close(cursor));
    finalize_files(flist, count);
}

static void
take_incr_backup(WT_SESSION *session, int i)
{
    FILELIST *flist;
    WT_CURSOR *backup_cur, *incr_cur;
    size_t alloc, count, rdsize, tmp_sz;
    uint64_t offset, size, type;
    int j, ret, rfd, wfd;
    char buf[1024], h[256], *tmp;
    const char *filename;
    bool first;

    tmp = NULL;
    tmp_sz = 0;
    /* Open the backup data source for incremental backup. */
    testutil_check(
      __wt_snprintf(buf, sizeof(buf), "incremental=(src_id=ID%d,this_id=ID%d)", i - 1, i));
    testutil_check(session->open_cursor(session, "backup:", NULL, buf, &backup_cur));
    rfd = wfd = -1;
    count = 0;
    alloc = FLIST_INIT;
    flist = calloc(alloc, sizeof(FILELIST));
    testutil_assert(flist != NULL);
    /* For each file listed, open a duplicate backup cursor and copy the blocks. */
    while ((ret = backup_cur->next(backup_cur)) == 0) {
        testutil_check(backup_cur->get_key(backup_cur, &filename));
        process_file(&flist, &count, &alloc, filename);
        testutil_check(__wt_snprintf(h, sizeof(h), "%s.0", home_incr));
        if (strncmp(filename, WTLOG, WTLOGLEN) == 0)
            testutil_check(__wt_snprintf(buf, sizeof(buf), "cp %s/%s/%s %s/%s/%s", home, logpath,
              filename, h, logpath, filename));
        else
            testutil_check(
              __wt_snprintf(buf, sizeof(buf), "cp %s/%s %s/%s", home, filename, h, filename));
#if 0
        printf("Copying backup: %s\n", buf);
#endif
        testutil_check(system(buf));
        first = true;

        testutil_check(__wt_snprintf(buf, sizeof(buf), "incremental=(file=%s)", filename));
        testutil_check(session->open_cursor(session, NULL, backup_cur, buf, &incr_cur));
#if 0
        printf("Taking incremental %d: File %s\n", i, filename);
#endif
        while ((ret = incr_cur->next(incr_cur)) == 0) {
            testutil_check(incr_cur->get_key(incr_cur, &offset, &size, &type));
            scan_end_check(type == WT_BACKUP_FILE || type == WT_BACKUP_RANGE);
#if 0
            printf("Incremental %s: KEY: Off %" PRIu64 " Size: %" PRIu64 " %s\n", filename, offset,
              size, type == WT_BACKUP_FILE ? "WT_BACKUP_FILE" : "WT_BACKUP_RANGE");
#endif
            /*
             * The condition (new_object == false) is to skip copying blocks for new created
             * objects. The code below tries to open a write file descriptor on the newly created
             * files to copy the blocks and it fails because the newly created file does not exist
             * in the incremental directory
             */
            if ((type == WT_BACKUP_RANGE) && (new_object == false)) {
                /*
                 * We should never get a range key after a whole file so the read file descriptor
                 * should be valid. If the read descriptor is valid, so is the write one.
                 */
                if (tmp_sz < size) {
                    tmp = realloc(tmp, size);
                    testutil_assert(tmp != NULL);
                    tmp_sz = size;
                }
                if (first) {
                    testutil_check(__wt_snprintf(buf, sizeof(buf), "%s/%s", home, filename));
                    error_sys_check(rfd = open(buf, O_RDONLY, 0));
                    testutil_check(__wt_snprintf(h, sizeof(h), "%s.%d", home_incr, i));
                    testutil_check(__wt_snprintf(buf, sizeof(buf), "%s/%s", h, filename));
                    error_sys_check(wfd = open(buf, O_WRONLY, 0));
                    first = false;
                }

                error_sys_check(lseek(rfd, (wt_off_t)offset, SEEK_SET));
                error_sys_check(rdsize = (size_t)read(rfd, tmp, (size_t)size));
                error_sys_check(lseek(wfd, (wt_off_t)offset, SEEK_SET));
                /* Use the read size since we may have read less than the granularity. */
                error_sys_check(write(wfd, tmp, rdsize));
            } else {
                /* Whole file, so close both files and just copy the whole thing. */
                testutil_assert(first == true);
                rfd = wfd = -1;
                if (strncmp(filename, WTLOG, WTLOGLEN) == 0)
                    testutil_check(__wt_snprintf(buf, sizeof(buf), "cp %s/%s/%s %s/%s/%s", home,
                      logpath, filename, h, logpath, filename));
                else
                    testutil_check(__wt_snprintf(
                      buf, sizeof(buf), "cp %s/%s %s/%s", home, filename, h, filename));
#if 0
                printf("Incremental: Whole file copy: %s\n", buf);
#endif
                testutil_check(system(buf));
            }
        }
        scan_end_check(ret == WT_NOTFOUND);
        /* Done processing this file. Close incremental cursor. */
        testutil_check(incr_cur->close(incr_cur));

        /* Close file descriptors if they're open. */
        if (rfd != -1) {
            testutil_check(close(rfd));
            testutil_check(close(wfd));
            // Reset file descriptors to default value after closing.
            rfd = wfd = -1;
        }
        /*
         * For each file, we want to copy the file into each of the later incremental directories so
         * that they start out at the same for the next incremental round. We then check each
         * incremental directory along the way.
         */
        for (j = i; j <= MAX_ITERATIONS; j++) {
            testutil_check(__wt_snprintf(h, sizeof(h), "%s.%d", home_incr, j));
            if (strncmp(filename, WTLOG, WTLOGLEN) == 0)
                testutil_check(__wt_snprintf(buf, sizeof(buf), "cp %s/%s/%s %s/%s/%s", home,
                  logpath, filename, h, logpath, filename));
            else
                testutil_check(
                  __wt_snprintf(buf, sizeof(buf), "cp %s/%s %s/%s", home, filename, h, filename));
            testutil_check(system(buf));
        }
    }
    scan_end_check(ret == WT_NOTFOUND);

    /* Done processing all files. Close backup cursor. */
    testutil_check(backup_cur->close(backup_cur));
    finalize_files(flist, count);
    free(tmp);
}

/*
 * This function will add records to the table, do intermittent checkpoints, take
 * incremental/full backups and validate the backups.
 */ 
static void
add_data_validate_backups(WT_SESSION *session)
{
    int i, j;
    printf("Adding initial data\n");
    add_work(session, 0, 0, uri, uri2);

    printf("Taking initial backup\n");
    take_full_backup(session, 0);

    testutil_check(session->checkpoint(session, NULL));

    for (i = 1; i < ITERATIONS_MULTIPLIER; i++) {
        printf("Iteration %d: adding data\n", i);
        /* For each iteration we may add work and checkpoint multiple times. */
        for (j = 0; j < i; j++) {
            add_work(session, i, j, uri, uri2);
            testutil_check(session->checkpoint(session, NULL));
        }

        /*
         * The full backup here is only needed for testing and comparison purposes. A normal
         * incremental backup procedure would not include this.
         */
        printf("Iteration %d: taking full backup\n", i);
        take_full_backup(session, i);
        /*
         * Taking the incremental backup also calls truncate to archive the log files, if the copies
         * were successful. See that function for details on that call.
         */
        printf("Iteration %d: taking incremental backup\n", i);
        take_incr_backup(session, i);

        printf("Iteration %d: dumping and comparing data\n", i);
        testutil_check(compare_backups(i, uri));
    }
}

/*
 * Remove all the records, take backup and validate the backup.
 */
static void
remove_all_records_validate(WT_SESSION *session)
{
    int i, j;

    remove_work(session, 0, 0);
    for (i = 1; i < ITERATIONS_MULTIPLIER; i++) {
        printf("Iteration %d: Removing data\n", i);
        for (j = 0; j < i; j++) {
            remove_work(session, i, j);
        }
    }
    take_full_backup(session, i);

    take_incr_backup(session, i);

    printf("Dumping and comparing data\n");
    testutil_check(compare_backups(i, uri));
}

/*
 * This function will drop the existing table uri2 (table:extra) that is part of the backups 
 * and create new table (table:new_table), take incremental backup and validate.
 */ 
static void
drop_old_add_new_table(WT_SESSION *session)
{
    //WT_SESSION *session;
    int ret, i, j;
    char buf[1024];

    testutil_check(session->create(session, uri3, "key_format=S,value_format=S"));
    testutil_check(session->drop(session, uri2, "force"));

    new_object = true;

    for (i = ITERATIONS_MULTIPLIER + 1; i < ITERATIONS_MULTIPLIER * 2; i++) {
        printf("Iteration %d: adding data\n", i);
        for (j = ITERATIONS_MULTIPLIER; j < i; j++) {
            add_work(session, i, j, uri, uri3);
            testutil_check(session->checkpoint(session, NULL));
        }

        printf("Iteration %d: taking incremental backup\n", i);
        take_incr_backup(session, i);

        // Check if the dropped object exists in the incremental folder.
        testutil_check(__wt_snprintf(
          buf, sizeof(buf), "%s -R -h %s.%d list | grep %s", WT_UTIL, home_incr, i, uri2));
        ret = system(buf);
        testutil_assertfmt(ret != 0, "%s dropped, but file exists in %s.%d\n", uri2, home_incr, i);

        // Clean up
        testutil_check(__wt_snprintf(buf, sizeof(buf), "rm -rf %s.%d", home_incr, i));
        testutil_check(system(buf));
    }
    new_object = false;
}

/*
 * This function will create previously dropped table uri2 (table:extra)
 * and add different content to it.
 */
static void
//create_dropped_table_add_new_content(WT_CONNECTION *wt_conn)
create_dropped_table_add_new_content(WT_SESSION *session)
{
    //WT_SESSION *session;
    int i, j;

    testutil_check(session->create(session, uri2, "key_format=S,value_format=S"));

    for (i = ITERATIONS_MULTIPLIER * 2; i < ITERATIONS_MULTIPLIER * 3; i++) {
        printf("Iteration %d: adding data\n", i);
        for (j = ITERATIONS_MULTIPLIER * 2; j < i; j++) {
            add_work(session, i, j, uri, uri2);
        }

        take_full_backup(session, i);

        take_incr_backup(session, i);

        printf("Dumping and comparing data\n");
        testutil_check(compare_backups(i, uri2));
    }
}

/*
 * This function will insert bulk data in logged and not-logged table
 */
static void
insert_bulk_data(WT_SESSION *session)
{
    new_object = true;
    /*
     * Insert bulk data into logged table
     */
    testutil_check(session->create(session, uri4, "key_format=i,value_format=S"));

    add_bulk_load(session, 0, 0, uri4);
    // ID 15 is used for logged table
    take_full_backup(session, ITERATIONS_MULTIPLIER * 3);
    take_incr_backup(session, ITERATIONS_MULTIPLIER * 3);
    printf("Dumping and comparing data\n");
    testutil_check(compare_backups(ITERATIONS_MULTIPLIER * 3, uri4));

    /*
     * Insert bulk data into not logged table
     */
    testutil_check(
      session->create(session, uri5, "key_format=i,value_format=S,log=(enabled=false)"));

    add_bulk_load(session, 0, 0, uri5);
    // ID 16 is used for not logged table
    take_full_backup(session, 16);
    take_incr_backup(session, 16);
    printf("Dumping and comparing data\n");
    testutil_check(compare_backups(16, uri5));

    new_object = false;
}

int
main(int argc, char *argv[])
{
    WT_CONNECTION *wt_conn;
    WT_CURSOR *backup_cur;
    WT_SESSION *session;
    int i;
    char cmd_buf[256];

    (void)argc; /* Unused variable */
    (void)testutil_set_progname(argv);

    testutil_check(
      __wt_snprintf(cmd_buf, sizeof(cmd_buf), "rm -rf %s && mkdir -p %s/%s", home, home, logpath));
    testutil_check(system(cmd_buf));
    testutil_check(wiredtiger_open(home, NULL, CONN_CONFIG, &wt_conn));

    setup_directories();
    testutil_check(wt_conn->open_session(wt_conn, NULL, NULL, &session));
    testutil_check(session->create(session, uri, "key_format=S,value_format=S"));
    testutil_check(session->create(session, uri2, "key_format=S,value_format=S"));

    printf("*** Add data, checkpoint, take backups and validate ***\n");
    add_data_validate_backups(session);

    printf("*** Remove old records and validate ***\n");
    remove_all_records_validate(session);

    /*
     * Close and re-open the connection to drop existing table.
     */ 
    testutil_check(wt_conn->close(wt_conn, NULL));
    testutil_check(wiredtiger_open(home, NULL, CONN_CONFIG, &wt_conn));
    testutil_check(wt_conn->open_session(wt_conn, NULL, NULL, &session));

    printf("*** Drop old and add new table ***\n");
    drop_old_add_new_table(session);

    printf("*** Create previously dropped table and add new content ***\n");
    create_dropped_table_add_new_content(session);

    printf("*** Insert data into Logged and Not-Logged tables ***\n");
    insert_bulk_data(session);

    printf("Close and reopen the connection\n");
    /*
     * Close and reopen the connection to illustrate the durability of id information.
     */
    testutil_check(wt_conn->close(wt_conn, NULL));
    testutil_check(wiredtiger_open(home, NULL, CONN_CONFIG, &wt_conn));
    testutil_check(wt_conn->open_session(wt_conn, NULL, NULL, &session));
    /*
     * We should have an entry for MAX_ITERATIONS-1 and MAX_ITERATIONS-2. Use the older one.
     */
    testutil_check(__wt_snprintf(cmd_buf, sizeof(cmd_buf), "incremental=(src_id=ID%d,this_id=ID%d)",
      MAX_ITERATIONS - 2, MAX_ITERATIONS));
    testutil_check(session->open_cursor(session, "backup:", NULL, cmd_buf, &backup_cur));
    testutil_check(backup_cur->close(backup_cur));

    /*
     * Close the connection. We're done and want to run the final comparison between the incremental
     * and original.
     */
    testutil_check(wt_conn->close(wt_conn, NULL));

    printf("Final comparison: dumping and comparing data\n");
    testutil_check(compare_backups(0, uri));
    for (i = 0; i < (int)filelist_count; ++i) {
        if (last_flist[i].name == NULL)
            break;
        free((void *)last_flist[i].name);
    }
    free(last_flist);

    return (EXIT_SUCCESS);
}
