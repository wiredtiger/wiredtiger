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

#define WT_UTIL "../../wt"
#define WTLOG "WiredTigerLog"
#define WTLOGLEN strlen(WTLOG)

static const char *const full_out = "./backup_block_full";
static const char *const incr_out = "./backup_block_incr";

static const char *const uri = "table:main";
static const char *const uri2 = "table:extra";
static const char *const uri3 = "table:logged_table";
static const char *const uri4 = "table:not_logged_table";

typedef struct __filelist {
    const char *name;
    bool exist;
} FILELIST;

static FILELIST *last_flist = NULL;
static size_t filelist_count = 0;
/*
 * The variable counter drives the backup iteration uniqueness and its max value is MAX_ITERATIONS.
 */
static int counter = 0;
static bool initial_backup = false;
static bool new_table = false;

#define FLIST_INIT 16

#define CONN_CONFIG "create,cache_size=100MB,log=(enabled=true,path=logpath,file_max=100K)"
#define MAX_ITERATIONS 6
#define MAX_KEYS 10000

static int
compare_backups(const char *t_uri)
{
    int ret;
    char buf[1024], msg[32];
    printf("Iteration %d, Dumping and comparing %s\n", counter, t_uri);
    /*
     * We run 'wt dump' on both the full backup directory and the
     * incremental backup directory for this iteration.  Since running
     * 'wt' runs recovery and makes both directories "live", we need
     * a new directory for each iteration.
     *
     * If counter == 0, we're comparing against the main, original directory
     * with the final incremental directory.
     */
    if (counter == 0)
        testutil_check(__wt_snprintf(buf, sizeof(buf), "%s -R -h %s dump %s > %s.%d", WT_UTIL, home,
          t_uri, full_out, counter));
    else
        testutil_check(__wt_snprintf(buf, sizeof(buf), "%s -R -h %s.%d dump %s > %s.%d", WT_UTIL,
          home_full, counter, t_uri, full_out, counter));
    testutil_check(system(buf));
    /*
     * Now run dump on the incremental directory.
     */
    testutil_check(__wt_snprintf(buf, sizeof(buf), "%s -R -h %s.%d dump %s > %s.%d", WT_UTIL,
      home_incr, counter, t_uri, incr_out, counter));
    testutil_check(system(buf));

    /*
     * Compare the files.
     */
    testutil_check(
      __wt_snprintf(buf, sizeof(buf), "cmp %s.%d %s.%d", full_out, counter, incr_out, counter));
    ret = system(buf);
    if (counter == 0)
        testutil_check(__wt_snprintf(msg, sizeof(msg), "%s", "MAIN"));
    else
        testutil_check(__wt_snprintf(msg, sizeof(msg), "%d", counter));
    printf("Iteration %s: Tables %s.%d and %s.%d %s\n", msg, full_out, counter, incr_out, counter,
      ret == 0 ? "identical" : "differ");
    if (ret != 0)
        exit(1);

    /*
     * If they compare successfully, clean up.
     */
    if (counter != 0) {
        testutil_check(__wt_snprintf(buf, sizeof(buf), "rm -rf %s.%d %s.%d %s.%d %s.%d", home_full,
          counter, home_incr, counter, full_out, counter, incr_out, counter));
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
add_work(WT_SESSION *session, const char *t_uri, bool bulk_load)
{
    WT_CURSOR *cursor;
    int i, key;
    char v[64];
    /*
     * Open the cursor with bulk option if bulk_load is true.
     */
    if (bulk_load) {
        testutil_check(session->open_cursor(session, t_uri, NULL, "bulk", &cursor));
    } else {
        testutil_check(session->open_cursor(session, t_uri, NULL, NULL, &cursor));
    }

    for (i = 0; i < MAX_KEYS; i++) {
        key = i + (counter * MAX_KEYS);
        testutil_check(__wt_snprintf(v, sizeof(v), "value.%d", key));
        cursor->set_key(cursor, key);
        cursor->set_value(cursor, v);
        testutil_check(cursor->insert(cursor));
    }
    testutil_check(cursor->close(cursor));
    /*
     * Increase the counter so that later calls insert unique items.
     */
    if (!initial_backup)
        ++counter;
}

static void
remove_work(WT_SESSION *session)
{
    WT_CURSOR *cursor;
    int i, j, key;
    char v[64];

    testutil_check(session->open_cursor(session, uri, NULL, NULL, &cursor));

    /*
     * We run the outer loop until counter value to make sure we remove all the inserted records
     * from the main table.
     */
    for (i = 0; i < counter; ++i) {
        for (j = i; j < MAX_KEYS; ++j) {
            key = j + (i * MAX_KEYS);
            testutil_check(__wt_snprintf(v, sizeof(v), "value.%d", key));
            cursor->set_key(cursor, key);
            testutil_check(cursor->remove(cursor));
        }
    }
    testutil_check(cursor->close(cursor));
    /*
     * Increase the counter so that upcoming backups uses the unique value.
     */
    ++counter;
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
take_full_backup(WT_SESSION *session)
{
    FILELIST *flist;
    WT_CURSOR *cursor;
    size_t alloc, count;
    int j, ret;
    char buf[1024], f[256], h[256];
    const char *filename, *hdir;

    printf("Full Backup iteration : %d\n", counter);
    /*
     * First time through we take a full backup into the incremental directories. Otherwise only
     * into the appropriate full directory.
     */
    if (counter != 0) {
        testutil_check(__wt_snprintf(h, sizeof(h), "%s.%d", home_full, counter));
        hdir = h;
    } else
        hdir = home_incr;
    if (initial_backup) {
        testutil_check(__wt_snprintf(
          buf, sizeof(buf), "incremental=(granularity=1M,enabled=true,this_id=ID%d)", 0));
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

        if (counter == 0)
            /*
             * Take a full backup into each incremental directory.
             */
            for (j = 0; j < MAX_ITERATIONS; j++) {
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
take_incr_backup(WT_SESSION *session)
{
    FILELIST *flist;
    WT_CURSOR *backup_cur, *incr_cur;
    size_t alloc, count, rdsize, tmp_sz;
    uint64_t offset, size, type;
    int j, ret, rfd, wfd;
    char buf[1024], h[256], *tmp;
    const char *filename;
    bool first;

    printf("Increment Backup iteration : %d\n", counter);
    tmp = NULL;
    tmp_sz = 0;
    /* Open the backup data source for incremental backup. */
    testutil_check(__wt_snprintf(
      buf, sizeof(buf), "incremental=(src_id=ID%d,this_id=ID%d)", counter - 1, counter));
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

        while ((ret = incr_cur->next(incr_cur)) == 0) {
            testutil_check(incr_cur->get_key(incr_cur, &offset, &size, &type));
            scan_end_check(type == WT_BACKUP_FILE || type == WT_BACKUP_RANGE);

            /*
             * The condition (new_table == false) is to skip copying blocks for new created objects.
             * The code below tries to open a write file descriptor on the newly created files to
             * copy the blocks and it fails because the newly created file does not exist in the
             * incremental directory
             */
            if ((type == WT_BACKUP_RANGE) && (new_table == false)) {
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
                    testutil_check(__wt_snprintf(h, sizeof(h), "%s.%d", home_incr, counter));
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
        for (j = counter; j <= MAX_ITERATIONS; j++) {
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
 * This function will add records to the table (table:main), take incremental/full backups and
 * validate the backups.
 */
static void
add_data_validate_backups(WT_SESSION *session)
{
    printf("Adding initial data\n");
    /*
     * We set initial_backup = true to take a full backup into the incremental directories.
     */
    initial_backup = true;
    add_work(session, uri, false);
    printf("Taking initial backup\n");
    take_full_backup(session);
    initial_backup = false;
    testutil_check(session->checkpoint(session, NULL));

    add_work(session, uri, false);
    take_full_backup(session);
    take_incr_backup(session);
    testutil_check(compare_backups(uri));
}

/*
 * This function will remove all the records from table (table:main), take backup and validate the
 * backup.
 */
static void
remove_all_records_validate(WT_SESSION *session)
{
    remove_work(session);
    take_full_backup(session);
    take_incr_backup(session);
    testutil_check(compare_backups(uri));
}

/*
 * This function will drop the existing table uri (table:main) that is part of the backups and
 * create new table uri2 (table:extra), take incremental backup and validate.
 */
static void
drop_old_add_new_table(WT_SESSION *session)
{
    int ret;
    char buf[1024];

    testutil_check(session->create(session, uri2, "key_format=i,value_format=S"));
    testutil_check(session->drop(session, uri, "force"));

    /*
     * Set this variable to true so that function take_incr_backup copies full file instead of
     * writing blocks to the new created objects
     */
    new_table = true;
    add_work(session, uri2, false);
    take_incr_backup(session);

    /*
     * Assert with message if the dropped table (table:main) exists in the incremental folder.
     */
    testutil_check(__wt_snprintf(
      buf, sizeof(buf), "%s -R -h %s.%d list | grep %s", WT_UTIL, home_incr, counter, uri));
    ret = system(buf);
    testutil_assertfmt(
      ret != 0, "%s dropped, but table exists in %s.%d\n", uri, home_incr, counter);

    /*
     * Clean up the folder.
     */
    testutil_check(__wt_snprintf(buf, sizeof(buf), "rm -rf %s.%d", home_incr, counter));
    testutil_check(system(buf));

    new_table = false;
}

/*
 * This function will create previously dropped table uri (table:main) and add different content to
 * it, take backups and validate the backups.
 */
static void
create_dropped_table_add_new_content(WT_SESSION *session)
{
    testutil_check(session->create(session, uri, "key_format=i,value_format=S"));
    add_work(session, uri, false);
    take_full_backup(session);
    take_incr_backup(session);
    testutil_check(compare_backups(uri));
}

/*
 * This function will insert bulk data in logged and not-logged table, take backups and validate the
 * backups.
 */
static void
insert_bulk_data(WT_SESSION *session)
{
    /*
     * Set this variable to true so that function take_incr_backup copies full file instead of
     * writing blocks to the new created objects
     */
    new_table = true;
    /*
     * Insert bulk data into logged table.
     */
    testutil_check(session->create(session, uri3, "key_format=i,value_format=S"));
    add_work(session, uri3, true);
    take_full_backup(session);
    take_incr_backup(session);
    testutil_check(compare_backups(uri3));

    /*
     * Insert bulk data into not logged table.
     */
    testutil_check(
      session->create(session, uri4, "key_format=i,value_format=S,log=(enabled=false)"));
    add_work(session, uri4, true);
    take_full_backup(session);
    take_incr_backup(session);
    testutil_check(compare_backups(uri4));
    new_table = false;
}

int
main(int argc, char *argv[])
{
    WT_CONNECTION *wt_conn;
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
    testutil_check(session->create(session, uri, "key_format=i,value_format=S"));
    testutil_check(session->create(session, uri2, "key_format=i,value_format=S"));

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

    for (i = 0; i < (int)filelist_count; ++i) {
        if (last_flist[i].name == NULL)
            break;
        free((void *)last_flist[i].name);
    }
    free(last_flist);

    return (EXIT_SUCCESS);
}
