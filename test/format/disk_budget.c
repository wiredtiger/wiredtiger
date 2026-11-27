/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "format.h"

/* Approximate per-row cell and page overhead beyond the key and value sizes. */
#define ROW_OVERHEAD_BYTES 128

/* Load at least a few rows, even if the budget is tiny; a run needs a workload. */
#define MIN_LOAD_ROWS 10

#ifndef _WIN32
#include <dirent.h>

/*
 * __disk_size --
 *     Measure the space allocated under a directory.
 */
static uint64_t
__disk_size(const char *path)
{
    struct dirent *entry;
    struct stat sb;
    DIR *dir;
    uint64_t bytes;
    char name[PATH_MAX];

    if ((dir = opendir(path)) == NULL) {
        if (errno == ENOENT)
            return (0);
        testutil_die(errno, "opendir: %s", path);
    }

    bytes = 0;
    for (;;) {
        errno = 0;
        if ((entry = readdir(dir)) == NULL) {
            if (errno != 0)
                testutil_die(errno, "readdir: %s", path);
            break;
        }
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
            continue;

        testutil_snprintf(name, sizeof(name), "%s/%s", path, entry->d_name);
        if (lstat(name, &sb) != 0) {
            /* Logs and backup files can disappear during the walk. */
            if (errno == ENOENT)
                continue;
            testutil_die(errno, "cannot stat %s", name);
        }
        bytes += (uint64_t)sb.st_blocks * 512;
        if (S_ISDIR(sb.st_mode))
            bytes += __disk_size(name);
    }
    testutil_assert_errno(closedir(dir) == 0);
    return (bytes);
}
#endif

/*
 * disk_budget_check --
 *     Return true if the completed phase used up the run's disk budget.
 */
bool
disk_budget_check(const char *phase)
{
#ifdef _WIN32
    WT_UNUSED(phase);
    return (false);
#else
    uint64_t budget, used;

    if (GV(RUNS_DISK_BUDGET_MB) == 0)
        return (false);

    budget = MEGABYTE((uint64_t)GV(RUNS_DISK_BUDGET_MB));
    used = __disk_size(g.home);
    if (used < budget)
        return (false);

    fprintf(stderr,
      "%s: disk budget reached after %s: used=%" PRIu64 ", budget=%" PRIu64 " bytes\n", progname,
      phase, used, budget);
    return (true);
#endif
}

/*
 * disk_budget_limit_rows --
 *     Estimate a starting row count before loading any of a set of mirrored tables.
 */
void
disk_budget_limit_rows(void)
{
    TABLE *source, *table;
    uint64_t bytes_per_row, load_budget, max_rows;
    u_int first, i;

    if (GV(RUNS_DISK_BUDGET_MB) == 0)
        return;

    first = ntables == 0 ? 0 : 1;
    bytes_per_row = 0;
    for (i = first; i <= ntables; ++i) {
        table = tables[i];
        source = table->mirror ? g.base_mirror : table;
        bytes_per_row += (uint64_t)NTV(source, BTREE_VALUE_MAX) + ROW_OVERHEAD_BYTES;
        if (table->type == ROW)
            bytes_per_row += (uint64_t)TV(BTREE_KEY_MAX) + TV(BTREE_PREFIX_LEN);
    }

    /*
     * Load at most half the budget, and a quarter when backups are enabled: a backup run may need
     * both a backup and a verification copy of the loaded tables.
     */
    load_budget = MEGABYTE((uint64_t)GV(RUNS_DISK_BUDGET_MB)) / (GV(BACKUP) ? 4 : 2);

    /* Logging and bulk tracing write significant extra data, leave more headroom. */
    if (GV(LOGGING) || FLD_ISSET(g.trace_flags, TRACE_BULK))
        load_budget /= 2;
    max_rows = WT_MAX(MIN_LOAD_ROWS, load_budget / bytes_per_row);

    for (i = first; i <= ntables; ++i) {
        table = tables[i];
        if (TV(RUNS_ROWS) > max_rows) {
            fprintf(stderr,
              "%s: disk budget limits table %u bulk load from %" PRIu32 " to %" PRIu64 " rows\n",
              progname, table->id, TV(RUNS_ROWS), max_rows);
            TV(RUNS_ROWS) = (uint32_t)max_rows;
        }
    }
}
