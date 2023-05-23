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
#include "test_util.h"

#ifdef _WIN32
/* TODO */
#else
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <libgen.h>
#include <unistd.h>
#include <utime.h>
#endif

typedef struct {
    const char *base_name;  /* The base name (the file name, without the path). */
    const char *start_path; /* The starting point of the traversal. */
    const char *rel_path;   /* The path relative to the start path. */

    bool directory;         /* This is a directory. */
    int depth;              /* The depth we are at (0 = the source). */

    /* TODO: Stat is Linux only. */
    struct stat stat;
} file_info_t;
typedef void (*file_callback_t)(const char *, const file_info_t *, void *);

/*
 * process_directory_tree --
 *     Process a directory tree recursively. Fail the test on error.
 */
static void
process_directory_tree(const char *start_path, const char *rel_path, int depth, bool must_exist,
  file_callback_t on_file, file_callback_t on_directory_enter, file_callback_t on_directory_leave,
  void *user_data)
{
#ifdef _WIN32
    /* TODO */
#else
    struct dirent *dp;
    DIR *dirp;
    WT_DECL_RET;
    char buf[PATH_MAX], path[PATH_MAX], s[PATH_MAX];
    file_info_t info;

    /* Sanitize the base path. */
    if (start_path == NULL || start_path[0] == '\0')
        start_path = ".";

    memset(&info, 0, sizeof(info));
    info.depth = depth;
    info.rel_path = rel_path;
    info.start_path = start_path;

    /* Get the full path to the provided file or a directory. */
    if (rel_path == NULL || rel_path[0] == '\0')
        testutil_check(__wt_snprintf(path, sizeof(path), "%s", start_path));
    else
        testutil_check(
          __wt_snprintf(path, sizeof(path), "%s" DIR_DELIM_STR "%s", start_path, info.rel_path));

    /* Get just the base name. */
    testutil_check(__wt_snprintf(buf, sizeof(buf), "%s", path));
    info.base_name = basename(buf);

    /* Check if the provided path points to a file. */
    ret = stat(path, &info.stat);
    if (ret != 0 && (must_exist || errno != ENOENT))
        testutil_assert_errno(ret);

    if (!S_ISDIR(info.stat.st_mode)) {
        if (ret == 0 && on_file != NULL)
            on_file(path, &info, user_data);
        return;
    }

    /* It is a directory, so process it recursively. */
    dirp = opendir(path);
    testutil_assert_errno(dirp != NULL);
    info.directory = true;

    /* Invoke the directory enter callback. */
    if (on_directory_enter != NULL)
        on_directory_enter(path, &info, user_data);

    while ((dp = readdir(dirp)) != NULL) {
        testutil_assert(dp->d_name[0] != '\0');

        /* Skip . and .. */
        if (strcmp(dp->d_name, ".") == 0 || strcmp(dp->d_name, "..") == 0)
            continue;

        if (rel_path == NULL || rel_path[0] == '\0')
            testutil_check(__wt_snprintf(s, sizeof(s), "%s", dp->d_name));
        else
            testutil_check(
              __wt_snprintf(s, sizeof(s), "%s" DIR_DELIM_STR "%s", rel_path, dp->d_name));
        process_directory_tree(start_path, s, depth + 1, must_exist, on_file, on_directory_enter,
          on_directory_leave, user_data);
    }

    testutil_check(closedir(dirp));

    /* Invoke the directory leave callback. */
    if (on_directory_leave != NULL)
        on_directory_leave(path, &info, user_data);
#endif
}

#define COPY_BUF_SIZE ((size_t)(256 * WT_KILOBYTE))

struct copy_data {
    const WT_FILE_COPY_OPTS *opts;
    const char *dest;
    bool dest_is_dir;
    int link_depth;
};

/*
 * copy_on_file --
 *     Worker for copying a file.
 */
static void
copy_on_file(const char *path, const file_info_t *info, void *user_data)
{
    struct copy_data *d;
    struct timeval times[2];
    size_t n;
    FILE *rf, *wf;
    char *buf;
    char dest_path[PATH_MAX];

    d = (struct copy_data *)user_data;

    /* Get path to the new file. If the relative path is NULL, it means we are copying a file. */
    if (info->rel_path == NULL) {
        if (d->dest_is_dir) {
            testutil_check(__wt_snprintf(
              dest_path, sizeof(dest_path), "%s" DIR_DELIM_STR "%s", d->dest, info->base_name));
        } else
            testutil_check(__wt_snprintf(dest_path, sizeof(dest_path), "%s", d->dest));
    } else
        testutil_check(__wt_snprintf(
          dest_path, sizeof(dest_path), "%s" DIR_DELIM_STR "%s", d->dest, info->rel_path));

    /* Check if we need to switch to using links. */
    if (d->opts->link && d->link_depth < 0)
        if (strncmp(d->opts->link_if_prefix, info->base_name, strlen(d->opts->link_if_prefix)) == 0)
            d->link_depth = info->depth;

#ifndef _WIN32
    /* Check if we are actually creating a hard link instead. */
    if (d->link_depth >= 0 && info->depth >= d->link_depth) {
        testutil_assert_errno(link(path, dest_path) == 0);
        return;
    }
#endif

    /* Copy the file. */
    testutil_assert_errno((rf = fopen(path, "rb")) != NULL);
    testutil_assert_errno((wf = fopen(dest_path, "wb")) != NULL);

#ifndef _WIN32
    testutil_assert_errno(fchmod(fileno(wf), info->stat.st_mode) == 0);
#endif

    buf = dmalloc(COPY_BUF_SIZE);
    for (;;) {
        n = fread(buf, 1, COPY_BUF_SIZE, rf);
        testutil_assert_errno(ferror(rf) == 0);
        if (n == 0)
            break;
        testutil_assert_errno(fwrite(buf, 1, n, wf) == n);
    }

    testutil_check(fclose(rf));
    testutil_check(fclose(wf));
    free(buf);

    /* Change the timestamps. */
    if (d->opts->preserve) {
        times[0].tv_sec = info->stat.st_atim.tv_sec;
        times[0].tv_usec = info->stat.st_atim.tv_nsec / 1000;
        times[1].tv_sec = info->stat.st_mtim.tv_sec;
        times[1].tv_usec = info->stat.st_mtim.tv_nsec / 1000;
        testutil_assert_errno(utimes(dest_path, times) == 0);
    }
}

/*
 * copy_on_directory_enter --
 *     Worker for copying a directory.
 */
static void
copy_on_directory_enter(const char *path, const file_info_t *info, void *user_data)
{
    struct copy_data *d;
    char dest_path[PATH_MAX];

    WT_UNUSED(path);
    d = (struct copy_data *)user_data;

    /* No need to do anything for the top-level directory. This is handled elsewhere. */
    if (info->rel_path == NULL || strcmp(info->rel_path, ".") == 0)
        return;

    /* Check if we need to switch to using links. */
    if (d->opts->link && d->link_depth < 0)
        if (strncmp(d->opts->link_if_prefix, info->base_name, strlen(d->opts->link_if_prefix)) == 0)
            d->link_depth = info->depth;

    /* Otherwise we create a new directory. */
    testutil_check(__wt_snprintf(
      dest_path, sizeof(dest_path), "%s" DIR_DELIM_STR "%s", d->dest, info->rel_path));

    testutil_assert_errno(mkdir(dest_path, info->stat.st_mode) == 0);
}

/*
 * copy_on_directory_leave --
 *     Worker for copying a directory.
 */
static void
copy_on_directory_leave(const char *path, const file_info_t *info, void *user_data)
{
    struct copy_data *d;
    struct timeval times[2];
    char dest_path[PATH_MAX];

    WT_UNUSED(path);
    d = (struct copy_data *)user_data;

    if (info->depth <= d->link_depth)
        d->link_depth = -1;

    /* Change the timestamps. */
    if (d->opts->preserve) {
        testutil_check(__wt_snprintf(dest_path, sizeof(dest_path), "%s" DIR_DELIM_STR "%s", d->dest,
          info->rel_path == NULL ? "" : info->rel_path));
        times[0].tv_sec = info->stat.st_atim.tv_sec;
        times[0].tv_usec = info->stat.st_atim.tv_nsec / 1000;
        times[1].tv_sec = info->stat.st_mtim.tv_sec;
        times[1].tv_usec = info->stat.st_mtim.tv_nsec / 1000;
        testutil_assert_errno(utimes(dest_path, times) == 0);
    }
}

/*
 * testutil_copy --
 *     Recursively copy a file or a directory tree. Fail the test on error.
 */
void
testutil_copy(const char *source, const char *dest)
{
    testutil_copy_ext(source, dest, NULL);
}

/*
 * testutil_copy_ext --
 *     Recursively copy a file or a directory tree. Fail the test on error. With extra options.
 */
void
testutil_copy_ext(const char *source, const char *dest, const WT_FILE_COPY_OPTS *opts)
{
    struct copy_data data;
    struct stat source_stat, dest_stat;
    WT_FILE_COPY_OPTS default_opts;
    WT_DECL_RET;
    bool dest_exists;
    bool is_dest_dir;
    bool is_source_dir;

    if (opts == NULL) {
        memset(&default_opts, 0, sizeof(default_opts));
        opts = &default_opts;
    }

    memset(&data, 0, sizeof(data));
    data.dest = dest;
    data.link_depth = opts->link && opts->link_if_prefix == NULL ? 0 : -1;
    data.opts = opts;

    /* Check the source. */
    testutil_assertfmt((ret = stat(source, &source_stat)) == 0, "Failed to stat \"%s\": %s", source,
      strerror(errno));
    is_source_dir = S_ISDIR(source_stat.st_mode);

    /* Check the destination. */
    ret = stat(dest, &dest_stat);
    testutil_assert_errno(ret == 0 || errno == ENOENT);
    dest_exists = ret == 0;
    is_dest_dir = dest_exists ? S_ISDIR(dest_stat.st_mode) : false;
    data.dest_is_dir = is_dest_dir;

    /* If we are copying a directory, make sure that we are not copying over a file. */
    testutil_assert(!(is_source_dir && dest_exists && !is_dest_dir));

    /* If we are copying a directory to another directory that doesn't exist, create it. */
    if (is_source_dir && !dest_exists)
        testutil_assert_errno(mkdir(dest, source_stat.st_mode) == 0);

    process_directory_tree(
      source, NULL, 0, true, copy_on_file, copy_on_directory_enter, copy_on_directory_leave, &data);
}

/*
 * testutil_mkdir --
 *     Create a directory if it does not exist. Fail the test on error.
 */
void
testutil_mkdir(const char *path)
{
    WT_DECL_RET;

    ret = mkdir(path, 0755);
    testutil_assert_errno(ret == 0 || errno == ENOENT);
}

/*
 * remove_on_file --
 *     Worker for removing a file.
 */
static void
remove_on_file(const char *path, const file_info_t *info, void *user_data)
{
    WT_DECL_RET;

    WT_UNUSED(info);
    WT_UNUSED(user_data);

    ret = unlink(path);
    testutil_assert_errno(ret == 0 || errno == ENOENT);
}

/*
 * remove_on_directory_leave --
 *     Worker for removing a directory.
 */
static void
remove_on_directory_leave(const char *path, const file_info_t *info, void *user_data)
{
    WT_DECL_RET;

    WT_UNUSED(info);
    WT_UNUSED(user_data);

    ret = rmdir(path);
    testutil_assert_errno(ret == 0 || errno == ENOENT);
}

/*
 * testutil_remove --
 *     Recursively remove a file or a directory tree. Fail the test on error.
 */
void
testutil_remove(const char *path)
{
    process_directory_tree(
      path, NULL, 0, true, remove_on_file, NULL, remove_on_directory_leave, NULL);
}
