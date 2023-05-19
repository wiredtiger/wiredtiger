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
#include <dirent.h>
#include <libgen.h>
#include <unistd.h>
#endif

typedef struct {
    const char *base_path; /* The starting point of the traversal. */
    const char *rel_path;  /* The path relative to the base path. */

    bool directory; /* This is a directory. */
    /* TODO: Stat is Linux only. */
    struct stat stat;
} file_info_t;
typedef void (*file_callback_t)(const char *, const file_info_t *, void *);

/*
 * process_directory_tree --
 *     Process a directory tree recursively. Fail the test on error.
 */
static void
process_directory_tree(const char *base_path, const char *rel_path, bool must_exist,
  file_callback_t on_file, file_callback_t on_directory_enter, file_callback_t on_directory_leave,
  void *user_data)
{
#ifdef _WIN32
    /* TODO */
#else
    struct dirent *dp;
    DIR *dirp;
    WT_DECL_RET;
    char path[PATH_MAX], s[PATH_MAX];
    file_info_t info;

    /* Sanitize the base path. */
    if (base_path == NULL || base_path[0] == '\0')
        base_path = ".";

    memset(&info, 0, sizeof(info));
    info.base_path = base_path;
    info.rel_path = rel_path;

    /* Get the full path to the provided file or a directory. */
    if (rel_path == NULL || rel_path[0] == '\0')
        testutil_check(__wt_snprintf(path, sizeof(path), "%s", base_path));
    else
        testutil_check(
          __wt_snprintf(path, sizeof(path), "%s" DIR_DELIM_STR "%s", base_path, info.rel_path));

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
        process_directory_tree(
          base_path, s, must_exist, on_file, on_directory_enter, on_directory_leave, user_data);
    }

    testutil_check(closedir(dirp));

    /* Invoke the directory leave callback. */
    if (on_directory_leave != NULL)
        on_directory_leave(path, &info, user_data);
#endif
}

#define COPY_BUF_SIZE ((size_t)(256 * WT_KILOBYTE))

struct copy_data {
    const char *dest;
    bool dest_is_dir;
};

/*
 * copy_on_file --
 *     Worker for copying a file.
 */
static void
copy_on_file(const char *path, const file_info_t *info, void *user_data)
{
    struct copy_data *d;
    ssize_t n;
    int rfd, wfd;
    char *buf, *source_base_name;
    char dest_path[PATH_MAX], s[PATH_MAX];

    d = (struct copy_data *)user_data;

    /* Get path to the new file. If the relative path is NULL, it means we are copying a file. */
    if (info->rel_path == NULL) {
        if (d->dest_is_dir) {
            testutil_check(__wt_snprintf(s, sizeof(s), "%s", path));
            source_base_name = basename(s);
            testutil_check(__wt_snprintf(
              dest_path, sizeof(dest_path), "%s" DIR_DELIM_STR "%s", d->dest, source_base_name));
        } else
            testutil_check(__wt_snprintf(dest_path, sizeof(dest_path), "%s", d->dest));
    } else
        testutil_check(__wt_snprintf(
          dest_path, sizeof(dest_path), "%s" DIR_DELIM_STR "%s", d->dest, info->rel_path));

    /* Copy the file. */
    testutil_assert_errno((rfd = open(path, O_RDONLY)) > 0);
    testutil_assert_errno((wfd = open(dest_path, O_WRONLY | O_CREAT, info->stat.st_mode)) > 0);

    buf = dmalloc(COPY_BUF_SIZE);
    for (;;) {
        testutil_assert_errno((n = read(rfd, buf, COPY_BUF_SIZE)) >= 0);
        if (n == 0)
            break;
        testutil_assert_errno(write(wfd, buf, (size_t)n) == n);
    }

    testutil_check(close(rfd));
    testutil_check(close(wfd));
    free(buf);
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

    /* Otherwise we create a new directory. */
    testutil_check(__wt_snprintf(
      dest_path, sizeof(dest_path), "%s" DIR_DELIM_STR "%s", d->dest, info->rel_path));

    testutil_assert_errno(mkdir(dest_path, info->stat.st_mode) == 0);
}

/*
 * testutil_copy --
 *     Recursively copy a file or a directory tree. Fail the test on error.
 */
void
testutil_copy(const char *source, const char *dest)
{
    struct copy_data data;
    struct stat source_stat, dest_stat;
    WT_DECL_RET;
    bool dest_exists;
    bool is_dest_dir;
    bool is_source_dir;

    memset(&data, 0, sizeof(data));
    data.dest = dest;

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

    process_directory_tree(source, NULL, true, copy_on_file, copy_on_directory_enter, NULL, &data);
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
    process_directory_tree(path, NULL, true, remove_on_file, NULL, remove_on_directory_leave, NULL);
}
