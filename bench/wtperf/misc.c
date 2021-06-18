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

#include "wtperf.h"

#define WT_BACKUP_COPY_SIZE (128 * 1024)

/* Setup the logging output mechanism. */
int
setup_log_file(WTPERF *wtperf)
{
    CONFIG_OPTS *opts;
    size_t len;
    int ret;
    char *fname;

    opts = wtperf->opts;
    ret = 0;

    if (opts->verbose < 1)
        return (0);

    len = strlen(wtperf->monitor_dir) + strlen(opts->table_name) + strlen(".stat") + 2;
    fname = dmalloc(len);
    testutil_check(__wt_snprintf(fname, len, "%s/%s.stat", wtperf->monitor_dir, opts->table_name));
    if ((wtperf->logf = fopen(fname, "w")) == NULL) {
        ret = errno;
        fprintf(stderr, "%s: %s\n", fname, strerror(ret));
    }
    free(fname);
    if (wtperf->logf == NULL)
        return (ret);

    /* Use line buffering for the log file. */
    __wt_stream_set_line_buffer(wtperf->logf);
    return (0);
}

/*
 * Log printf - output a log message.
 */
void
lprintf(const WTPERF *wtperf, int err, uint32_t level, const char *fmt, ...)
{
    CONFIG_OPTS *opts;
    va_list ap;

    opts = wtperf->opts;

    if (err == 0 && level <= opts->verbose) {
        va_start(ap, fmt);
        vfprintf(wtperf->logf, fmt, ap);
        va_end(ap);
        fprintf(wtperf->logf, "\n");

        if (level < opts->verbose) {
            va_start(ap, fmt);
            vprintf(fmt, ap);
            va_end(ap);
            printf("\n");
        }
    }
    if (err == 0)
        return;

    /* We are dealing with an error. */
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
    fprintf(stderr, " Error: %s\n", wiredtiger_strerror(err));
    if (wtperf->logf != NULL) {
        va_start(ap, fmt);
        vfprintf(wtperf->logf, fmt, ap);
        va_end(ap);
        fprintf(wtperf->logf, " Error: %s\n", wiredtiger_strerror(err));
    }

    /* Never attempt to continue if we got a panic from WiredTiger. */
    if (err == WT_PANIC)
        abort();
}

/*
 * backup_read --
 *     Read in a file, used mainly to measure the impact of backup on a single machine. Backup is
 *     usually copied across different machines, therefore the write portion doesn't affect the
 *     machine backup is performing on.
 */
int
backup_read(WT_SESSION *wt_session, const WTPERF *wtperf, const char *from)
{
    WT_DECL_RET;
    WT_FH *fh;
    WT_SESSION_IMPL *session;
    wt_off_t n, offset, size;
    char *buf;

    buf = NULL;
    fh = NULL;
    session = (WT_SESSION_IMPL *)wt_session;

    /* Open the file handle. */
    if ((ret = __wt_open(session, from, WT_FS_OPEN_FILE_TYPE_REGULAR, 0, &fh)) != 0) {
        lprintf(wtperf, ret, 0, "Open file handle %s for backup failed", from);
        goto err;
    }
    buf = dmalloc(WT_BACKUP_COPY_SIZE);

    /* Get the file's size, then read the bytes. */
    if ((ret = __wt_filesize(session, fh, &size)) != 0) {
        lprintf(wtperf, ret, 0, "Grab file size for %s failed", from);
        goto err;
    }
    for (offset = 0; size > 0; size -= n, offset += n) {
        n = WT_MIN(size, WT_BACKUP_COPY_SIZE);
        if ((ret = __wt_read(session, fh, offset, (size_t)n, buf)) != 0) {
            lprintf(wtperf, ret, 0, "Read file %s failed", from);
            goto err;
        }
    }

err:
    WT_TRET(__wt_close(session, &fh));

    free(buf);
    return (ret);
}