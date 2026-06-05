/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "util.h"

/*
 * usage --
 *     Display a usage message for the stat command.
 */
static int
usage(void)
{
    static const char *options[] = {"-f", "include only \"fast\" statistics in the output", "-?",
      "show this message", NULL, NULL};

    util_usage("stat [-f] [uri]", "options:", options);
    return (1);
}

/*
 * util_stat --
 *     The stat command.
 */
int
util_stat(WT_SESSION *session, int argc, char *argv[])
{
    WT_CURSOR *cursor;
    WT_DECL_RET;
    size_t urilen;
    int ch;
    char *objname, *uri;
    const char *config, *desc, *pval;
    bool objname_free;

    objname_free = false;
    objname = uri = NULL;
    config = NULL;
    while ((ch = __wt_getopt(progname, argc, argv, "af?")) != EOF)
        switch (ch) {
        case 'a':
            /*
             * Historically, the -a option meant include all of the statistics; because we are
             * opening the database with statistics=(all), that is now the default, allow the option
             * for compatibility.
             */
            config = NULL;
            break;
        case 'f':
            config = "statistics=(fast)";
            break;
        case '?':
            usage();
            return (0);
        default:
            return (usage());
        }
    argc -= __wt_optind;
    argv += __wt_optind;

    /*
     * If there are no arguments, the statistics cursor operates on the connection, otherwise, the
     * optional remaining argument is a file.
     */
    switch (argc) {
    case 0:
        objname = (char *)"";
        break;
    case 1:
        if ((objname = util_uri(session, *argv, "table")) == NULL)
            return (1);
        objname_free = true;
        break;
    default:
        return (usage());
    }

    urilen = strlen("statistics:") + strlen(objname) + 1;
    if ((uri = util_calloc(urilen, 1)) == NULL) {
        fprintf(stderr, "%s: %s\n", progname, strerror(errno));
        goto err;
    }
    if ((ret = __wt_snprintf(uri, urilen, "statistics:%s", objname)) != 0) {
        fprintf(stderr, "%s: %s\n", progname, strerror(ret));
        goto err;
    }

    /*
     * The dhandle-open path unconditionally clears the WT_SESSION_QUIET_CORRUPT_FILE flag on exit.
     * Pre-loading the dhandle (caches the dhandle), then setting the flag before opening the
     * statistics cursor reuses the cached dhandle and skips the scoped clear.
     *
     * Only covers leaf-page corruption. If the pre-load fails because the root or other
     * dhandle-open block is corrupt, the dhandle is not cached, the subsequent statistics cursor
     * open enters scoped-clear path, the flag is wiped, and the walk panics.
     */
    if (quiet_corrupt && objname_free) {
        WT_CURSOR *prewarm;
        if (session->open_cursor(session, objname, NULL, NULL, &prewarm) == 0)
            (void)prewarm->close(prewarm);
        F_SET((WT_SESSION_IMPL *)session, WT_SESSION_QUIET_CORRUPT_FILE);
    }

    /*
     * Tack read_corrupt=true onto whatever stats config was selected, so the stat cursor's tree
     * walk opts into corruption-skip when -q is in effect.
     */
    if (quiet_corrupt) {
        if (config == NULL)
            config = "read_corrupt=true";
        else if (strcmp(config, "statistics=(fast)") == 0)
            config = "statistics=(fast),read_corrupt=true";
    }

    /*
     * Corrupt reads are caught here. Unless fast mode is enabled, the cursor walks the entire btree
     * including the leaf pages, which could be corrupt.
     */
    if ((ret = session->open_cursor(session, uri, NULL, config, &cursor)) != 0) {
        fprintf(stderr, "%s: cursor open(%s) failed: %s\n", progname, uri,
          session->strerror(session, ret));
        goto err;
    }

    /*
     * List the statistics. In quiet-corrupt mode a cursor error mid-iteration is reported and exits
     * gracefully so any partial output is flushed.
     */
    while ((ret = cursor->next(cursor)) == 0 &&
      (ret = cursor->get_value(cursor, &desc, &pval, NULL)) == 0)
        if (printf("%s=%s\n", desc, pval) < 0) {
            (void)util_err(session, errno, "printf");
            goto err;
        }
    if (ret == WT_NOTFOUND)
        ret = 0;

    if (ret != 0) {
        fprintf(stderr, "%s: cursor get(%s) failed: %s\n", progname, objname,
          session->strerror(session, ret));
        if (!quiet_corrupt)
            goto err;
        ret = 1;
    }

    if (0) {
err:
        ret = 1;
    }
    if (objname_free)
        util_free(objname);
    util_free(uri);

    return (ret);
}
