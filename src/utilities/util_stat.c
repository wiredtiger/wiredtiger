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
     * statistics=(all) triggers a btree walk inside the statistics cursor's open path, so a corrupt
     * leaf is hit before util_stat ever sees the cursor. The session-level quiet flag must be live
     * during that walk. The wrinkle: scoped set/clear pairs in the dhandle-open path (notably the
     * root-page read in __btree_tree_open and five similar sites) unconditionally clear the flag on
     * exit. We work around that by pre-loading the dhandle with a throwaway cursor open + close
     * (caches the dhandle, lets the scoped clear fire harmlessly), then setting the flag before
     * opening the statistics cursor, which reuses the cached dhandle and skips the scoped clear.
     *
     * Scope: this only covers leaf-page corruption. If the pre-load fails because the root or other
     * dhandle-open block is corrupt, the dhandle is not cached, the subsequent statistics cursor
     * open re-enters the same scoped-clear path, the flag is wiped, and the walk panics. The
     * principled fix is save-and-restore at the scoped set/clear sites; that is left as a follow-up
     * since it touches several subsystems beyond the wt utility.
     */
    if (quiet_corrupt) {
        WT_CURSOR *prewarm;
        if (session->open_cursor(session, objname, NULL, NULL, &prewarm) == 0)
            (void)prewarm->close(prewarm);
        F_SET((WT_SESSION_IMPL *)session, WT_SESSION_QUIET_CORRUPT_FILE);
    }

    if ((ret = session->open_cursor(session, uri, NULL, config, &cursor)) != 0) {
        fprintf(stderr, "%s: cursor open(%s) failed: %s\n", progname, uri,
          session->strerror(session, ret));
        goto err;
    }

    /*
     * List the statistics. In quiet-corrupt mode (global -q) a cursor error mid-iteration is
     * reported and we end the loop gracefully so any partial output is flushed; the command still
     * exits non-zero.
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
        if (!F_ISSET((WT_SESSION_IMPL *)session, WT_SESSION_QUIET_CORRUPT_FILE))
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
