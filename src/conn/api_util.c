/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

typedef struct __util_maintain_config UTIL_MAINTAIN_CONFIG;

struct __util_maintain_config {

#define WT_UTIL_MAINTAIN_COMMAND_NONE 0
#define WT_UTIL_MAINTAIN_COMMAND_FETCH_DATABASE_SIZE 1
    int command;

    struct {
        /*
         * FIXME-WT-17945: support local=false to dynamically recalculate the database size.
         */
        bool local;
    } fetch_database_size;

    /* Target URI for the command, or NULL for all URIs */
    char *uri;
};

static int __util_config_decode(WT_SESSION_IMPL *, WT_ITEM *, const char *, UTIL_MAINTAIN_CONFIG *);
static int __util_config_set_command(
  WT_SESSION_IMPL *, WT_ITEM *, WT_CONFIG_ITEM *, UTIL_MAINTAIN_CONFIG *, int);

static int __util_fetch_database_size(WT_SESSION_IMPL *, WT_ITEM *);

/*
 * WT_ERR_REPORT / WT_RET_REPORT --
 *     Like WT_ERR_MSG / WT_RET_MSG, but append the diagnostic to the caller-owned report buffer
 *     (which wiredtiger_util hands back) instead of logging it. The return of buffer write is
 *     ignored so it cannot clobber the requested error v -- v is what must propagate.
 */
#define WT_ERR_REPORT(session, v, ...)                                \
    do {                                                              \
        ret = (v);                                                    \
        WT_IGNORE_RET(__wt_buf_catfmt(session, report, __VA_ARGS__)); \
        goto err;                                                     \
    } while (0)

/*
 * __util_fetch_database_size --
 *     Read-only database size inspection: return the in-memory database size.
 */
static int
__util_fetch_database_size(WT_SESSION_IMPL *session, WT_ITEM *report)
{
    WT_RET(__wt_buf_catfmt(session, report, "fetch_database_size(local): %" PRIu64,
      S2C(session)->disaggregated_storage.database_size));
    return (0);
}

/*
 * __util_config_set_command --
 *     Set the command in the util_config based on the parsed config item.
 */
static int
__util_config_set_command(WT_SESSION_IMPL *session, WT_ITEM *report, WT_CONFIG_ITEM *config_item,
  UTIL_MAINTAIN_CONFIG *util_config, int command)
{
    WT_CONFIG_ITEM item;
    WT_DECL_RET;

    if (util_config->command != WT_UTIL_MAINTAIN_COMMAND_NONE)
        WT_ERR_REPORT(session, EINVAL, "Only one command is allowed in the config");

    util_config->command = command;

    if (util_config->command == WT_UTIL_MAINTAIN_COMMAND_FETCH_DATABASE_SIZE) {
        WT_ERR_NOTFOUND_OK(__wt_config_subgets(session, config_item, "local", &item), true);
        if (WT_CHECK_AND_RESET(ret, WT_NOTFOUND))
            util_config->fetch_database_size.local = true;
        else
            util_config->fetch_database_size.local = item.val != 0;
    }

err:
    return (ret);
}

/*
 * __util_config_decode --
 *     The config is parsed with the normal WT config parser:
 *
 * uri="file:collection-...wt_stable" The shared target. Absent or empty means "all URLs". It's used
 *     for all the sub-commands.
 *
 * fetch_database_size=(local=<bool>) Read-only inspection: return the in-memory database size.
 *     local=true (default) reads conn->disaggregated_storage.database_size. FIXME-WT-17945:
 *     local=false to dynamically recalculate the database size is not yet supported.
 */
static int
__util_config_decode(
  WT_SESSION_IMPL *session, WT_ITEM *report, const char *config, UTIL_MAINTAIN_CONFIG *util_config)
{
    WT_CONFIG_ITEM item;
    WT_DECL_RET;

    WT_CLEAR(item);

    util_config->command = WT_UTIL_MAINTAIN_COMMAND_NONE;

    WT_ERR_NOTFOUND_OK(__wt_config_getones(session, config, "uri", &item), true);
    if (!WT_CHECK_AND_RESET(ret, WT_NOTFOUND))
        WT_ERR(__wt_strndup(session, item.str, item.len, &util_config->uri));

    /* Check for commands */
    WT_ERR_NOTFOUND_OK(__wt_config_getones(session, config, "fetch_database_size", &item), true);
    if (!WT_CHECK_AND_RESET(ret, WT_NOTFOUND))
        WT_ERR(__util_config_set_command(
          session, report, &item, util_config, WT_UTIL_MAINTAIN_COMMAND_FETCH_DATABASE_SIZE));

    if (util_config->command == WT_UTIL_MAINTAIN_COMMAND_NONE)
        WT_ERR_REPORT(session, EINVAL, "No command found in the config");

err:
    return (ret);
}

/*
 * wiredtiger_util --
 *     WiredTiger utility in runtime. Each config can only carry one active sub-command.
 */
const char *
wiredtiger_util(WT_CONNECTION *connection, const char *config)
{
    UTIL_MAINTAIN_CONFIG util_config;
    WT_CONNECTION_IMPL *conn;
    WT_SESSION_IMPL *default_session, *session;
    WT_DECL_ITEM(report);
    WT_DECL_RET;

    WT_CLEAR(util_config);
    conn = NULL;
    default_session = NULL;
    session = NULL;

    if (connection == NULL)
        return ("wiredtiger_util: NULL connection");

    if (config == NULL || strlen(config) == 0)
        return ("wiredtiger_util: empty config");

    conn = (WT_CONNECTION_IMPL *)connection;
    default_session = conn->default_session;

    /*
     * The report buffer is owned by the connection so the returned string stays valid after this
     * call returns, until the next call reuses it. Reset it and build the new report in place.
     */
    report = &conn->util_maintain.last_report;
    report->size = 0;

    /* Open a public session for the parsing and the work; the default session owns the report. */
    WT_ERR(connection->open_session(connection, NULL, NULL, (WT_SESSION **)&session));

    WT_ERR(__util_config_decode(session, report, config, &util_config));

    if (util_config.command == WT_UTIL_MAINTAIN_COMMAND_FETCH_DATABASE_SIZE)
        WT_ERR(__util_fetch_database_size(session, report));

err:
    if (ret != 0)
        WT_IGNORE_RET(
          __wt_buf_catfmt(default_session, report, " Failed: %s", wiredtiger_strerror(ret)));

    __wt_free(default_session, util_config.uri);

    if (session != NULL)
        WT_TRET(((WT_SESSION *)session)->close((WT_SESSION *)session, NULL));

    return (report->size > 0 ? report->data : "");
}
