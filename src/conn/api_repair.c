/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

typedef struct __repair_config REPAIR_CONFIG;

struct __repair_config {

#define WT_REPAIR_COMMAND_NONE 0
#define WT_REPAIR_COMMAND_FETCH_DATABASE_SIZE 1
    int command;

    struct {
        /*
         * FIXME-WT-17945: support local=false to dynamically recalculate the database size.
         */
        bool local;
    } fetch_database_size;
};

static int __repair_config_decode(WT_SESSION_IMPL *, WT_ITEM *, const char *, REPAIR_CONFIG *);
static int __repair_config_set_command(
  WT_SESSION_IMPL *, WT_ITEM *, WT_CONFIG_ITEM *, REPAIR_CONFIG *, int);

static int __repair_fetch_database_size(WT_SESSION_IMPL *, WT_ITEM *, bool);

/*
 * WT_ERR_REPORT --
 *     Like WT_ERR_MSG, but append the diagnostic to the caller-owned report buffer (which
 *     wiredtiger_repair hands back) instead of logging it. The return of buffer write is ignored so
 *     it cannot clobber the requested error v -- v is what must propagate.
 */
#define WT_ERR_REPORT(session, v, ...)                                \
    do {                                                              \
        ret = (v);                                                    \
        WT_IGNORE_RET(__wt_buf_catfmt(session, report, __VA_ARGS__)); \
        goto err;                                                     \
    } while (0)

/*
 * __repair_fetch_database_size --
 *     Read-only database size inspection: return the in-memory database size.
 */
static int
__repair_fetch_database_size(WT_SESSION_IMPL *session, WT_ITEM *report, bool is_local)
{
    /*
     * FIXME-WT-17945: support local=false to dynamically recalculate the database size.
     */
    if (is_local == false)
        WT_RET_MSG(session, ENOTSUP, "fetch_database_size(local=false) is not yet supported");

    WT_RET(__wt_buf_catfmt(session, report, "fetch_database_size(local): %" PRIu64,
      S2C(session)->disaggregated_storage.database_size));
    return (0);
}

/*
 * __repair_config_set_command --
 *     Set the command in the repair_config based on the parsed config item.
 */
static int
__repair_config_set_command(WT_SESSION_IMPL *session, WT_ITEM *report, WT_CONFIG_ITEM *config_item,
  REPAIR_CONFIG *repair_config, int command)
{
    WT_CONFIG_ITEM item;
    WT_DECL_RET;

    if (repair_config->command != WT_REPAIR_COMMAND_NONE)
        WT_ERR_REPORT(session, EINVAL, "Only one command is allowed in the config");

    repair_config->command = command;

    if (repair_config->command == WT_REPAIR_COMMAND_FETCH_DATABASE_SIZE) {
        WT_ERR_NOTFOUND_OK(__wt_config_subgets(session, config_item, "local", &item), true);
        if (WT_CHECK_AND_RESET(ret, WT_NOTFOUND))
            repair_config->fetch_database_size.local = true;
        else
            repair_config->fetch_database_size.local = item.val != 0;
    }

err:
    return (ret);
}

/*
 * __repair_config_decode --
 *     The config is parsed with the normal WT config parser:
 *
 * fetch_database_size=(local=<bool>) Read-only inspection: return the in-memory database size.
 *     local=true (default) reads conn->disaggregated_storage.database_size. FIXME-WT-17945:
 *     local=false to dynamically recalculate the database size is not yet supported.
 */
static int
__repair_config_decode(
  WT_SESSION_IMPL *session, WT_ITEM *report, const char *config, REPAIR_CONFIG *repair_config)
{
    WT_CONFIG_ITEM item;
    WT_DECL_RET;

    WT_CLEAR(item);

    repair_config->command = WT_REPAIR_COMMAND_NONE;

    /* Check for commands */
    WT_ERR_NOTFOUND_OK(__wt_config_getones(session, config, "fetch_database_size", &item), true);
    if (!WT_CHECK_AND_RESET(ret, WT_NOTFOUND))
        WT_ERR(__repair_config_set_command(
          session, report, &item, repair_config, WT_REPAIR_COMMAND_FETCH_DATABASE_SIZE));

    if (repair_config->command == WT_REPAIR_COMMAND_NONE)
        WT_ERR_REPORT(session, EINVAL, "No command found in the config");

err:
    return (ret);
}

/*
 * wiredtiger_repair --
 *     WiredTiger repair in runtime. Each config can only carry one active sub-command.
 */
const char *
wiredtiger_repair(WT_CONNECTION *connection, const char *config)
{
    REPAIR_CONFIG repair_config;
    WT_CONNECTION_IMPL *conn;
    WT_SESSION_IMPL *default_session, *session;
    WT_DECL_ITEM(report);
    WT_DECL_RET;

    WT_CLEAR(repair_config);
    default_session = NULL;
    session = NULL;

    if (!__wt_atomic_cas_uint8(&((WT_CONNECTION_IMPL *)connection)->repair.op_lock, 0, 1)) {
        return ("wiredtiger_repair: another repair operation is in progress");
    }

    if (connection == NULL)
        return ("wiredtiger_repair: NULL connection");

    if (config == NULL || strlen(config) == 0)
        return ("wiredtiger_repair: empty config");

    conn = (WT_CONNECTION_IMPL *)connection;
    default_session = conn->default_session;

    /*
     * The report buffer is owned by the connection so the returned string stays valid after this
     * call returns, until the next call reuses it. Reset it and build the new report in place.
     */
    report = &conn->repair.last_report;
    report->size = 0;

    /* Open a public session for the parsing and the work; the default session owns the report. */
    WT_ERR(connection->open_session(connection, NULL, NULL, (WT_SESSION **)&session));

    WT_ERR(__repair_config_decode(session, report, config, &repair_config));

    if (repair_config.command == WT_REPAIR_COMMAND_FETCH_DATABASE_SIZE)
        WT_ERR(
          __repair_fetch_database_size(session, report, repair_config.fetch_database_size.local));

err:
    if (ret != 0)
        WT_IGNORE_RET(
          __wt_buf_catfmt(default_session, report, " Failed: %s", wiredtiger_strerror(ret)));

    if (session != NULL)
        WT_TRET(((WT_SESSION *)session)->close((WT_SESSION *)session, NULL));

    /* Release the repair operation lock. */
    __wt_atomic_store_uint8(&((WT_CONNECTION_IMPL *)connection)->repair.op_lock, 0);

    return (report->size > 0 ? report->data : "");
}
