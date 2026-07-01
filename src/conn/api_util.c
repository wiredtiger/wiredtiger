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
#define WT_UTIL_MAINTAIN_COMMAND_FETCH_METADATA 1
#define WT_UTIL_MAINTAIN_COMMAND_FETCH_DATABASE_SIZE 2
#define WT_UTIL_MAINTAIN_COMMAND_FIX_SIZE 3
    int command;

    struct {
        /* Fetch metadata from local cache, not page server. */
        bool local;
        const char *key;
    } fetch_metadata;

    struct {
        /*
         * Fetch database size source from memory or recalculate.
         */
        bool local;
    } fetch_database_size;

    struct {
        /* Size is 0 means no guard. */
        uint64_t old_size;
    } fix_size;

    /* Target URI for the command, or NULL for all URIs */
    char *uri;
};

static int __util_config_decode(WT_SESSION_IMPL *, WT_ITEM *, const char *, UTIL_MAINTAIN_CONFIG *);
static int __util_config_set_command(
  WT_SESSION_IMPL *, WT_ITEM *, WT_CONFIG_ITEM *, UTIL_MAINTAIN_CONFIG *, int);

static int __util_fetch_metadata(WT_SESSION_IMPL *, WT_ITEM *, const char *, const char *, bool);
static int __util_fetch_database_size(WT_SESSION_IMPL *, WT_ITEM *, bool);
static int __util_fix_size(WT_SESSION_IMPL *, WT_ITEM *, uint64_t);

#define WT_ERR_REPORT(session, v, ...)                                \
    do {                                                              \
        ret = (v);                                                    \
        WT_IGNORE_RET(__wt_buf_catfmt(session, report, __VA_ARGS__)); \
        goto err;                                                     \
    } while (0)

#define WT_RET_REPORT(session, v, ...)                                \
    do {                                                              \
        int __ret = (v);                                              \
        WT_IGNORE_RET(__wt_buf_catfmt(session, report, __VA_ARGS__)); \
        return (__ret);                                               \
    } while (0)

/*
 * __util_fetch_metadata --
 *     Read-only metadata inspection: return metadata without modifying anything.
 */
static int
__util_fetch_metadata(
  WT_SESSION_IMPL *session, WT_ITEM *report, const char *uri, const char *key, bool is_local)
{
    WT_CONFIG_ITEM item;
    WT_CURSOR *cursor;
    WT_DECL_ITEM(uri_buf);
    WT_DECL_RET;
    const char *ckpt_name, *k, *v;
    bool found;

    cursor = NULL;
    ckpt_name = k = v = NULL;
    found = false;

    if (is_local)
        WT_RET(__wt_metadata_cursor(session, &cursor));
    else {
        const char *cfg[] = {WT_CONFIG_BASE(session, WT_SESSION_open_cursor), NULL};

        /*
         * Find the shared metadata table's most recent checkpoint, the page-server-durable state.
         */
        WT_ERR_NOTFOUND_OK(
          __wt_meta_checkpoint_last_name(session, WT_DISAGG_METADATA_URI, &ckpt_name, NULL, NULL),
          false);
        if (ckpt_name == NULL)
            WT_ERR_REPORT(session, WT_NOTFOUND, "The shared metadata table has no checkpoint yet");

        WT_ERR(__wt_scr_alloc(session, 0, &uri_buf));
        WT_ERR(__wt_buf_fmt(session, uri_buf, "%s/%s", WT_DISAGG_METADATA_URI, ckpt_name));
        WT_ERR(__wt_open_cursor(session, uri_buf->data, NULL, cfg, &cursor));
    }

    /*
     * Walk through the metadata and dump target entries.
     */
    while ((ret = cursor->next(cursor)) == 0) {
        WT_ERR(cursor->get_key(cursor, &k));
        if (uri != NULL && strcmp(k, uri) != 0)
            continue;

        found = true;
        WT_ERR(cursor->get_value(cursor, &v));
        if (key != NULL) {
            WT_ERR_NOTFOUND_OK(__wt_config_getones(session, v, key, &item), true);
            if (WT_CHECK_AND_RESET(ret, WT_NOTFOUND)) {
                WT_ERR(__wt_buf_catfmt(session, report, "\n  %s: <no \"%s\">", k, key));
                continue;
            }
            WT_ERR(
              __wt_buf_catfmt(session, report, "\n  %s: %s=%.*s", k, key, (int)item.len, item.str));
        } else
            WT_ERR(__wt_buf_catfmt(session, report, "\n  %s: %s", k, v));
    }

    WT_ERR_NOTFOUND_OK(ret, false);

    if (!found)
        WT_ERR(
          __wt_buf_catfmt(session, report, " <no matching metadata entry for uri:\"%s\">", uri));

err:
    if (cursor != NULL) {
        if (is_local)
            WT_TRET(__wt_metadata_cursor_release(session, &cursor));
        else
            WT_TRET(cursor->close(cursor));
    }
    __wt_free(session, ckpt_name);
    __wt_scr_free(session, &uri_buf);
    return (ret);
}

/*
 * __util_fetch_database_size --
 *     Read-only database size inspection: return the in-memory database size.
 */
static int
__util_fetch_database_size(WT_SESSION_IMPL *session, WT_ITEM *report, bool is_local)
{
    if (is_local)
        WT_RET(__wt_buf_catfmt(session, report, "fetch_database_size(local): %" PRIu64,
          S2C(session)->disaggregated_storage.database_size));
    else {
        uint64_t database_size = 0;
        WT_RET(__wt_disagg_get_database_size(session, &database_size));
        WT_RET(__wt_buf_catfmt(
          session, report, "fetch_database_size(accumulate): %" PRIu64, database_size));
    }
    return (0);
}

/*
 * __util_fix_size --
 *     Metadata-only size change for one stable file: pre-check, then rewrite plus shared-metadata
 *     enqueue. Returns the size the file had before the change.
 */
static int
__util_fix_size(WT_SESSION_IMPL *session, WT_ITEM *report, uint64_t old_size)
{
    WT_CONNECTION_IMPL *conn;

    conn = S2C(session);

    if (old_size != 0) {
        uint64_t current_size = conn->disaggregated_storage.database_size;
        if (current_size != old_size)
            WT_RET_REPORT(session, EINVAL,
              "size_fix: stored database size %" PRIu64
              " does not match requested old_size %" PRIu64,
              current_size, old_size);
    }

    /*
     * Claim the cycle. PROCESSING holds it while we stage so a checkpoint cannot consume a partial
     * fix; the CAS also rejects a second concurrent repair.
     */
    if (!__wt_atomic_cas_uint8(
          &conn->util_maintain.state, WT_UTIL_MAINTAIN_IDLE, WT_UTIL_MAINTAIN_DB_SIZE_FIX))
        WT_RET_REPORT(session, EBUSY, "size_fix: a util maintain is already in progress");

    WT_RET(__wt_buf_catfmt(session, report, "size_fix triggered"));

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
    bool require_disagg, require_disagg_leader;

    require_disagg = false;
    require_disagg_leader = false;

    if (util_config->command != WT_UTIL_MAINTAIN_COMMAND_NONE)
        WT_ERR_REPORT(session, EINVAL, "Only one command is allowed in the config");

    util_config->command = command;

    if (util_config->command == WT_UTIL_MAINTAIN_COMMAND_FETCH_METADATA) {
        WT_ERR_NOTFOUND_OK(__wt_config_subgets(session, config_item, "local", &item), true);
        if (WT_CHECK_AND_RESET(ret, WT_NOTFOUND))
            util_config->fetch_metadata.local = true;
        else
            util_config->fetch_metadata.local = item.val != 0;
        WT_ERR_NOTFOUND_OK(__wt_config_subgets(session, config_item, "key", &item), true);
        if (!WT_CHECK_AND_RESET(ret, WT_NOTFOUND))
            WT_ERR(__wt_strndup(session, item.str, item.len, &util_config->fetch_metadata.key));

        require_disagg = !util_config->fetch_metadata.local;
    } else if (util_config->command == WT_UTIL_MAINTAIN_COMMAND_FETCH_DATABASE_SIZE) {
        WT_ERR_NOTFOUND_OK(__wt_config_subgets(session, config_item, "local", &item), true);
        if (WT_CHECK_AND_RESET(ret, WT_NOTFOUND))
            util_config->fetch_database_size.local = true;
        else
            util_config->fetch_database_size.local = item.val != 0;

        require_disagg = !util_config->fetch_database_size.local;
    } else if (util_config->command == WT_UTIL_MAINTAIN_COMMAND_FIX_SIZE) {
        WT_ERR_NOTFOUND_OK(__wt_config_subgets(session, config_item, "old_size", &item), true);
        if (WT_CHECK_AND_RESET(ret, WT_NOTFOUND))
            util_config->fix_size.old_size = 0; /* No guard */
        else
            util_config->fix_size.old_size = (uint64_t)item.val;

        require_disagg = true;
        require_disagg_leader = true;
    }

    if (require_disagg) {
        if (!__wt_disagg_has_picked_up_checkpoint(session))
            WT_ERR_REPORT(session, EINVAL,
              "This command requires a disaggregated connection with a valid checkpoint");

        if (require_disagg_leader && !S2C(session)->layered_table_manager.leader)
            WT_ERR_REPORT(
              session, EINVAL, "This command requires a disaggregated leader connection");
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
 *     local=true (default) reads the local database size. local=false (Disagg-only) reads the
 *     shared database size.
 *
 * fetch_metadata=(local=<bool>,key="<key>") Read-only inspection: return metadata values.
 *     local=true (default) reads the local metadata cursor. local=false (Disagg-only) reads the
 *     shared metadata cursor. key="" selects one first-layer config value. Absent or empty means
 *     "all Keys".
 *
 * fix=(size=(old_size=<n>)) (Disagg-only, require leader)reset the database size to the sum of the
 *     per-file checkpoint sizes. old_size is an optional guards as checkpoints shift quickly.
 *
 */
static int
__util_config_decode(
  WT_SESSION_IMPL *session, WT_ITEM *report, const char *config, UTIL_MAINTAIN_CONFIG *util_config)
{
    WT_CONFIG_ITEM item, sub_item;
    WT_DECL_RET;

    WT_CLEAR(item);

    util_config->command = WT_UTIL_MAINTAIN_COMMAND_NONE;

    WT_ERR_NOTFOUND_OK(__wt_config_getones(session, config, "uri", &item), true);
    if (!WT_CHECK_AND_RESET(ret, WT_NOTFOUND))
        WT_ERR(__wt_strndup(session, item.str, item.len, &util_config->uri));

    /* Check for commands */
    WT_ERR_NOTFOUND_OK(__wt_config_getones(session, config, "fetch_metadata", &item), true);
    if (!WT_CHECK_AND_RESET(ret, WT_NOTFOUND))
        WT_ERR(__util_config_set_command(
          session, report, &item, util_config, WT_UTIL_MAINTAIN_COMMAND_FETCH_METADATA));

    WT_ERR_NOTFOUND_OK(__wt_config_getones(session, config, "fetch_database_size", &item), true);
    if (!WT_CHECK_AND_RESET(ret, WT_NOTFOUND))
        WT_ERR(__util_config_set_command(
          session, report, &item, util_config, WT_UTIL_MAINTAIN_COMMAND_FETCH_DATABASE_SIZE));

    WT_ERR_NOTFOUND_OK(__wt_config_getones(session, config, "fix", &item), true);
    if (!WT_CHECK_AND_RESET(ret, WT_NOTFOUND)) {
        WT_ERR_NOTFOUND_OK(__wt_config_subgets(session, &item, "size", &sub_item), true);
        if (WT_CHECK_AND_RESET(ret, WT_NOTFOUND))
            WT_ERR_REPORT(session, EINVAL, "No sub-command found in the fix config");

        WT_ERR(__util_config_set_command(
          session, report, &sub_item, util_config, WT_UTIL_MAINTAIN_COMMAND_FIX_SIZE));
    }

    if (util_config->command == WT_UTIL_MAINTAIN_COMMAND_NONE)
        WT_ERR_REPORT(session, EINVAL, "No command found in the config");

err:
    return (ret);
}

/*
 * wiredtiger_util --
 *     WiredTiger utility in runtime. ! Each config can only carry one active sub-command. ! Each
 *     fix should be validate by a following fetch command.
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

    if (util_config.command == WT_UTIL_MAINTAIN_COMMAND_FETCH_METADATA)
        WT_ERR(__util_fetch_metadata(session, report, util_config.uri,
          util_config.fetch_metadata.key, util_config.fetch_metadata.local));
    else if (util_config.command == WT_UTIL_MAINTAIN_COMMAND_FETCH_DATABASE_SIZE)
        WT_ERR(__util_fetch_database_size(session, report, util_config.fetch_database_size.local));
    else if (util_config.command == WT_UTIL_MAINTAIN_COMMAND_FIX_SIZE)
        WT_ERR(__util_fix_size(session, report, util_config.fix_size.old_size));

err:
    if (ret != 0)
        WT_IGNORE_RET(
          __wt_buf_catfmt(default_session, report, " Failed: %s", wiredtiger_strerror(ret)));

    __wt_free(default_session, util_config.fetch_metadata.key);
    __wt_free(default_session, util_config.uri);

    if (session != NULL)
        WT_TRET(((WT_SESSION *)session)->close((WT_SESSION *)session, NULL));

    return (report->size > 0 ? report->data : "");
}
