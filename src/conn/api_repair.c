/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

typedef struct __wt_repair_config WT_REPAIR_CONFIG;

struct __wt_repair_config {

#define WT_REPAIR_COMMAND_NONE 0
#define WT_REPAIR_COMMAND_FETCH_DATABASE_SIZE 1
#define WT_REPAIR_COMMAND_FETCH_METADATA 2
#define WT_REPAIR_COMMAND_FIX_ID 3
    int command;

    struct {
        /* local=false recomputes the size from the metadata instead of reading the running total.
         */
        bool local;
    } fetch_database_size;

    struct {
        /* Fetch metadata from the local cursor, not the shared page-server checkpoint. */
        bool local;
        /* Target URI for the command, or NULL for all URIs. */
        const char *uri;
        /* Single metadata key to report, or NULL for the whole value. */
        const char *key;
    } fetch_metadata;

    struct {
        /* The metadata entry to renumber; the entry holding "id", e.g. a file: URI. */
        const char *uri;
        /* The table the URI is the stable file of, or NULL when there is no shared metadata. */
        const char *table_name;
        /* The current id, used as a guard against fixing the wrong entry. */
        uint32_t old_id;
        /* The replacement id. */
        uint32_t new_id;
    } fix_id;
};

static int __repair_config_decode(WT_SESSION_IMPL *, WT_ITEM *, const char *, WT_REPAIR_CONFIG *);
static int __repair_config_set_command(
  WT_SESSION_IMPL *, WT_ITEM *, WT_CONFIG_ITEM *, WT_REPAIR_CONFIG *, int);

static int __repair_fetch_database_size(WT_SESSION_IMPL *, WT_ITEM *, bool);
static int __repair_fetch_metadata(WT_SESSION_IMPL *, WT_ITEM *, const char *, const char *, bool);
static int __repair_fix_id(
  WT_SESSION_IMPL *, WT_ITEM *, const char *, const char *, uint32_t, uint32_t);

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
 *     Read-only database size inspection: local=true returns the maintained total; local=false
 *     recomputes it from the metadata.
 */
static int
__repair_fetch_database_size(WT_SESSION_IMPL *session, WT_ITEM *report, bool is_local)
{
    uint64_t database_size;

    if (is_local)
        WT_RET(__wt_buf_catfmt(session, report, "fetch_database_size(local): %" PRIu64,
          S2C(session)->disaggregated_storage.database_size));
    else {
        WT_RET(__wt_disagg_get_database_size(session, &database_size));
        WT_RET(__wt_buf_catfmt(session, report, "fetch_database_size(recompute): %" PRIu64,
          database_size + WT_DISAGG_CHECKPOINT_SIZE_BUFFER));
    }
    return (0);
}

/*
 * __repair_fetch_metadata --
 *     Read-only metadata inspection: return metadata without modifying anything.
 */
static int
__repair_fetch_metadata(
  WT_SESSION_IMPL *session, WT_ITEM *report, const char *uri, const char *key, bool is_local)
{
    WT_CONFIG_ITEM item;
    WT_CURSOR *cursor;
    WT_DECL_ITEM(ckpt_uri);
    WT_DECL_RET;
    const char *ckpt_name, *k, *v;
    bool found;

    cursor = NULL;
    ckpt_name = NULL;
    found = false;

    if (is_local)
        WT_ERR(__wt_metadata_cursor(session, &cursor));
    else {
        const char *cfg[] = {WT_CONFIG_BASE(session, WT_SESSION_open_cursor), NULL};

        /*
         * The require_disagg check in __repair_config_set_command already confirmed this connection
         * has picked up a checkpoint, which guarantees the shared metadata table has a local
         * checkpoint (the page-server-durable state) to open here.
         */
        WT_ERR(
          __wt_meta_checkpoint_last_name(session, WT_DISAGG_METADATA_URI, &ckpt_name, NULL, NULL));

        WT_ERR(__wt_scr_alloc(session, 0, &ckpt_uri));
        WT_ERR(__wt_buf_fmt(session, ckpt_uri, "%s/%s", WT_DISAGG_METADATA_URI, ckpt_name));
        WT_ERR(__wt_open_cursor(session, ckpt_uri->data, NULL, cfg, &cursor));
    }

    /* Walk the metadata and report the entries matching the target URI. */
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
        WT_ERR(__wt_buf_catfmt(session, report, " <no matching metadata entry for uri:\"%s\">",
          uri == NULL ? "<all>" : uri));

err:
    if (cursor != NULL) {
        if (is_local)
            WT_TRET(__wt_metadata_cursor_release(session, &cursor));
        else
            WT_TRET(cursor->close(cursor));
    }
    __wt_free(session, ckpt_name);
    __wt_scr_free(session, &ckpt_uri);
    return (ret);
}

/*
 * __repair_fix_id_validate --
 *     Check that the fix is safe and return the metadata value to rewrite.
 */
static int
__repair_fix_id_validate(WT_SESSION_IMPL *session, WT_ITEM *report, const char *uri,
  uint32_t old_id, uint32_t new_id, char **valuep)
{
    WT_CONFIG_ITEM item;
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    char *conflict_uri, *value;

    conn = S2C(session);
    conflict_uri = value = NULL;
    *valuep = NULL;

    WT_ERR_NOTFOUND_OK(__wt_metadata_search(session, uri, &value), true);
    if (WT_CHECK_AND_RESET(ret, WT_NOTFOUND))
        WT_ERR_REPORT(session, EINVAL, " no metadata entry for uri:\"%s\"", uri);

    WT_ERR_NOTFOUND_OK(__wt_config_getones(session, value, "id", &item), true);
    if (WT_CHECK_AND_RESET(ret, WT_NOTFOUND))
        WT_ERR_REPORT(session, EINVAL, " uri:\"%s\" has no id", uri);
    if ((uint32_t)item.val != old_id)
        WT_ERR_REPORT(session, EINVAL,
          " uri:\"%s\" has id=%" PRId64 ", not the expected old_id=%" PRIu32, uri, item.val,
          old_id);

    /*
     * The low bits of an id say whether the table is local, shared or one of the fixed special
     * tables, so they have to be preserved.
     */
    if (WT_BTREE_ID_NAMESPACE_ID(new_id) != WT_BTREE_ID_NAMESPACE_ID(old_id))
        WT_ERR_REPORT(session, EINVAL,
          " new_id=%" PRIu32 " is not in the same namespace as old_id=%" PRIu32, new_id, old_id);

    /* A unique id is the whole point, so refuse to swap one collision for another. */
    WT_ERR_NOTFOUND_OK(__wt_metadata_btree_id_to_uri(session, new_id, &conflict_uri), true);
    if (!WT_CHECK_AND_RESET(ret, WT_NOTFOUND))
        WT_ERR_REPORT(session, EINVAL, " new_id=%" PRIu32 " is already used by uri:\"%s\"", new_id,
          conflict_uri);

    /* Stay above the allocator, or a later create hands the same id out again. */
    if (WT_BTREE_ID_UNNAMESPACED(new_id) <= conn->next_file_id)
        WT_ERR_REPORT(session, EINVAL,
          " new_id=%" PRIu32 " is not above the largest id allocated so far (%" PRIu32 ")", new_id,
          WT_BTREE_ID_NAMESPACED(conn->next_file_id, WT_BTREE_ID_NAMESPACE_ID(new_id)));

    /*
     * An open handle caches the old id in its btree, and in disaggregated storage a page service
     * handle opened with it, so the fix would not take effect.
     */
    WT_WITH_HANDLE_LIST_READ_LOCK(
      session, WT_SAVE_DHANDLE(session, ret = __wt_conn_dhandle_find(session, uri, NULL)));
    if (ret == 0)
        WT_ERR_REPORT(session, EBUSY,
          " uri:\"%s\" is open; close its cursors and let the handle sweep before fixing it", uri);
    WT_ERR_NOTFOUND_OK(ret, false);

    *valuep = value;
    value = NULL;

err:
    __wt_free(session, conflict_uri);
    __wt_free(session, value);
    return (ret);
}

/*
 * __repair_fix_id_apply --
 *     Write the new id to the local metadata and, in disaggregated storage, queue the same change
 *     for the shared metadata table. The schema lock is held.
 */
static int
__repair_fix_id_apply(WT_SESSION_IMPL *session, WT_ITEM *report, const char *uri,
  const char *table_name, uint32_t old_id, uint32_t new_id)
{
    WT_DECL_RET;
    char id_cfg[32], *new_value, *value;
    const char *cfg[3] = {NULL, NULL, NULL};

    new_value = value = NULL;

    WT_ERR(__repair_fix_id_validate(session, report, uri, old_id, new_id, &value));

    WT_ERR(__wt_snprintf(id_cfg, sizeof(id_cfg), "id=%" PRIu32, new_id));
    cfg[0] = value;
    cfg[1] = id_cfg;
    WT_ERR(__wt_config_collapse(session, cfg, &new_value));
    WT_ERR(__wt_metadata_update(session, uri, new_value));

    S2C(session)->next_file_id = WT_BTREE_ID_UNNAMESPACED(new_id);

    /*
     * A checkpoint only rewrites the shared metadata of the tables it writes, and this table is not
     * being written, so queue the entry by hand. No schema epoch and deferred=false put it in the
     * checkpoint the caller is about to take.
     */
    if (table_name != NULL)
        WT_ERR(__wt_disagg_enqueue_metadata_operation(
          session, uri, table_name, WT_SHARED_METADATA_UPDATE, WT_SCHEMA_EPOCH_NONE, false));

err:
    __wt_free(session, new_value);
    __wt_free(session, value);
    return (ret);
}

/*
 * __repair_fix_id --
 *     Give one table a new btree id, using its current id as a guard. This renumbers metadata only:
 *     in disaggregated storage the id is the table's page service namespace, so the pages already
 *     written under the old id are not reachable afterwards and the caller has to rebuild or drop
 *     the table. Renumbering first is what makes that drop safe, because a drop trims every page
 *     under the table's id, including the pages of whichever table it collided with.
 */
static int
__repair_fix_id(WT_SESSION_IMPL *session, WT_ITEM *report, const char *uri, const char *table_name,
  uint32_t old_id, uint32_t new_id)
{
    WT_DECL_RET;

    WT_RET(__wt_buf_catfmt(
      session, report, "fix_id(uri=\"%s\", %" PRIu32 " -> %" PRIu32 "):", uri, old_id, new_id));

    WT_WITH_SCHEMA_LOCK(
      session, ret = __repair_fix_id_apply(session, report, uri, table_name, old_id, new_id));
    WT_RET(ret);

    /* Make the new id durable, then read it back from where it has to be durable. */
    WT_RET(((WT_SESSION *)session)->checkpoint((WT_SESSION *)session, NULL));

    WT_RET(__wt_buf_catfmt(session, report,
      " fixed, now rebuild or drop this table: its data is not reachable under the new id."));

    return (__repair_fetch_metadata(session, report, uri, "id", !__wt_conn_is_disagg(session)));
}

/*
 * __repair_config_set_command --
 *     Set the command in the repair_config based on the parsed config item.
 */
static int
__repair_config_set_command(WT_SESSION_IMPL *session, WT_ITEM *report, WT_CONFIG_ITEM *config_item,
  WT_REPAIR_CONFIG *repair_config, int command)
{
    WT_CONFIG_ITEM item;
    WT_DECL_RET;
    int64_t new_id, old_id;
    const char *name, *suffix;
    bool require_disagg;

    new_id = old_id = 0;
    require_disagg = false;

    if (repair_config->command != WT_REPAIR_COMMAND_NONE)
        WT_ERR_REPORT(session, EINVAL, "Only one command is allowed in the config");

    repair_config->command = command;

    if (repair_config->command == WT_REPAIR_COMMAND_FETCH_DATABASE_SIZE) {
        WT_ERR_NOTFOUND_OK(__wt_config_subgets(session, config_item, "local", &item), true);
        if (WT_CHECK_AND_RESET(ret, WT_NOTFOUND))
            repair_config->fetch_database_size.local = true;
        else
            repair_config->fetch_database_size.local = item.val != 0;

        /*
         * Both variants read or derive conn->disaggregated_storage.database_size, a concept that
         * only exists on a disaggregated connection.
         */
        require_disagg = true;
    } else if (repair_config->command == WT_REPAIR_COMMAND_FETCH_METADATA) {
        WT_ERR_NOTFOUND_OK(__wt_config_subgets(session, config_item, "local", &item), true);
        if (WT_CHECK_AND_RESET(ret, WT_NOTFOUND))
            repair_config->fetch_metadata.local = true;
        else
            repair_config->fetch_metadata.local = item.val != 0;

        /* An empty uri/key is treated as absent (NULL): all URIs / the whole value. */
        WT_ERR_NOTFOUND_OK(__wt_config_subgets(session, config_item, "uri", &item), true);
        if (!WT_CHECK_AND_RESET(ret, WT_NOTFOUND) && item.len != 0)
            WT_ERR(__wt_strndup(session, item.str, item.len, &repair_config->fetch_metadata.uri));

        WT_ERR_NOTFOUND_OK(__wt_config_subgets(session, config_item, "key", &item), true);
        if (!WT_CHECK_AND_RESET(ret, WT_NOTFOUND) && item.len != 0)
            WT_ERR(__wt_strndup(session, item.str, item.len, &repair_config->fetch_metadata.key));

        require_disagg = !repair_config->fetch_metadata.local;
    } else if (repair_config->command == WT_REPAIR_COMMAND_FIX_ID) {
        /* All three settings are required; none of them has a safe default. */
        WT_ERR_NOTFOUND_OK(__wt_config_subgets(session, config_item, "uri", &item), true);
        if (WT_CHECK_AND_RESET(ret, WT_NOTFOUND) || item.len == 0)
            WT_ERR_REPORT(session, EINVAL, "fix_id requires uri, old_id and new_id");
        WT_ERR(__wt_strndup(session, item.str, item.len, &repair_config->fix_id.uri));

        WT_ERR_NOTFOUND_OK(__wt_config_subgets(session, config_item, "old_id", &item), true);
        if (WT_CHECK_AND_RESET(ret, WT_NOTFOUND))
            WT_ERR_REPORT(session, EINVAL, "fix_id requires uri, old_id and new_id");
        old_id = item.val;

        WT_ERR_NOTFOUND_OK(__wt_config_subgets(session, config_item, "new_id", &item), true);
        if (WT_CHECK_AND_RESET(ret, WT_NOTFOUND))
            WT_ERR_REPORT(session, EINVAL, "fix_id requires uri, old_id and new_id");
        new_id = item.val;

        if (old_id <= 0 || old_id >= WT_BTREE_ID_INVALID || new_id <= 0 ||
          new_id >= WT_BTREE_ID_INVALID)
            WT_ERR_REPORT(session, EINVAL, "fix_id ids must be between 1 and %" PRIu32,
              WT_BTREE_ID_INVALID - 1);
        if (old_id == new_id)
            WT_ERR_REPORT(session, EINVAL, "fix_id old_id and new_id are the same");

        repair_config->fix_id.old_id = (uint32_t)old_id;
        repair_config->fix_id.new_id = (uint32_t)new_id;

        /* The metadata write needs somewhere to land: a local connection or a disagg leader. */
        if (__wt_conn_is_disagg(session)) {
            if (!S2C(session)->layered_table_manager.leader)
                WT_ERR_REPORT(
                  session, ENOTSUP, "fix_id requires a disaggregated leader connection");

            /*
             * Only the stable file of a layered table is copied to the shared metadata table, so a
             * disaggregated fix has to name one. Derive the table name the copy is keyed by.
             */
            name = repair_config->fix_id.uri + strlen("file:");
            suffix = strstr(repair_config->fix_id.uri, ".wt_stable");
            if (!WT_PREFIX_MATCH(repair_config->fix_id.uri, "file:") || suffix == NULL ||
              suffix <= name)
                WT_ERR_REPORT(session, EINVAL,
                  "fix_id needs a \"file:<name>.wt_stable\" uri on a disaggregated connection");
            WT_ERR(__wt_strndup(
              session, name, (size_t)(suffix - name), &repair_config->fix_id.table_name));
        }
    }

    if (require_disagg && !__wt_disagg_has_picked_up_checkpoint(session))
        WT_ERR_REPORT(session, EINVAL,
          "This command requires a disaggregated connection with a valid checkpoint");

err:
    return (ret);
}

/*
 * __repair_config_decode --
 *     The config is parsed with the normal WT config parser:
 *
 * fetch_database_size=(local=<bool>) Read-only inspection: return the database size (disagg-only).
 *     local=true (default) reads conn->disaggregated_storage.database_size, the maintained running
 *     total. local=false recomputes the same total from scratch by walking the metadata (the same
 *     computation session->checkpoint(debug=(database_size_fix=true)) uses to correct drift).
 *
 * fetch_metadata=(local=<bool>,uri="<uri>",key="<key>") Read-only inspection: return metadata
 *     values. local=true (default) reads the local metadata cursor; local=false (disagg-only) reads
 *     the shared, page-server-durable metadata checkpoint. uri="" selects one target; absent or
 *     empty means all URIs. key="" selects one first-level config value out of the matching
 *     entries; absent or empty means the whole value.
 *
 * fix_id=(uri="<uri>",old_id=<id>,new_id=<id>) Write: give one table a new btree id, to break a
 *     duplicated id between two tables. All three settings are required; old_id must match the
 *     table's current id, which guards against renumbering the wrong table. The table must not be
 *     open, new_id must be unused and above every id allocated so far, and in disaggregated storage
 *     this must run on the leader. The renumbered table's data is left behind under the old id, so
 *     rebuild or drop it afterwards; pick the table that can be rebuilt.
 */
static int
__repair_config_decode(
  WT_SESSION_IMPL *session, WT_ITEM *report, const char *config, WT_REPAIR_CONFIG *repair_config)
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

    WT_ERR_NOTFOUND_OK(__wt_config_getones(session, config, "fetch_metadata", &item), true);
    if (!WT_CHECK_AND_RESET(ret, WT_NOTFOUND))
        WT_ERR(__repair_config_set_command(
          session, report, &item, repair_config, WT_REPAIR_COMMAND_FETCH_METADATA));

    WT_ERR_NOTFOUND_OK(__wt_config_getones(session, config, "fix_id", &item), true);
    if (!WT_CHECK_AND_RESET(ret, WT_NOTFOUND))
        WT_ERR(__repair_config_set_command(
          session, report, &item, repair_config, WT_REPAIR_COMMAND_FIX_ID));

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
    WT_CONNECTION_IMPL *conn;
    WT_DECL_ITEM(report);
    WT_DECL_RET;
    WT_REPAIR_CONFIG repair_config;
    WT_SESSION_IMPL *default_session, *session;

    WT_CLEAR(repair_config);
    session = NULL;

    if (connection == NULL)
        return ("wiredtiger_repair: NULL connection");

    conn = (WT_CONNECTION_IMPL *)connection;
    default_session = conn->default_session;

    if (!__wt_atomic_cas_uint8(
          &conn->repair.state, WT_REPAIR_STATE_IDLE, WT_REPAIR_STATE_OPERATING))
        return ("wiredtiger_repair: another repair operation is in progress");

    /*
     * The report buffer is owned by the connection so the returned string stays valid after this
     * call returns, until the next call reuses it. Reset it and build the new report in place.
     */
    report = &conn->repair.last_report;
    report->size = 0;

    if (config == NULL || strlen(config) == 0)
        WT_ERR_REPORT(default_session, EINVAL, "wiredtiger_repair: empty config");

    /* Open a public session for the parsing and the work; the default session owns the report. */
    WT_ERR(connection->open_session(connection, NULL, NULL, (WT_SESSION **)&session));

    WT_ERR(__repair_config_decode(session, report, config, &repair_config));

    switch (repair_config.command) {
    case WT_REPAIR_COMMAND_FETCH_DATABASE_SIZE:
        WT_ERR(
          __repair_fetch_database_size(session, report, repair_config.fetch_database_size.local));
        break;
    case WT_REPAIR_COMMAND_FETCH_METADATA:
        WT_ERR(__repair_fetch_metadata(session, report, repair_config.fetch_metadata.uri,
          repair_config.fetch_metadata.key, repair_config.fetch_metadata.local));
        break;
    case WT_REPAIR_COMMAND_FIX_ID:
        WT_ERR(__repair_fix_id(session, report, repair_config.fix_id.uri,
          repair_config.fix_id.table_name, repair_config.fix_id.old_id,
          repair_config.fix_id.new_id));
        break;
    default:
        WT_ERR(__wt_illegal_value(session, repair_config.command));
    }

err:
    if (ret != 0)
        WT_IGNORE_RET(
          __wt_buf_catfmt(default_session, report, " Failed: %s", wiredtiger_strerror(ret)));

    __wt_free(default_session, repair_config.fetch_metadata.uri);
    __wt_free(default_session, repair_config.fetch_metadata.key);
    __wt_free(default_session, repair_config.fix_id.uri);
    __wt_free(default_session, repair_config.fix_id.table_name);

    if (session != NULL)
        WT_IGNORE_RET(((WT_SESSION *)session)->close((WT_SESSION *)session, NULL));

    WT_IGNORE_RET(
      __wt_atomic_cas_uint8(&conn->repair.state, WT_REPAIR_STATE_OPERATING, WT_REPAIR_STATE_IDLE));

    return (report->size > 0 ? report->data : "");
}
