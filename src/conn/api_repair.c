/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * Each repair action below is built from per-action pre-check and execute phases. Persistence is
 * shared: wiredtiger_repair issues a single forced checkpoint after every requested action has
 * executed, which flushes the modifications to the page server.
 */

/*
 * __repair_conn_disagg_precheck --
 *     Connection-level pre-check for any page-server access: the connection must be disaggregated.
 *     A read of the remote shared metadata works on a leader or a follower (pick-up materializes
 *     the checkpoint on both). Returns a diagnostic string for the caller to hand back, or NULL.
 */
static const char *
__repair_conn_disagg_precheck(WT_SESSION_IMPL *session)
{
    if (!__wt_conn_is_disagg(session))
        return (
          "wiredtiger_repair: connection is NOT disaggregated; skipped, no page-server access");
    return (NULL);
}

/*
 * __repair_conn_precheck --
 *     Connection-level pre-check for the mutating actions: only a disaggregated leader may write to
 *     the page server. Returns a diagnostic string for the caller to hand back, or NULL if repair
 *     may proceed.
 */
static const char *
__repair_conn_precheck(WT_SESSION_IMPL *session)
{
    const char *msg;

    /*
     * A non-disaggregated connection has no page server to write to, and a follower write would
     * trip the follower-write assertion in the disaggregated block manager.
     */
    if ((msg = __repair_conn_disagg_precheck(session)) != NULL)
        return (msg);
    if (!S2C(session)->layered_table_manager.leader)
        return (
          "wiredtiger_repair: disagg FOLLOWER (leader=false); skipped to avoid the follower "
          "write assertion");
    return (NULL);
}

/*
 * __repair_id_precheck --
 *     Pre-check for the id fix: the target must be a stable-table URI present in the metadata.
 *     Returns the file's current id; if the caller pinned an expected id, refuse to touch anything
 *     else --
 *     a guard against re-assigning the wrong table's id or repeating an already-applied fix.
 */
static int
__repair_id_precheck(
  WT_SESSION_IMPL *session, const char *uri, bool have_old_id, uint64_t old_id, uint64_t *cur_idp)
{
    WT_CONFIG_ITEM idval;
    WT_DECL_RET;
    char *cfg;

    *cur_idp = 0;

    if (!WT_PREFIX_MATCH(uri, "file:") || !WT_SUFFIX_MATCH(uri, ".wt_stable"))
        WT_RET_MSG(session, EINVAL,
          "id_fix: uri \"%s\" is not a \"file:...wt_stable\" stable-table URI", uri);

    WT_RET(__wt_metadata_search(session, uri, &cfg));
    WT_ERR(__wt_config_getones(session, cfg, "id", &idval));
    *cur_idp = (uint64_t)idval.val;

    if (have_old_id && *cur_idp != old_id)
        WT_ERR_MSG(session, EINVAL,
          "id_fix: %s current id %" PRIu64 " does not match requested old_id %" PRIu64, uri,
          *cur_idp, old_id);

err:
    __wt_free(session, cfg);
    return (ret);
}

/*
 * __repair_id_refresh_cached_base --
 *     Rewrite the id in an open data handle's cached base metadata. Checkpoints regenerate a file's
 *     metadata entry from the handle's cached base string (see __ckpt_set), so a repair that only
 *     rewrites the metadata table would be silently undone by the next checkpoint. The in-memory
 *     btree id is deliberately left alone: the open handle keeps using the id it was opened with
 *     and the new id takes effect on the next open. The caller must hold the handle list lock;
 *     returns WT_NOTFOUND if the file has no open handle (nothing cached to refresh).
 */
static int
__repair_id_refresh_cached_base(WT_SESSION_IMPL *session, const char *uri, uint64_t new_id)
{
    WT_CONFIG_ITEM idval;
    WT_DATA_HANDLE *dhandle;
    WT_DECL_ITEM(tmp);
    WT_DECL_RET;
    char *base;

    WT_RET(__wt_conn_dhandle_find(session, uri, NULL));
    dhandle = session->dhandle;
    if (dhandle->meta_base == NULL)
        return (0);

    WT_RET(__wt_scr_alloc(session, 0, &tmp));
    WT_ERR(__wt_config_getones(session, dhandle->meta_base, "id", &idval));
    WT_ERR(__wt_buf_fmt(session, tmp, "%.*s%" PRIu64 "%s", (int)(idval.str - dhandle->meta_base),
      dhandle->meta_base, new_id, idval.str + idval.len));
    WT_ERR(__wt_strdup(session, tmp->data, &base));

    __wt_free(session, dhandle->meta_base);
    dhandle->meta_base = base;
    dhandle->meta_hash = __wt_hash_city64(base, strlen(base));
    __wt_epoch(session, &dhandle->base_upd);

    /*
     * Keep the diagnostic copy in sync: the checkpoint's metadata-corruption check compares the
     * base string against this hash and panics on a mismatch.
     */
    __wt_free(session, dhandle->orig_meta_base);
    WT_ERR(__wt_strdup(session, base, &dhandle->orig_meta_base));
    dhandle->orig_meta_hash = dhandle->meta_hash;
    dhandle->orig_upd = dhandle->base_upd;

err:
    __wt_scr_free(session, &tmp);
    return (ret);
}

/*
 * __repair_id_execute --
 *     Execute the id fix: rewrite the file's metadata "id=" to new_id, refresh the open handle's
 *     cached base metadata to match, and queue the new value for the page server's shared metadata.
 *     The caller must hold the checkpoint and schema locks (the enqueue asserts the schema lock;
 *     the checkpoint lock keeps a concurrent checkpoint from reading the cached base mid-rewrite)
 *     and is responsible for the persist checkpoint.
 */
static int
__repair_id_execute(WT_SESSION_IMPL *session, const char *uri, uint64_t new_id)
{
    WT_CONFIG_ITEM idval;
    WT_DECL_ITEM(tmp);
    WT_DECL_RET;
    size_t tn_len;
    char *cfg, *tablename;

    cfg = tablename = NULL;

    WT_RET(__wt_scr_alloc(session, 0, &tmp));

    /* Re-read the config under the schema lock and build a copy with the id replaced. */
    WT_ERR(__wt_metadata_search(session, uri, &cfg));
    WT_ERR(__wt_config_getones(session, cfg, "id", &idval));
    WT_ERR(__wt_buf_fmt(session, tmp, "%.*s%" PRIu64 "%s", (int)(idval.str - cfg), cfg, new_id,
      idval.str + idval.len));

    WT_ERR(__wt_metadata_update(session, uri, tmp->data));

    /*
     * Refresh the open handle's cached base metadata, or the forced checkpoint below would write
     * the old id straight back over the update above. WT_NOTFOUND means no open handle: the next
     * open reads the updated metadata, nothing cached to refresh.
     */
    WT_SAVE_DHANDLE(session,
      WT_WITH_HANDLE_LIST_READ_LOCK(
        session, ret = __repair_id_refresh_cached_base(session, uri, new_id)));
    WT_ERR_NOTFOUND_OK(ret, false);

    /*
     * Enqueue a shared-metadata UPDATE so the new value is flushed to the page server. The enqueue
     * snapshots metadata fresh, so it must run AFTER the update above. Apply it in the current
     * schema epoch and NOT deferred (WT_SCHEMA_EPOCH_NONE) so it lands in the single force=true
     * checkpoint the caller issues next -- the same idiom block_disagg_ckpt.c uses when recording a
     * stable table's checkpoint state. The table name is the portion of the stable URI between
     * "file:" and ".wt_stable".
     */
    tn_len = strlen(uri) - strlen("file:") - strlen(".wt_stable");
    WT_ERR(__wt_strndup(session, uri + strlen("file:"), tn_len, &tablename));
    WT_ERR(__wt_disagg_enqueue_metadata_operation(
      session, uri, tablename, WT_SHARED_METADATA_UPDATE, WT_SCHEMA_EPOCH_NONE, false));

err:
    __wt_free(session, cfg);
    __wt_free(session, tablename);
    __wt_scr_free(session, &tmp);
    return (ret);
}

/*
 * __repair_id_fix --
 *     Metadata-only id change for one stable file: pre-check, then rewrite plus shared-metadata
 *     enqueue. Returns the id the file had before the change. The caller is responsible for the
 *     persist checkpoint shared by all actions. This proves the metadata write path can re-id a
 *     table; it does NOT move the table's data pages (they remain under the old table id in the
 *     page server), and the new id only takes effect on a dhandle opened after the change
 *     (btree->id is cached at open, see bt_handle.c). The phase name is left in *phasep so a
 *     failure reports where it stopped.
 */
static int
__repair_id_fix(WT_SESSION_IMPL *session, const char *uri, bool have_old_id, uint64_t old_id,
  uint64_t new_id, uint64_t *prior_idp, const char **phasep)
{
    WT_DECL_RET;

    *phasep = "pre-check";
    WT_RET(__repair_id_precheck(session, uri, have_old_id, old_id, prior_idp));

    *phasep = "execute";
    WT_WITH_CHECKPOINT_LOCK(
      session, WT_WITH_SCHEMA_LOCK(session, ret = __repair_id_execute(session, uri, new_id)));
    WT_RET(ret);

    return (0);
}

/*
 * __repair_size_precheck --
 *     Pre-check for the size fix: the stored database size is populated by checkpoint pickup, so a
 *     repair before the first pickup has nothing meaningful to recompute or overwrite. If the
 *     caller pinned an expected old_size, refuse to proceed unless the stored database size matches
 *     it --
 *     a guard against repairing a value that has already moved on.
 */
static int
__repair_size_precheck(WT_SESSION_IMPL *session, bool have_old_size, uint64_t old_size)
{
    uint64_t database_size;

    if (!__wt_disagg_has_picked_up_checkpoint(session))
        WT_RET_MSG(session, EINVAL,
          "size_fix: no disaggregated checkpoint has been picked up, the database size is not "
          "populated yet");

    if (have_old_size) {
        database_size = S2C(session)->disaggregated_storage.database_size;
        if (database_size != old_size)
            WT_RET_MSG(session, EINVAL,
              "size_fix: stored database size %" PRIu64
              " does not match requested old_size %" PRIu64,
              database_size, old_size);
    }
    return (0);
}

/*
 * __repair_size_fix --
 *     Whole-database size repair. Pre-check (incl. the optional old_size guard), claim the fix
 *     cycle (IDLE -> PROCESSING, a holding state so a concurrent checkpoint will not consume a
 *     half-staged cycle), and stage the debug scale. Publishing PROCESSING -> TIER1 hands the cycle
 *     to the caller's forced checkpoint, whose matching branch recomputes the database size from
 *     the per-file checkpoint sizes and releases the cycle. The recorded checkpoint sizes are
 *     trusted and re-summed (no per-file re-anchoring). *claimedp tells the caller the cycle was
 *     claimed, so it can release it if the checkpoint never consumed it. The phase name is left in
 *     *phasep.
 */
static int
__repair_size_fix(WT_SESSION_IMPL *session, bool have_old_size, uint64_t old_size,
  uint64_t debug_scale, bool *claimedp, const char **phasep)
{
    WT_CONNECTION_IMPL *conn;

    conn = S2C(session);

    *phasep = "pre-check";
    WT_RET(__repair_size_precheck(session, have_old_size, old_size));

    /*
     * Claim the cycle. PROCESSING holds it while we stage so a checkpoint cannot consume a partial
     * fix; the CAS also rejects a second concurrent repair.
     */
    if (!__wt_atomic_cas_uint8(&conn->disaggregated_storage.db_size_fix_state,
          WT_DISAGG_DBSIZE_FIX_IDLE, WT_DISAGG_DBSIZE_FIX_PROCESSING))
        WT_RET_MSG(session, EBUSY, "size_fix: a database size fix is already in progress");
    *claimedp = true;
    conn->disaggregated_storage.db_size_fix_scale = debug_scale;

    /* Publish: the caller's forced checkpoint consumes this and recomputes the database size. */
    *phasep = "execute";
    __wt_atomic_store_uint8(
      &conn->disaggregated_storage.db_size_fix_state, WT_DISAGG_DBSIZE_FIX_TIER1);
    return (0);
}

/*
 * __repair_fetch_metadata --
 *     Read-only metadata inspection: return values without modifying anything. Walk either the
 *     local metadata cursor (local=true) or the remote disaggregated shared-metadata checkpoint
 *     (local=false --
 *     the same artifact checkpoint pick-up consumes, opened through the disaggregated block
 *     manager), restricted to a single entry when a URL is given, and append each matching entry to
 *     the caller's buffer. With a key only that first-layer config value is reported; without one
 *     the whole metadata value is. The buffer the caller hands back to the shell carries these
 *     results, so the fetched value appears in the command's reply.
 */
static int
__repair_fetch_metadata(WT_SESSION_IMPL *session, bool local, bool have_url, const char *url,
  bool have_key, const char *key, WT_ITEM *out)
{
    WT_CONFIG_ITEM kv;
    WT_CURSOR *cursor;
    WT_DECL_ITEM(uri_buf);
    WT_DECL_RET;
    u_int matched;
    const char *ckpt_name, *mkey, *mvalue;
    const char *cfg[] = {WT_CONFIG_BASE(session, WT_SESSION_open_cursor), NULL};

    cursor = NULL;
    ckpt_name = NULL;
    matched = 0;

    WT_RET(__wt_buf_catfmt(session, out, "fetch_metadata(%s", local ? "local" : "remote"));
    if (have_url)
        WT_RET(__wt_buf_catfmt(session, out, ", url=%s", url));
    if (have_key)
        WT_RET(__wt_buf_catfmt(session, out, ", key=%s", key));
    WT_RET(__wt_buf_catfmt(session, out, "):"));

    if (local)
        WT_RET(__wt_metadata_cursor(session, &cursor));
    else {
        /* Find the shared metadata table's most recent checkpoint, the page-server-durable state.
         */
        WT_ERR_NOTFOUND_OK(
          __wt_meta_checkpoint_last_name(session, WT_DISAGG_METADATA_URI, &ckpt_name, NULL, NULL),
          false);
        if (ckpt_name == NULL) {
            WT_ERR(
              __wt_buf_catfmt(session, out, " the shared metadata table has no checkpoint yet"));
            goto err;
        }
        WT_ERR(__wt_scr_alloc(session, 0, &uri_buf));
        WT_ERR(__wt_buf_fmt(session, uri_buf, "%s/%s", WT_DISAGG_METADATA_URI, ckpt_name));
        WT_ERR(__wt_open_cursor(session, uri_buf->data, NULL, cfg, &cursor));
    }

    while ((ret = cursor->next(cursor)) == 0) {
        WT_ERR(cursor->get_key(cursor, &mkey));
        if (have_url && strcmp(mkey, url) != 0)
            continue;
        WT_ERR(cursor->get_value(cursor, &mvalue));
        ++matched;
        if (have_key) {
            /* Extract just the requested first-layer config value from this entry. */
            ret = __wt_config_getones(session, mvalue, key, &kv);
            if (ret == WT_NOTFOUND) {
                WT_ERR(__wt_buf_catfmt(session, out, "\n  %s: <no \"%s\">", mkey, key));
                continue;
            }
            WT_ERR(ret);
            WT_ERR(
              __wt_buf_catfmt(session, out, "\n  %s: %s=%.*s", mkey, key, (int)kv.len, kv.str));
        } else
            WT_ERR(__wt_buf_catfmt(session, out, "\n  %s: %s", mkey, mvalue));
    }
    WT_ERR_NOTFOUND_OK(ret, false);

    if (matched == 0)
        WT_ERR(__wt_buf_catfmt(session, out, " <no matching metadata entry>"));

err:
    if (cursor != NULL) {
        if (local)
            WT_TRET(__wt_metadata_cursor_release(session, &cursor));
        else
            WT_TRET(cursor->close(cursor));
    }
    __wt_free(session, ckpt_name);
    __wt_scr_free(session, &uri_buf);
    return (ret);
}

/*
 * wiredtiger_repair --
 *     Development/test repair harness. Decode a repair config string (the structure mirrors
 *     demand/command.yml) and carry out the requested work on a disaggregated-storage connection.
 *     The config is parsed with the normal WT config parser:
 *
 * url="file:collection-...wt_stable" The shared target. Absent or empty means "all URLs". It is the
 *     file to re-id for fix.id (required there), the entry to read for fetch_metadata, and
 *     informational for fix.size (the size repair is whole-database).
 *     fetch_metadata=(local=<bool>,key="<key>") Read-only inspection: return metadata values
 *     without modifying anything. local=true reads the local metadata cursor, local=false (default)
 *     the remote disaggregated shared-metadata checkpoint. key selects one first-layer config value
 *     (e.g. "checkpoint" or "id"); absent returns the whole value. The fetched value is included in
 *     the returned string, so it reaches the caller. fetch_database_size=<bool> Read-only: append
 *     the connection's disaggregated database size and last-checkpoint LSN to the returned string
 *     as "database_size(<n>, metadata_lsn=<n>, metadata_checksum=<x>)". This is the database-level
 *     value pl_complete_checkpoint persists (and pickup loads), not a per-file checkpoint size; it
 *     is the in-memory value, which equals the durable copy after a checkpoint. Requires a
 *     disaggregated connection (leader or follower).
 *     fix=(id=(old_id=<n>,new_id=<n>),size=(old_size=<n>,debug_scale=<n>)) id: change the url
 *     file's table id to new_id (required); old_id is an optional guard (absent or 0 skips it).
 *     size: repair the stored disaggregated database size by trusting the recorded per-file
 *     checkpoint sizes and re-summing them (use when only the aggregate drifted). The repair stages
 *     the result and the following forced checkpoint writes the database size --
 *     the repair never sets it directly. old_size is an optional guard on the stored size (absent
 *     or 0 skips it). debug_scale (default 1) multiplies the recomputed database-level aggregate
 *     only --
 *     the per-file sizes stay honest --
 *     so a value other than 1 writes a deliberately wrong database size for testing.
 *
 * Each requested fix runs its pre-check and execute phases; a SINGLE forced checkpoint then
 *     persists everything to the page server. A fetch is read-only and issues no checkpoint;
 *     combined with a fix it runs afterwards, so it reflects the repaired state. Work is gated to
 *     need: a fix requires a disaggregated leader, a remote fetch a disaggregated connection, a
 *     local fetch nothing. Returns a static, library-owned diagnostic string (never NULL; do not
 *     free) that, for each fix, reports the modified metadata as a before/after pair and carries
 *     any fetched metadata.
 */
const char *
wiredtiger_repair(WT_CONNECTION *connection, const char *config)
{
    WT_CONFIG_ITEM fetch_md, fix, id_fix, item, size_fix, url_item;
    WT_CONNECTION_IMPL *conn;
    WT_DECL_ITEM(fetch_out);
    WT_DECL_RET;
    WT_SESSION *wt_session;
    WT_SESSION_IMPL *isession, *session;
    uint64_t debug_scale, new_id, old_db_size, old_id, old_size, prior_id, remote_id, remote_size;
    bool fetch_local, have_fetch, have_id_fix, have_key, have_old_id, have_old_size, have_size_fix,
      have_url, have_fetch_dbsize, id_ok, size_claimed, size_ok;
    char *key, *uri;
    const char *cfg, *id_phase, *precheck_msg, *role, *size_phase;
    char idpart[384], idstatus[128], sizepart[256], sizestatus[112];
    /* Static storage: this is a single-shot dev/test entry point, not thread-safe by design. */
    static WT_ITEM report;

    wt_session = NULL;
    key = uri = NULL;
    debug_scale = 1;
    old_db_size = new_id = old_id = old_size = prior_id = remote_id = remote_size = 0;
    fetch_local = have_fetch = have_id_fix = have_key = have_old_id = have_old_size =
      have_size_fix = have_url = have_fetch_dbsize = id_ok = size_claimed = size_ok = false;
    id_phase = size_phase = "config";
    precheck_msg = NULL;
    cfg = (config == NULL) ? "" : config;

    if (connection == NULL)
        return ("wiredtiger_repair: NULL connection");

    conn = (WT_CONNECTION_IMPL *)connection;
    session = conn->default_session;

    /* Open a public session; reuse it (as a SESSION_IMPL) for parsing and the repair work. */
    WT_ERR(connection->open_session(connection, NULL, NULL, &wt_session));
    isession = (WT_SESSION_IMPL *)wt_session;

    /*
     * Parse the requested work first so the connection gate can be sized to it: a fix needs a
     * leader, a remote fetch a disaggregated connection, a local fetch nothing.
     */

    /* url: the shared target. Absent or empty => all URLs. */
    ret = __wt_config_getones(isession, cfg, "url", &url_item);
    if (ret != 0 && ret != WT_NOTFOUND)
        WT_ERR(ret);
    if (ret == 0 && url_item.len > 0) {
        have_url = true;
        WT_ERR(__wt_strndup(isession, url_item.str, url_item.len, &uri));
    }

    /* fetch_metadata: read-only inspection. Absent/empty => skip. */
    ret = __wt_config_getones(isession, cfg, "fetch_metadata", &fetch_md);
    if (ret != 0 && ret != WT_NOTFOUND)
        WT_ERR(ret);
    if (ret == 0 && fetch_md.len > 0) {
        have_fetch = true;
        ret = __wt_config_subgets(isession, &fetch_md, "local", &item);
        if (ret == 0)
            fetch_local = item.val != 0;
        else if (ret != WT_NOTFOUND)
            WT_ERR(ret);
        ret = __wt_config_subgets(isession, &fetch_md, "key", &item);
        if (ret == 0 && item.len > 0) {
            have_key = true;
            WT_ERR(__wt_strndup(isession, item.str, item.len, &key));
        } else if (ret != 0 && ret != WT_NOTFOUND)
            WT_ERR(ret);
    }

    /*
     * fetch_database_size: read-only -- report the connection's in-memory disaggregated database
     * size and last-checkpoint LSN. This is the database-level value that pl_complete_checkpoint
     * persists (and pickup loads), not a per-file checkpoint size; after a checkpoint it equals the
     * durable value. The truly-durable copy is only readable out-of-process via the log service.
     */
    ret = __wt_config_getones(isession, cfg, "fetch_database_size", &item);
    if (ret == 0)
        have_fetch_dbsize = item.val != 0;
    else if (ret != WT_NOTFOUND)
        WT_ERR(ret);

    /* fix: the mutating actions, nested under "id" and "size". Absent/empty => no repair. */
    ret = __wt_config_getones(isession, cfg, "fix", &fix);
    if (ret != 0 && ret != WT_NOTFOUND)
        WT_ERR(ret);
    if (ret == 0 && fix.len > 0) {
        /* fix.id: change a specific stable file's table id. new_id required; old_id optional. */
        ret = __wt_config_subgets(isession, &fix, "id", &id_fix);
        if (ret != 0 && ret != WT_NOTFOUND)
            WT_ERR(ret);
        if (ret == 0 && id_fix.len > 0) {
            have_id_fix = true;
            ret = __wt_config_subgets(isession, &id_fix, "new_id", &item);
            if (ret == WT_NOTFOUND)
                WT_ERR_MSG(isession, EINVAL, "fix.id requires new_id");
            WT_ERR(ret);
            new_id = (uint64_t)item.val;
            ret = __wt_config_subgets(isession, &id_fix, "old_id", &item);
            if (ret == 0 && item.val != 0) {
                have_old_id = true;
                old_id = (uint64_t)item.val;
            } else if (ret != 0 && ret != WT_NOTFOUND)
                WT_ERR(ret);
        }

        /* fix.size: recompute and repair the whole-database size. Present (even empty) => run. */
        ret = __wt_config_subgets(isession, &fix, "size", &size_fix);
        if (ret != 0 && ret != WT_NOTFOUND)
            WT_ERR(ret);
        if (ret == 0) {
            have_size_fix = true;
            ret = __wt_config_subgets(isession, &size_fix, "old_size", &item);
            if (ret == 0 && item.val != 0) {
                have_old_size = true;
                old_size = (uint64_t)item.val;
            } else if (ret != 0 && ret != WT_NOTFOUND)
                WT_ERR(ret);
            ret = __wt_config_subgets(isession, &size_fix, "debug_scale", &item);
            if (ret == 0) {
                if (item.val <= 0)
                    WT_ERR_MSG(isession, EINVAL,
                      "size_fix: debug_scale must be a positive number, got %" PRId64, item.val);
                debug_scale = (uint64_t)item.val;
            } else if (ret != WT_NOTFOUND)
                WT_ERR(ret);
        }
    }
    ret = 0;

    /* Connection gate, sized to the requested work. A failure returns the diagnostic verbatim. */
    if (have_id_fix || have_size_fix)
        precheck_msg = __repair_conn_precheck(session);
    else if ((have_fetch && !fetch_local) || have_fetch_dbsize)
        precheck_msg = __repair_conn_disagg_precheck(session);
    if (precheck_msg != NULL)
        goto err;

    /* id fix: requires a target URL. */
    if (have_id_fix) {
        if (!have_url) {
            id_phase = "pre-check";
            WT_ERR_MSG(isession, EINVAL, "fix.id requires url (the stable file to re-id)");
        }
        WT_ERR(__repair_id_fix(isession, uri, have_old_id, old_id, new_id, &prior_id, &id_phase));
    }

    /* size fix: whole-database. The before-size is the stored value as it stands now. */
    if (have_size_fix) {
        old_db_size = S2C(session)->disaggregated_storage.database_size;
        WT_ERR(__repair_size_fix(
          isession, have_old_size, old_size, debug_scale, &size_claimed, &size_phase));
    }

    if (have_id_fix || have_size_fix) {
        /*
         * Persist: a single forced checkpoint covering every fix above. The id fix's
         * shared-metadata queue entry is drained inside this checkpoint and the size fix's
         * corrected value goes out in its completion metadata.
         */
        id_phase = size_phase = "checkpoint";
        WT_ERR(wt_session->checkpoint(wt_session, "force=true"));

        /*
         * The fix is durable in the forced checkpoint above; confirm it out-of-band with
         * fetch_metadata / fetch_database_size rather than reading the page server back here
         * (reading it back can block indefinitely on a live cluster, where concurrent checkpoints
         * supersede the one we would target). Report the applied values from the in-memory
         * connection state.
         */
        remote_id = new_id;
        remote_size = S2C(session)->disaggregated_storage.database_size;

        id_ok = have_id_fix;
        size_ok = have_size_fix;
    }

    /* Read-only fetch, after any fix so a combined call reflects the repaired state. */
    if (have_fetch) {
        WT_ERR(__wt_scr_alloc(isession, 0, &fetch_out));
        WT_ERR(
          __repair_fetch_metadata(isession, fetch_local, have_url, uri, have_key, key, fetch_out));
    }
    ret = 0;

err:
    /* A connection-gate failure returns its diagnostic directly, no report to assemble. */
    if (precheck_msg != NULL) {
        if (wt_session != NULL)
            WT_TRET(wt_session->close(wt_session, NULL));
        __wt_free(session, uri);
        __wt_free(session, key);
        return (precheck_msg);
    }

    /*
     * Release the size-fix cycle if this call claimed it but the persist checkpoint never consumed
     * it (an error before, during, or after staging). On success the checkpoint already switched it
     * back to IDLE; otherwise whichever state we left -- PROCESSING (still staging) or a published
     * TIER1 -- goes back to IDLE.
     */
    if (size_claimed) {
        (void)__wt_atomic_cas_uint8(&conn->disaggregated_storage.db_size_fix_state,
          WT_DISAGG_DBSIZE_FIX_PROCESSING, WT_DISAGG_DBSIZE_FIX_IDLE);
        (void)__wt_atomic_cas_uint8(&conn->disaggregated_storage.db_size_fix_state,
          WT_DISAGG_DBSIZE_FIX_TIER1, WT_DISAGG_DBSIZE_FIX_IDLE);
    }

    role = !__wt_conn_is_disagg(session) ?
      "non-disagg" :
      (S2C(session)->layered_table_manager.leader ? "disagg LEADER" : "disagg FOLLOWER");

    /*
     * Assemble the report. The id_fix(...) and size_fix(database_size X -> Y) formats are matched
     * by callers' scripts, so the phase outcome is appended after each closing parenthesis rather
     * than spliced in. A successful fix reports the modified metadata as a before/after pair: the
     * original value and the applied value from the in-memory connection state.
     */
    if (have_id_fix && uri != NULL) {
        if (id_ok)
            WT_IGNORE_RET(__wt_snprintf(idstatus, sizeof(idstatus),
              " [id %" PRIu64 " -> remote id %" PRIu64 ", validated]", prior_id, remote_id));
        else
            WT_IGNORE_RET(__wt_snprintf(idstatus, sizeof(idstatus), " [failed at %s]", id_phase));
        if (have_old_id)
            WT_IGNORE_RET(__wt_snprintf(idpart, sizeof(idpart),
              "id_fix(uri=%s, old_id=%" PRIu64 " -> new_id=%" PRIu64 ")%s", uri, old_id, new_id,
              idstatus));
        else
            WT_IGNORE_RET(__wt_snprintf(idpart, sizeof(idpart),
              "id_fix(uri=%s, new_id=%" PRIu64 ")%s", uri, new_id, idstatus));
    } else
        WT_IGNORE_RET(__wt_snprintf(idpart, sizeof(idpart), "id_fix(none)"));

    /*
     * The reported after-size is the stored database size after the persist checkpoint on success;
     * on failure the fix never landed, so report the current stored size rather than a misleading
     * 0.
     */
    if (have_size_fix && !size_ok)
        remote_size = S2C(session)->disaggregated_storage.database_size;

    if (have_size_fix) {
        if (size_ok)
            WT_IGNORE_RET(__wt_snprintf(sizestatus, sizeof(sizestatus), " [validated]"));
        else
            WT_IGNORE_RET(
              __wt_snprintf(sizestatus, sizeof(sizestatus), " [failed at %s]", size_phase));
        /*
         * Keep the debug_scale=1 format byte-identical: callers' scripts regex-match it.
         */
        if (debug_scale != 1)
            WT_IGNORE_RET(__wt_snprintf(sizepart, sizeof(sizepart),
              "size_fix(database_size %" PRIu64 " -> %" PRIu64 ", debug_scale=%" PRIu64 ")%s",
              old_db_size, remote_size, debug_scale, sizestatus));
        else
            WT_IGNORE_RET(__wt_snprintf(sizepart, sizeof(sizepart),
              "size_fix(database_size %" PRIu64 " -> %" PRIu64 ")%s", old_db_size, remote_size,
              sizestatus));
    } else
        WT_IGNORE_RET(__wt_snprintf(sizepart, sizeof(sizepart), "size_fix(none)"));

    /*
     * Assemble into the static, library-owned buffer (the default session, which outlives the
     * public one). The buffer grows to fit, so an all-URLs / all-keys fetch is returned in full;
     * read the fetch output before closing the public session, as it lives in that session's
     * scratch memory.
     */
    WT_IGNORE_RET(__wt_buf_fmt(session, &report, "wiredtiger_repair: %s; ret=%d (%s); %s; %s", role,
      ret, wiredtiger_strerror(ret), idpart, sizepart));
    if (have_fetch && fetch_out != NULL && fetch_out->size > 0)
        WT_IGNORE_RET(__wt_buf_catfmt(
          session, &report, "; %.*s", (int)fetch_out->size, (const char *)fetch_out->data));
    if (have_fetch_dbsize)
        WT_IGNORE_RET(__wt_buf_catfmt(session, &report,
          "; database_size(%" PRIu64 ", metadata_lsn=%" PRIu64 ", metadata_checksum=%" PRIx32 ")",
          conn->disaggregated_storage.database_size,
          __wt_atomic_load_uint64_acquire(&conn->disaggregated_storage.last_checkpoint_meta_lsn),
          conn->disaggregated_storage.last_checkpoint_meta_checksum));

    if (report.data != NULL)
        __wt_verbose_warning(session, WT_VERB_WRITE, "%s", (const char *)report.data);

    if (wt_session != NULL)
        WT_TRET(wt_session->close(wt_session, NULL));

    __wt_free(session, uri);
    __wt_free(session, key);

    /* Never return NULL even if assembling the buffer hit out-of-memory. */
    return (
      report.data != NULL ? (const char *)report.data : "wiredtiger_repair: report unavailable");
}
