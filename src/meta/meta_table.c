/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __metadata_turtle --
 *     Return if a key's value should be taken from the turtle file.
 */
static bool
__metadata_turtle(const char *key)
{
    switch (key[0]) {
    case 'C':
        if (strcmp(key, WT_METADATA_COMPAT) == 0)
            return (true);
        break;
    case 'f':
        if (strcmp(key, WT_METAFILE_URI) == 0)
            return (true);
        break;
    case 'L':
        if (strcmp(key, WT_METADATA_LIVE_RESTORE) == 0)
            return (true);
        break;
    case 'W':
        if (strcmp(key, WT_METADATA_VERSION) == 0)
            return (true);
        if (strcmp(key, WT_METADATA_VERSION_STR) == 0)
            return (true);
        break;
    }
    return (false);
}

typedef struct {
    const char *full;
    const char *abbr;
    uint8_t flen;
    uint8_t alen;
} WT_METADATA_ABBREV;

#define WT_METADATA_ABBREV_ENTRY(f, a) \
    {f, a, (uint8_t)(sizeof(f) - 1), (uint8_t)(sizeof(a) - 1)}

/*
 * Stored metadata values use 1-2 byte aliases for repeated key names. The mapping is also written
 * as an abbrev: row so a dump of WiredTiger.wt shows the namesake of each alias.
 */
static const WT_METADATA_ABBREV wt_metadata_abbrev[] = {
  WT_METADATA_ABBREV_ENTRY("addr", "ad"),
  WT_METADATA_ABBREV_ENTRY("approx_leaf_pages", "ap"),
  WT_METADATA_ABBREV_ENTRY("auth_token", "at"),
  WT_METADATA_ABBREV_ENTRY("block_manager", "bm"),
  WT_METADATA_ABBREV_ENTRY("bucket", "bu"),
  WT_METADATA_ABBREV_ENTRY("bucket_prefix", "bp"),
  WT_METADATA_ABBREV_ENTRY("cache_directory", "cd"),
  WT_METADATA_ABBREV_ENTRY("checkpoint", "ch"),
  WT_METADATA_ABBREV_ENTRY("checkpoint_backup_info", "cb"),
  WT_METADATA_ABBREV_ENTRY("checkpoint_lsn", "cl"),
  WT_METADATA_ABBREV_ENTRY("columns", "cn"),
  WT_METADATA_ABBREV_ENTRY("disaggregated", "dg"),
  WT_METADATA_ABBREV_ENTRY("enabled", "en"),
  WT_METADATA_ABBREV_ENTRY("in_memory", "im"),
  WT_METADATA_ABBREV_ENTRY("ingest", "ig"),
  WT_METADATA_ABBREV_ENTRY("key_format", "kf"),
  WT_METADATA_ABBREV_ENTRY("leaf_entry_ewma", "le"),
  WT_METADATA_ABBREV_ENTRY("local_retention", "lt"),
  WT_METADATA_ABBREV_ENTRY("log", "lg"),
  WT_METADATA_ABBREV_ENTRY("major", "mj"),
  WT_METADATA_ABBREV_ENTRY("minor", "mn"),
  WT_METADATA_ABBREV_ENTRY("name", "nm"),
  WT_METADATA_ABBREV_ENTRY("newest_start_durable_ts", "nd"),
  WT_METADATA_ABBREV_ENTRY("newest_stop_durable_ts", "sd"),
  WT_METADATA_ABBREV_ENTRY("newest_stop_ts", "ns"),
  WT_METADATA_ABBREV_ENTRY("newest_stop_txn", "nq"),
  WT_METADATA_ABBREV_ENTRY("newest_txn", "nx"),
  WT_METADATA_ABBREV_ENTRY("next_page_id", "np"),
  WT_METADATA_ABBREV_ENTRY("object_target_size", "ot"),
  WT_METADATA_ABBREV_ENTRY("oldest_start_ts", "os"),
  WT_METADATA_ABBREV_ENTRY("order", "or"),
  WT_METADATA_ABBREV_ENTRY("page_log", "pl"),
  WT_METADATA_ABBREV_ENTRY("prepare", "pr"),
  WT_METADATA_ABBREV_ENTRY("run_write_gen", "rw"),
  WT_METADATA_ABBREV_ENTRY("shared", "sh"),
  WT_METADATA_ABBREV_ENTRY("size", "sz"),
  WT_METADATA_ABBREV_ENTRY("source", "so"),
  WT_METADATA_ABBREV_ENTRY("stable", "sb"),
  WT_METADATA_ABBREV_ENTRY("storage_source", "ss"),
  WT_METADATA_ABBREV_ENTRY("time", "tm"),
  WT_METADATA_ABBREV_ENTRY("tiered_storage", "ts"),
  WT_METADATA_ABBREV_ENTRY("type", "ty"),
  WT_METADATA_ABBREV_ENTRY("value_format", "vf"),
  WT_METADATA_ABBREV_ENTRY("version", "vr"),
  WT_METADATA_ABBREV_ENTRY("write_gen", "wg"),
  {NULL, NULL, 0, 0},
};

static const char *
__metadata_abbrev_of(const char *name, size_t len)
{
    const WT_METADATA_ABBREV *p;

    for (p = wt_metadata_abbrev; p->full != NULL; ++p)
        if (p->flen == len && memcmp(p->full, name, len) == 0)
            return (p->abbr);
    return (NULL);
}

static const char *
__metadata_unabbrev_of(const char *name, size_t len)
{
    const WT_METADATA_ABBREV *p;

    for (p = wt_metadata_abbrev; p->full != NULL; ++p)
        if (p->alen == len && memcmp(p->abbr, name, len) == 0)
            return (p->full);
    return (NULL);
}

/*
 * __wt_metadata_key_match_abbrev --
 *     True if the stored key is the abbreviation of the lookup name.
 */
bool
__wt_metadata_key_match_abbrev(const WT_CONFIG_ITEM *k, const char *sought, size_t slen)
{
    const char *abbr;

    abbr = __metadata_abbrev_of(sought, slen);
    return (abbr != NULL && k->len == strlen(abbr) && memcmp(k->str, abbr, k->len) == 0);
}

/*
 * __metadata_defaults --
 *     Default configuration for a metadata URI, or NULL to store the value unchanged.
 */
static const char *
__metadata_defaults(WT_SESSION_IMPL *session, const char *key)
{
    if (WT_PREFIX_MATCH(key, "file:"))
        return (WT_CONFIG_BASE(session, file_meta));
    if (WT_PREFIX_MATCH(key, "table:"))
        return (WT_CONFIG_BASE(session, table_meta));
    if (WT_PREFIX_MATCH(key, "colgroup:"))
        return (WT_CONFIG_BASE(session, colgroup_meta));
    if (WT_PREFIX_MATCH(key, "layered:"))
        return (WT_CONFIG_BASE(session, layered_meta));
    return (NULL);
}

/*
 * __metadata_value_is_default --
 *     True if the stored value matches the default, including empty vs "none".
 */
static bool
__metadata_value_is_default(const WT_CONFIG_ITEM *v, const WT_CONFIG_ITEM *dv)
{
    if (__wt_string_slice_cmp(v->str, v->len, dv->str, dv->len) == 0)
        return (true);
    if (v->len == 0 && dv->len == 4 && strncmp(dv->str, "none", 4) == 0)
        return (true);
    if (dv->len == 0 && v->len == 4 && strncmp(v->str, "none", 4) == 0)
        return (true);
    return (false);
}

/*
 * __metadata_strip_against --
 *     Drop keys whose values match defaults. Nested structs are compared key-by-key so a category
 *     still drops when it differs only by omitted keys (tiered_storage without shared).
 */
static int
__metadata_strip_against(WT_SESSION_IMPL *session, const char *value, size_t vlen,
  const char *defaults, size_t dlen, char **strippedp)
{
    WT_CONFIG cparser;
    WT_CONFIG_ITEM defitem, dv, k, v;
    WT_DECL_ITEM(tmp);
    WT_DECL_RET;
    char *nested;
    bool dropped;

    *strippedp = NULL;
    nested = NULL;
    dropped = false;

    WT_CLEAR(defitem);
    defitem.str = defaults;
    defitem.len = dlen;

    WT_RET(__wt_scr_alloc(session, 1024, &tmp));
    __wt_config_initn(session, &cparser, value, vlen);
    while ((ret = __wt_config_next(&cparser, &k, &v)) == 0) {
        ret = __wt_config_subgetraw(session, &defitem, &k, &dv);
        if (ret == 0 && __metadata_value_is_default(&v, &dv)) {
            dropped = true;
            continue;
        }
        if (ret == 0 && v.type == WT_CONFIG_ITEM_STRUCT && dv.type == WT_CONFIG_ITEM_STRUCT &&
          memchr(v.str, '=', v.len) != NULL && memchr(dv.str, '=', dv.len) != NULL) {
            WT_ERR(__metadata_strip_against(session, v.str, v.len, dv.str, dv.len, &nested));
            if (nested != NULL) {
                dropped = true;
                if (nested[0] != '\0') {
                    if (k.type == WT_CONFIG_ITEM_STRING)
                        WT_CONFIG_PRESERVE_QUOTES(session, &k);
                    WT_ERR(
                      __wt_buf_catfmt(session, tmp, "%.*s=(%s),", (int)k.len, k.str, nested));
                }
                __wt_free(session, nested);
                nested = NULL;
                continue;
            }
        }
        WT_ERR_NOTFOUND_OK(ret, false);

        if (k.type == WT_CONFIG_ITEM_STRING)
            WT_CONFIG_PRESERVE_QUOTES(session, &k);
        if (v.type == WT_CONFIG_ITEM_STRING)
            WT_CONFIG_PRESERVE_QUOTES(session, &v);
        WT_ERR(__wt_buf_catfmt(session, tmp, "%.*s=%.*s,", (int)k.len, k.str, (int)v.len, v.str));
    }
    WT_ERR_NOTFOUND_OK(ret, false);

    if (dropped) {
        if (tmp->size != 0)
            --tmp->size;
        WT_ERR(__wt_strndup(session, tmp->data, tmp->size, strippedp));
    }

err:
    __wt_free(session, nested);
    __wt_scr_free(session, &tmp);
    return (ret);
}

/*
 * __metadata_strip_defaults --
 *     Drop keys whose values match the URI type's defaults, including nested category fields.
 *     Callers must strip after collapse/merge: those rebuild from the full base string, so a
 *     create-time strip is undone on the next update.
 */
static int
__metadata_strip_defaults(
  WT_SESSION_IMPL *session, const char *key, const char *value, char **strippedp)
{
    const char *defaults;

    *strippedp = NULL;
    defaults = __metadata_defaults(session, key);
    if (defaults == NULL)
        return (0);

    return (__metadata_strip_against(
      session, value, strlen(value), defaults, strlen(defaults), strippedp));
}

/*
 * __metadata_rewrite_confign --
 *     Replace configuration key names using the metadata abbreviation table. Nested structs that
 *     contain field names are rewritten; checkpoint names such as WiredTigerCheckpoint.N are
 *     left unchanged.
 */
static int
__metadata_rewrite_confign(
  WT_SESSION_IMPL *session, const char *value, size_t len, bool to_short, char **outp)
{
    WT_CONFIG cparser;
    WT_CONFIG_ITEM k, v;
    WT_DECL_ITEM(tmp);
    WT_DECL_RET;
    char *nested;
    const char *ok;
    size_t olen;
    bool changed;

    *outp = NULL;
    nested = NULL;
    changed = false;

    WT_RET(__wt_scr_alloc(session, 512, &tmp));
    __wt_config_initn(session, &cparser, value, len);
    while ((ret = __wt_config_next(&cparser, &k, &v)) == 0) {
        ok = k.str;
        olen = k.len;
        if (to_short) {
            const char *abbr = __metadata_abbrev_of(k.str, k.len);
            if (abbr != NULL) {
                ok = abbr;
                olen = strlen(abbr);
                changed = true;
            }
        } else {
            const char *full = __metadata_unabbrev_of(k.str, k.len);
            if (full != NULL) {
                ok = full;
                olen = strlen(full);
                changed = true;
            }
        }

        if (v.type == WT_CONFIG_ITEM_STRUCT && memchr(v.str, '=', v.len) != NULL) {
            WT_ERR(__metadata_rewrite_confign(session, v.str, v.len, to_short, &nested));
            if (nested != NULL) {
                WT_ERR(__wt_buf_catfmt(session, tmp, "%.*s=(%s),", (int)olen, ok, nested));
                __wt_free(session, nested);
                nested = NULL;
                changed = true;
                continue;
            }
        }

        if (ok == k.str && k.type == WT_CONFIG_ITEM_STRING)
            WT_CONFIG_PRESERVE_QUOTES(session, &k);
        if (v.type == WT_CONFIG_ITEM_STRING)
            WT_CONFIG_PRESERVE_QUOTES(session, &v);
        if (ok == k.str) {
            ok = k.str;
            olen = k.len;
        }
        WT_ERR(__wt_buf_catfmt(session, tmp, "%.*s=%.*s,", (int)olen, ok, (int)v.len, v.str));
    }
    WT_ERR_NOTFOUND_OK(ret, false);

    if (changed) {
        if (tmp->size != 0)
            --tmp->size;
        WT_ERR(__wt_strndup(session, tmp->data, tmp->size, outp));
    }

err:
    __wt_free(session, nested);
    __wt_scr_free(session, &tmp);
    return (ret);
}

/*
 * __metadata_compact --
 *     Strip default keys, then abbreviate remaining names. Either step may be a no-op.
 */
static int
__metadata_compact(WT_SESSION_IMPL *session, const char *key, const char *value, char **storedp)
{
    WT_DECL_RET;
    char *rewritten, *stripped;
    const char *src;

    *storedp = NULL;
    rewritten = stripped = NULL;

    if (strcmp(key, WT_METADATA_ABBREV_URI) == 0)
        return (0);

    WT_RET(__metadata_strip_defaults(session, key, value, &stripped));
    src = stripped != NULL ? stripped : value;
    if (__metadata_defaults(session, key) != NULL)
        WT_ERR(__metadata_rewrite_confign(session, src, strlen(src), true, &rewritten));

    if (rewritten != NULL) {
        *storedp = rewritten;
        rewritten = NULL;
        __wt_free(session, stripped);
    } else
        *storedp = stripped;

    if (0) {
err:
        __wt_free(session, rewritten);
        __wt_free(session, stripped);
    }
    return (ret);
}

/*
 * __metadata_expand --
 *     Expand abbreviated key names for callers of metadata search. The metadata: cursor is left
 *     compact so size measurements see the stored form.
 */
static int
__metadata_expand(WT_SESSION_IMPL *session, const char *key, const char *value, char **outp)
{
    *outp = NULL;
    if (strcmp(key, WT_METADATA_ABBREV_URI) == 0 || __metadata_defaults(session, key) == NULL)
        return (0);
    return (__metadata_rewrite_confign(session, value, strlen(value), false, outp));
}

/*
 * __wt_metadata_insert_abbrev_dict --
 *     Write the long-to-short key mapping as the first metadata row.
 */
int
__wt_metadata_insert_abbrev_dict(WT_SESSION_IMPL *session)
{
    WT_DECL_ITEM(buf);
    WT_DECL_RET;
    const WT_METADATA_ABBREV *p;

    WT_RET(__wt_scr_alloc(session, 512, &buf));
    for (p = wt_metadata_abbrev; p->full != NULL; ++p)
        WT_ERR(__wt_buf_catfmt(
          session, buf, "%s%s=%s", buf->size == 0 ? "" : ",", p->full, p->abbr));
    ret = __wt_metadata_insert(session, WT_METADATA_ABBREV_URI, buf->data);
err:
    __wt_scr_free(session, &buf);
    return (ret);
}

/*
 * __wt_metadata_turtle_rewrite --
 *     Rewrite the turtle file. We wrap this because the lower functions expect a URI key and config
 *     value pair for the metadata. This function exists to push out the other contents to the
 *     turtle file such as a change in compatibility information.
 */
int
__wt_metadata_turtle_rewrite(WT_SESSION_IMPL *session)
{
    /* Require single-threading. */
    WT_ASSERT(session, FLD_ISSET(session->lock_flags, WT_SESSION_LOCKED_TURTLE));
    WT_ASSERT_SPINLOCK_OWNED(session, &S2C(session)->turtle_lock);

    char *existing_config;
    WT_RET(__wt_turtle_read(session, WT_METAFILE_URI, &existing_config));

    if (F_ISSET(S2C(session), WT_CONN_LIVE_RESTORE_FS))
        WT_RET(__wt_live_restore_turtle_update(session, WT_METAFILE_URI, existing_config, false));
    else
        WT_RET(__wt_turtle_update(session, WT_METAFILE_URI, existing_config));

    __wt_free(session, existing_config);
    return (0);
}

/*
 * __wt_metadata_cursor_open --
 *     Opens a cursor on the metadata.
 */
int
__wt_metadata_cursor_open(WT_SESSION_IMPL *session, const char *config, WT_CURSOR **cursorp)
{
    WT_BTREE *btree;
    WT_DECL_RET;
    const char *open_cursor_cfg[] = {WT_CONFIG_BASE(session, WT_SESSION_open_cursor), config, NULL};

    WT_WITHOUT_DHANDLE(
      session, ret = __wt_open_cursor(session, WT_METAFILE_URI, NULL, open_cursor_cfg, cursorp));
    WT_RET(ret);

    /*
     * Retrieve the btree from the cursor, rather than the session because we don't always switch
     * the metadata handle in to the session before entering this function.
     */
    btree = CUR2BT(*cursorp);

#define WT_EVICT_META_SKEW (10 * WT_THOUSAND)
    /*
     * Skew eviction so metadata almost always stays in cache.
     *
     * Test before setting so updates can't race in subsequent opens (the first update is safe
     * because it's single-threaded from wiredtiger_open).
     */
    if (btree->evict_priority == 0)
        WT_WITH_BTREE(session, btree, __wt_evict_priority_set(session, WT_EVICT_META_SKEW));

    return (0);
}

/*
 * __wt_metadata_cursor --
 *     Returns the session's cached metadata cursor, unless it's in use, in which case it opens and
 *     returns another metadata cursor.
 */
int
__wt_metadata_cursor(WT_SESSION_IMPL *session, WT_CURSOR **cursorp)
{
    WT_CURSOR *cursor;

    /*
     * If we don't have a cached metadata cursor, or it's already in use, we'll need to open a new
     * one.
     */
    cursor = NULL;
    if (session->meta_cursor == NULL || F_ISSET(session->meta_cursor, WT_CURSTD_META_INUSE)) {
        WT_RET(__wt_metadata_cursor_open(session, NULL, &cursor));
        if (session->meta_cursor == NULL) {
            session->meta_cursor = cursor;
            cursor = NULL;
        }
    }

    /*
     * If there's no cursor return, we're done, our caller should have just been triggering the
     * creation of the session's cached cursor. There should not be an open local cursor in that
     * case, but caution doesn't cost anything.
     */
    if (cursorp == NULL)
        return (cursor == NULL ? 0 : cursor->close(cursor));

    /*
     * If the cached cursor is in use, return the newly opened cursor, else mark the cached cursor
     * in use and return it.
     */
    if (F_ISSET(session->meta_cursor, WT_CURSTD_META_INUSE))
        *cursorp = cursor;
    else {
        *cursorp = session->meta_cursor;
        F_SET(session->meta_cursor, WT_CURSTD_META_INUSE);
    }
    return (0);
}

/*
 * __wt_metadata_cursor_close --
 *     Close a metadata cursor.
 */
int
__wt_metadata_cursor_close(WT_SESSION_IMPL *session)
{
    WT_DECL_RET;

    if (session->meta_cursor != NULL)
        ret = session->meta_cursor->close(session->meta_cursor);
    session->meta_cursor = NULL;
    return (ret);
}

/*
 * __wt_metadata_cursor_release --
 *     Release a metadata cursor.
 */
int
__wt_metadata_cursor_release(WT_SESSION_IMPL *session, WT_CURSOR **cursorp)
{
    WT_CURSOR *cursor;

    WT_UNUSED(session);

    if ((cursor = *cursorp) == NULL)
        return (0);
    *cursorp = NULL;

    /*
     * If using the session's cached metadata cursor, clear the in-use flag and reset it, otherwise,
     * discard the cursor.
     */
    if (F_ISSET(cursor, WT_CURSTD_META_INUSE)) {
        WT_ASSERT(session, cursor == session->meta_cursor);

        F_CLR(cursor, WT_CURSTD_META_INUSE);
        return (cursor->reset(cursor));
    }
    return (cursor->close(cursor));
}

/*
 * __wt_metadata_insert --
 *     Insert a row into the metadata.
 */
int
__wt_metadata_insert(WT_SESSION_IMPL *session, const char *key, const char *value)
{
    WT_CURSOR *cursor;
    WT_DECL_RET;
    char *stored_buf;
    const char *stored;

    cursor = NULL;
    stored_buf = NULL;
    stored = value;

    if (__metadata_turtle(key)) {
        __wt_verbose_debug3(session, WT_VERB_METADATA,
          "Insert: key: %s, value: %s, tracking: %s, %s"
          "turtle",
          key, value, WT_META_TRACKING(session) ? "true" : "false", "");
        WT_RET_MSG(session, EINVAL, "%s: insert not supported on the turtle file", key);
    }

    WT_ERR(__metadata_compact(session, key, value, &stored_buf));
    if (stored_buf != NULL)
        stored = stored_buf;

    __wt_verbose_debug3(session, WT_VERB_METADATA,
      "Insert: key: %s, value: %s, tracking: %s, %s"
      "turtle",
      key, stored, WT_META_TRACKING(session) ? "true" : "false",
      __metadata_turtle(key) ? "" : "not ");

    WT_ERR(__wt_metadata_cursor(session, &cursor));
    cursor->set_key(cursor, key);
    cursor->set_value(cursor, stored);
    WT_ERR(cursor->insert(cursor));
    if (WT_META_TRACKING(session))
        WT_ERR(__wti_meta_track_insert(session, key));
err:
    __wt_free(session, stored_buf);
    WT_TRET(__wt_metadata_cursor_release(session, &cursor));
    return (ret);
}

/*
 * __wt_metadata_update --
 *     Update a row in the metadata.
 */
int
__wt_metadata_update(WT_SESSION_IMPL *session, const char *key, const char *value)
{
    WT_CURSOR *cursor;
    WT_DECL_RET;
    char *stored_buf;
    const char *stored;

    cursor = NULL;
    stored_buf = NULL;
    stored = value;

    if (__metadata_turtle(key)) {
        __wt_verbose_debug3(session, WT_VERB_METADATA,
          "Update: key: %s, value: %s, tracking: %s, %s"
          "turtle",
          key, value, WT_META_TRACKING(session) ? "true" : "false", "");
        if (F_ISSET(S2C(session), WT_CONN_LIVE_RESTORE_FS))
            ret = __wt_live_restore_turtle_update(session, key, value, true);
        else
            WT_WITH_TURTLE_LOCK(session, ret = __wt_turtle_update(session, key, value));
        return (ret);
    }

    WT_ERR(__metadata_compact(session, key, value, &stored_buf));
    if (stored_buf != NULL)
        stored = stored_buf;

    __wt_verbose_debug3(session, WT_VERB_METADATA,
      "Update: key: %s, value: %s, tracking: %s, %s"
      "turtle",
      key, stored, WT_META_TRACKING(session) ? "true" : "false",
      __metadata_turtle(key) ? "" : "not ");

    if (WT_META_TRACKING(session))
        WT_ERR(__wti_meta_track_update(session, key));

    WT_ERR(__wt_metadata_cursor(session, &cursor));
    /* This cursor needs to have overwrite semantics. */
    WT_ASSERT(session, F_ISSET(cursor, WT_CURSTD_OVERWRITE));

    cursor->set_key(cursor, key);
    cursor->set_value(cursor, stored);
    WT_ERR(cursor->insert(cursor));
err:
    __wt_free(session, stored_buf);
    WT_TRET(__wt_metadata_cursor_release(session, &cursor));
    return (ret);
}

/*
 * __wt_metadata_remove --
 *     Remove a row from the metadata.
 */
int
__wt_metadata_remove(WT_SESSION_IMPL *session, const char *key)
{
    WT_CURSOR *cursor;
    WT_DECL_RET;

    __wt_verbose_debug3(session, WT_VERB_METADATA,
      "Remove: key: %s, tracking: %s, %s"
      "turtle",
      key, WT_META_TRACKING(session) ? "true" : "false", __metadata_turtle(key) ? "" : "not ");

    if (__metadata_turtle(key))
        WT_RET_MSG(session, EINVAL, "%s: remove not supported on the turtle file", key);

    /*
     * Take, release, and reacquire the metadata cursor. It's complicated, but that way the
     * underlying meta-tracking function doesn't have to open a second metadata cursor, it can use
     * the session's cached one.
     */
    WT_RET(__wt_metadata_cursor(session, &cursor));
    cursor->set_key(cursor, key);
    WT_ERR(cursor->search(cursor));
    WT_ERR(__wt_metadata_cursor_release(session, &cursor));

    if (WT_META_TRACKING(session))
        WT_ERR(__wti_meta_track_update(session, key));

    WT_ERR(__wt_metadata_cursor(session, &cursor));
    cursor->set_key(cursor, key);
    ret = cursor->remove(cursor);

err:
    WT_TRET(__wt_metadata_cursor_release(session, &cursor));
    return (ret);
}

/*
 * __wt_metadata_search --
 *     Return a copied row from the metadata. The caller is responsible for freeing the allocated
 *     memory.
 */
int
__wt_metadata_search(WT_SESSION_IMPL *session, const char *key, char **valuep)
{
    WT_CURSOR *cursor;
    WT_DECL_RET;
    const char *value;

    *valuep = NULL;

    __wt_verbose_debug3(session, WT_VERB_METADATA,
      "Search: key: %s, tracking: %s, %s"
      "turtle",
      key, WT_META_TRACKING(session) ? "true" : "false", __metadata_turtle(key) ? "" : "not ");

    if (__metadata_turtle(key)) {
        /*
         * The returned value should only be set if ret is non-zero, but Coverity is convinced
         * otherwise. The code path is used enough that Coverity complains a lot, add an error check
         * to get some peace and quiet.
         */
        if (F_ISSET(S2C(session), WT_CONN_LIVE_RESTORE_FS))
            ret = __wt_live_restore_turtle_read(session, key, valuep);
        else
            WT_WITH_TURTLE_LOCK(session, ret = __wt_turtle_read(session, key, valuep));

        if (ret != 0)
            __wt_free(session, *valuep);
        return (ret);
    }

    /*
     * All metadata reads are at read-uncommitted isolation. That's because once a schema-level
     * operation completes, subsequent operations must see the current version of checkpoint
     * metadata, or they may try to read blocks that may have been freed from a file. Metadata
     * updates use non-transactional techniques (such as the schema and metadata locks) to protect
     * access to in-flight updates.
     */
    WT_RET(__wt_metadata_cursor(session, &cursor));
    cursor->set_key(cursor, key);
    WT_WITH_TXN_ISOLATION(session, WT_ISO_READ_UNCOMMITTED, ret = cursor->search(cursor));
    WT_ERR(ret);

    WT_ERR(cursor->get_value(cursor, &value));
    WT_ERR(__metadata_expand(session, key, value, valuep));
    if (*valuep == NULL)
        WT_ERR(__wt_strdup(session, value, valuep));

err:
    WT_TRET(__wt_metadata_cursor_release(session, &cursor));

    if (ret != 0)
        __wt_free(session, *valuep);
    return (ret);
}

/*
 * __wt_metadata_btree_id_to_uri --
 *     Given a btree id, find the matching entry in the metadata and return a copy of the uri. The
 *     caller has to free the returned uri.
 */
int
__wt_metadata_btree_id_to_uri(WT_SESSION_IMPL *session, uint32_t btree_id, char **uri)
{
    WT_CONFIG_ITEM id;
    WT_CURSOR *cursor;
    WT_DECL_RET;
    char *key, *value;

    *uri = NULL;
    key = value = NULL;

    WT_RET(__wt_metadata_cursor(session, &cursor));
    while ((ret = cursor->next(cursor)) == 0) {
        WT_ERR(cursor->get_value(cursor, &value));
        if ((ret = __wt_config_getones(session, value, "id", &id)) == 0 && btree_id == id.val) {
            WT_ERR(cursor->get_key(cursor, &key));
            /* Return a copy as the uri. */
            WT_ERR(__wt_strdup(session, key, uri));
            break;
        }
        WT_ERR_NOTFOUND_OK(ret, false);
    }

err:
    WT_TRET(__wt_metadata_cursor_release(session, &cursor));
    return (ret);
}

/*
 * __btree_id_cmp --
 *     Compare two btree IDs for qsort, sorts in ascending order.
 */
static int
__btree_id_cmp(const void *a, const void *b)
{
    uint32_t id_a, id_b;

    id_a = *(const uint32_t *)a;
    id_b = *(const uint32_t *)b;
    return (id_a < id_b ? -1 : (id_a == id_b ? 0 : 1));
}

/*
 * __wt_metadata_btree_ids_find_duplicate --
 *     Sort a list of btree IDs and return whether any two of them are the same, setting the shared
 *     ID when they are.
 */
bool
__wt_metadata_btree_ids_find_duplicate(uint32_t *btree_ids, size_t count, uint32_t *dup_idp)
{
    size_t i;

    *dup_idp = 0;

    __wt_qsort(btree_ids, count, sizeof(uint32_t), __btree_id_cmp);
    for (i = 0; i + 1 < count; ++i)
        if (btree_ids[i] == btree_ids[i + 1]) {
            *dup_idp = btree_ids[i];
            return (true);
        }

    return (false);
}

/*
 * __wt_metadata_stable_uris_for_id --
 *     Find the two stable files in the metadata carrying the given btree ID, returning WT_NOTFOUND
 *     if fewer than two do. Callers only ask in order to name a conflict they have already
 *     detected, so the scan falls on a path that is already failing. Uses a private cursor: a
 *     caller failing mid-transaction may need the session's cached one to unroll.
 */
int
__wt_metadata_stable_uris_for_id(
  WT_SESSION_IMPL *session, uint32_t btree_id, char **first_urip, char **second_urip)
{
    WT_CONFIG_ITEM id_val;
    WT_CURSOR *cursor;
    WT_DECL_RET;
    const char *key, *value;

    *first_urip = *second_urip = NULL;
    cursor = NULL;

    WT_ERR(__wt_metadata_cursor_open(session, NULL, &cursor));
    while ((ret = cursor->next(cursor)) == 0) {
        WT_ERR(cursor->get_key(cursor, &key));
        if (!WT_PREFIX_MATCH(key, "file:") || !WT_URI_IS_STABLE(key))
            continue;
        WT_ERR(cursor->get_value(cursor, &value));
        WT_ERR(__wt_config_getones(session, value, "id", &id_val));
        if ((uint32_t)id_val.val != btree_id)
            continue;
        WT_ERR(__wt_strdup(session, key, *first_urip == NULL ? first_urip : second_urip));
        if (*second_urip != NULL)
            break;
    }
    WT_ERR_NOTFOUND_OK(ret, false);

    if (*second_urip == NULL)
        ret = WT_NOTFOUND;

err:
    WT_TRET(__wt_metadata_cursor_release(session, &cursor));
    if (ret != 0) {
        __wt_free(session, *first_urip);
        __wt_free(session, *second_urip);
    }
    return (ret);
}

/*
 * __wt_verbose_dump_metadata --
 *     Output diagnostic information about metadata contents.
 */
int
__wt_verbose_dump_metadata(WT_SESSION_IMPL *session)
{
    WT_CURSOR *cursor;
    WT_DECL_RET;
    const char *config, *uri;

    WT_RET(__wt_msg(session, "%s", WT_DIVIDER));
    WT_RET(__wt_msg(session, "metadata dump"));

    WT_RET(__wt_metadata_cursor(session, &cursor));
    while ((ret = cursor->next(cursor)) == 0) {
        WT_ERR(cursor->get_key(cursor, &uri));
        WT_ERR(cursor->get_value(cursor, &config));

        WT_ERR(__wt_msg(session, "uri: %s | config: %s", uri, config));
    }
    WT_ERR_NOTFOUND_OK(ret, false);

err:
    WT_TRET(__wt_metadata_cursor_release(session, &cursor));
    return (ret);
}
