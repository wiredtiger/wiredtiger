/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

#if 0
/*
 * __tiered_bucket_config --
 *     Return a bucket storage handle based on the configuration.
 */
static int
__tiered_bucket_config(WT_SESSION_IMPL *session, const char **cfg, WT_BUCKET_STORAGE **bstoragep)
{
    WT_BUCKET_STORAGE *bstorage;
    WT_CONFIG_ITEM bucket, cval;
    WT_DECL_RET;
    bool local_free;

    /*
     * We do not use __wt_config_gets_none for name because "none" and the empty string have
     * different meanings. The empty string means inherit the system tiered storage setting and
     * "none" means this table is not using tiered storage.
     */
    *bstoragep = NULL;
    local_free = false;
    WT_RET(__wt_config_gets(session, cfg, "tiered_storage.name", &cval));
    if (cval.len == 0)
        *bstoragep = S2C(session)->bstorage;
    else if (!WT_STRING_MATCH("none", cval.str, cval.len)) {
        WT_RET(__wt_config_gets_none(session, cfg, "tiered_storage.bucket", &bucket));
        WT_RET(__wt_tiered_bucket_config(session, &cval, &bucket, bstoragep));
        local_free = true;
        WT_ASSERT(session, *bstoragep != NULL);
    }
    bstorage = *bstoragep;
    if (bstorage != NULL) {
        /*
         * If we get here then we have a valid bucket storage entry. Now see if the config overrides
         * any of the other settings.
         */
        if (bstorage != S2C(session)->bstorage)
            WT_ERR(__wt_tiered_common_config(session, cfg, bstorage));
        WT_STAT_DATA_SET(session, tiered_object_size, bstorage->object_size);
        WT_STAT_DATA_SET(session, tiered_retention, bstorage->retain_secs);
    }
    return (0);
err:
    /* If the bucket storage was set up with copies of the strings, free them here. */
    if (bstorage != NULL && local_free && F_ISSET(bstorage, WT_BUCKET_FREE)) {
        __wt_free(session, bstorage->auth_token);
        __wt_free(session, bstorage->bucket);
        __wt_free(session, bstorage);
    }
    return (ret);
}
#endif

/*
 * __wt_tiered_tree_add --
 *     Given a tiered table, add a tiered: entry to the table and metadata. XXX No op if exists?
 *     Assume it doesn't exist?
 */
int
__wt_tiered_tree_add(WT_SESSION_IMPL *session, WT_TIERED *tiered, WT_TIERED_TREE **tiered_tree)
{
    WT_DATA_HANDLE *dhandle;
    WT_DECL_RET;
    WT_TIERED_TREE *tree;

    WT_UNUSED(session);
    WT_UNUSED(tiered);
    WT_UNUSED(tiered_tree);
    WT_UNUSED(dhandle);
    WT_UNUSED(ret);
    WT_UNUSED(tree);
    /* XXX Do we want to just return 0 here but NULL tree? */
    return (0);
}

/*
 * __wt_tiered_tree_find --
 *     Given a tiered table, find the tier: entry if one exists yet for this table.
 */
int
__wt_tiered_tree_find(WT_SESSION_IMPL *session, WT_TIERED *tiered, WT_TIERED_TREE **tiered_tree)
{
    WT_DATA_HANDLE *dhandle;
    uint32_t i;

    *tiered_tree = NULL;
    if (tiered->ntiers == 0)
        return (0);
    WT_ASSERT(session, tiered->tiers != NULL);
    for (dhandle = *tiered->tiers, i = 0; i < tiered->ntiers; ++i, ++dhandle) {
        WT_ASSERT(session, dhandle != NULL);
        if (dhandle->type == WT_DHANDLE_TYPE_TIERED_TREE) {
            *tiered_tree = (WT_TIERED_TREE *)dhandle->handle;
            return (0);
        }
    }
    /* XXX Do we want to just return 0 here but NULL tree? */
    return (WT_NOTFOUND);
}

/*
 * __tiered_switch --
 *     Given a tiered table, make all the metadata updates underneath to switch to the next object.
 *     The switch handles going from nothing to local-only, local-only to both local and shared, and
 *     having shared-only and creating a local object. Must be single threaded.
 */
static int
__tiered_switch(WT_SESSION_IMPL *session, const char *config)
{
    WT_CONNECTION_IMPL *conn;
#if 0
    WT_CURSOR *cursor;
    uint64_t cur_objnum, next_objnum;
#endif
    WT_DATA_HANDLE *dhandle;
    WT_DECL_ITEM(tmp);
    WT_DECL_RET;
    WT_TIERED *tiered;
    WT_TIERED_TREE *tiered_tree;
    const char *cfg[4] = {NULL, NULL, NULL, NULL};
    const char *objname, *tiername;
    char *objconfig;
    uint32_t orig_ntiers;
    bool free_name;

    conn = S2C(session);
    dhandle = session->dhandle;
    tiered = (WT_TIERED *)dhandle;
    __wt_errx(session, "TIER_SWITCH: called %s %s", dhandle->name, config);
    objconfig = NULL;
    objname = NULL;
    orig_ntiers = tiered->ntiers;

    __wt_errx(session, "TIER_SWITCH: tiered_tree_find");
    ret = __wt_tiered_tree_find(session, tiered, &tiered_tree);
    /* We might only have a local file tree so far. */
    WT_RET_NOTFOUND_OK(ret);
    WT_RET(__wt_scr_alloc(session, 0, &tmp));
    /*
     * The steps to switching to the next tiered file are:
     *    - Start metadata tracking.
     *    - Close the current object if needed.
     *    - Copy the current one to the cloud. It also remains in the local store if needed.
     *    - Add an object: with the name of the current local object to metadata if needed.
     *    - Update the tier: metadata if needed.
     *    - Atomically increment to get the next object number.
     *    - Set up the new file: local object.
     *    - Update the tiered: metadata to new object number and tiered array.
     *    - Stop metadata tracking to make changes real.
     *
     * Note that removal of overlapping local objects is not in the purview of this function.
     * Some other mechanism will remove outdated tiers.
     */

    WT_RET(__wt_meta_track_on(session));
    /*
     * To be implemented with flush_tier:
     *    - Close the current object.
     *    - Copy the current one to the cloud. It also remains in the local store.
     */

    /* Create the object: entry in the metadata. */
    tiername = NULL;
    free_name = false;
    __wt_errx(session, "TIER_SWITCH: tiered flags 0x%x", (int)tiered->flags);
    if (F_ISSET(tiered, WT_TIERED_LOCAL)) {
        /* This takes the current number, and makes a tiered object name of the same number. */
        WT_ERR(__wt_tiered_name(session, tiered, tiered->current_num, WT_TIERED_OBJECT, &objname));
        cfg[0] = WT_CONFIG_BASE(session, object_meta);
        cfg[1] = tiered->obj_config;
        WT_ASSERT(session, conn->tiered_prefix != NULL);
        WT_ERR(__wt_buf_fmt(session, tmp, ",bucket_prefix=%s", conn->tiered_prefix));
        cfg[2] = tmp->data;
        WT_ERR(__wt_config_merge(session, cfg, NULL, (const char **)&objconfig));
        WT_ERR(__wt_schema_create(session, objname, objconfig));
        __wt_errx(session, "TIER_SWITCH: schema create OBJECT: %s : %s", objname, objconfig);
        __wt_free(session, objconfig);
        __wt_free(session, objname);
        if (tiered_tree == NULL) {
            WT_ERR(__wt_tiered_name(session, tiered, 0, WT_TIERED_SHARED, &tiername));
            cfg[0] = WT_CONFIG_BASE(session, tier_meta);
            WT_ASSERT(session, conn->tiered_prefix != NULL);
            WT_ASSERT(session, conn->bstorage != NULL);
            WT_ERR(__wt_buf_fmt(session, tmp,
             ",bucket=%s,bucket_prefix=%s", conn->bstorage->bucket,
              conn->tiered_prefix));
            cfg[2] = tmp->data;
            WT_ERR(__wt_config_merge(session, cfg, NULL, (const char **)&objconfig));
            /* Set up a tiered: metadata for the first time. */
            __wt_errx(
              session, "TIER_SWITCH: schema create TIERED_TREE: %s : %s", tiername, objconfig);
            WT_ERR(__wt_schema_create(session, tiername, objconfig));
            __wt_free(session, objconfig);
            ++tiered->ntiers;
            free_name = true;
        } else
            tiername = tiered_tree->name;
        /* XXX Need to update in last and tiers metadata entries in tier tree no matter what. */
    }

    /*
     * Figure out what switching we need to make. In all cases we need to create a new local file.
     * If we already have a local one we move it to the shared tier. Any special cases will fall out
     * of those, such as having no objects at all or having only shared tier information.
     */
    tiered->current_num = __wt_atomic_add64(&tiered->object_num, 1);
    WT_ERR(__wt_tiered_name(session, tiered, tiered->current_num, WT_TIERED_LOCAL, &objname));
    cfg[0] = WT_CONFIG_BASE(session, object_meta);
    cfg[1] = tiered->obj_config;
    WT_ASSERT(session, conn->tiered_prefix != NULL);
    WT_ERR(__wt_buf_fmt(session, tmp, ",tiered_storage=(bucket_prefix=%s)", conn->tiered_prefix));
    cfg[2] = tmp->data;
    WT_ERR(__wt_config_merge(session, cfg, NULL, (const char **)&objconfig));
    /*
     * XXX Need to verify user doesn't create a table of the same name. What does LSM do? It
     * definitely has the same problem with chunks.
     */
    WT_ERR(__wt_schema_create(session, objname, objconfig));
    if (orig_ntiers == 0)
        ++tiered->ntiers;
    __wt_errx(session, "TIER_SWITCH: schema create LOCAL: %s : %s", objname, objconfig);
    __wt_free(session, objconfig);

    /* Potentially remove old file object */

    /* Update the tiered: metadata to new object number and tiered array. */
    cfg[0] = WT_CONFIG_BASE(session, tiered_meta);
    cfg[1] = config;
    if (tiername == NULL)
        WT_ERR(__wt_buf_fmt(session, tmp, ",tiers=(\"%s\")", objname));
    else
        WT_ERR(__wt_buf_fmt(session, tmp, ",tiers=(\"%s\", \"%s\")", objname, tiername));
    cfg[2] = tmp->data;
    WT_ERR(__wt_config_merge(session, cfg, NULL, (const char **)&objconfig));
    __wt_errx(session, "TIER_SWITCH: Update TIERED: %s %s", dhandle->name, objconfig);
    WT_ERR(__wt_metadata_update(session, dhandle->name, objconfig));

err:
    __wt_errx(session, "TIER_SWITCH: session dh %p original dh %p", (void *)session->dhandle, (void *)dhandle);
    session->dhandle = dhandle;
    __wt_errx(session, "TIER_SWITCH: DONE ret %d", ret);
    WT_RET(__wt_meta_track_off(session, true, ret != 0));
    __wt_free(session, objconfig);
    __wt_free(session, objname);
    __wt_free(session, tiername);
    if (free_name)
        __wt_free(session, tiername);
    __wt_scr_free(session, &tmp);
    return (ret);
}

/*
 * __wt_tiered_switch --
 *     Switch metadata, external version.
 */
int
__wt_tiered_switch(WT_SESSION_IMPL *session, const char *config)
{
    /*
     * For now just a wrapper to internal function.
     */
    return (__tiered_switch(session, config));
}

/*
 * __wt_tiered_name --
 *     Given a tiered table structure and object number generate the URI name of the given type. XXX
 *     Currently this is only used in this file but I anticipate it may be of use outside. If not,
 *     make this static and tiered_name instead.
 */
int
__wt_tiered_name(
  WT_SESSION_IMPL *session, WT_TIERED *tiered, uint64_t id, uint32_t type, const char **retp)
{
    WT_DECL_ITEM(tmp);
    WT_DECL_RET;
    const char *name;

    WT_RET(__wt_scr_alloc(session, 0, &tmp));
    name = tiered->iface.name;
    WT_PREFIX_SKIP_REQUIRED(session, name, "tiered:");

    /*
     * Separate object numbers from the base table name with a dash. Separate from the suffix with a
     * dot. We generate a different name style based on the type.
     */
    if (type == WT_TIERED_LOCAL) {
        WT_ERR(__wt_buf_fmt(session, tmp, "file:%s-%010" PRIu64 ".wt", name, id));
    } else if (type == WT_TIERED_OBJECT) {
        WT_ERR(__wt_buf_fmt(session, tmp, "object:%s-%010" PRIu64 ".wtobj", name, id));
    } else {
        WT_ASSERT(session, type == WT_TIERED_SHARED);
        WT_ERR(__wt_buf_fmt(session, tmp, "tier:%s", name));
    }
    WT_ERR(__wt_strndup(session, tmp->data, tmp->size, retp));
    __wt_verbose(session, WT_VERB_TIERED, "Generated tiered name: %s", *retp);
err:
    __wt_scr_free(session, &tmp);
    return (ret);
}

/*
 * __tiered_open --
 *     Open a tiered data handle (internal version).
 */
static int
__tiered_open(WT_SESSION_IMPL *session, const char *cfg[])
{
    WT_CONFIG cparser;
    WT_CONFIG_ITEM ckey, cval, tierconf;
    WT_DATA_HANDLE *dhandle;
    WT_DECL_ITEM(buf);
    WT_DECL_RET;
    WT_TIERED *tiered;
    uint32_t i, type, unused;
#if 0
    char *uri;
    uint64_t objnum;
#endif
    char *metaconf;
    const char **tiered_cfg, *config;

    dhandle = session->dhandle;
    tiered = (WT_TIERED *)dhandle;
    tiered_cfg = dhandle->cfg;
    config = NULL;

    WT_UNUSED(cfg);
    /* Collapse into one string for later use in switch. */
    WT_RET(__wt_config_merge(session, tiered_cfg, NULL, &config));

    WT_ERR(__wt_config_getones(session, config, "key_format", &cval));
    WT_ERR(__wt_strndup(session, cval.str, cval.len, &tiered->key_format));
    WT_ERR(__wt_config_getones(session, config, "value_format", &cval));
    WT_ERR(__wt_strndup(session, cval.str, cval.len, &tiered->value_format));

    ret = __wt_config_getones(session, config, "tiers", &tierconf);
    WT_ERR_NOTFOUND_OK(ret, true);

    tiered->ntiers = 0;
    /* Count the number of tiers if we have some. */
    if (ret == 0) {
        __wt_config_subinit(session, &cparser, &tierconf);
        while ((ret = __wt_config_next(&cparser, &ckey, &cval)) == 0)
            ++tiered->ntiers;
        WT_ERR_NOTFOUND_OK(ret, false);
    }

    ret = 0;
    /*
     * If we have no tiers, then we're opening and creating this for the first time. We need to
     * create an initial local file object.
     */
    __wt_errx(session, "TIERED_OPEN: open/create %s ntiers %d", dhandle->name, (int)tiered->ntiers);
    metaconf = NULL;
    if (tiered->ntiers == 0) {
        WT_ERR(__tiered_switch(session, config));
        /* XXX brute force, need to figure out functions to use to do this properly. */
        WT_ERR(__wt_metadata_search(session, dhandle->name, &metaconf));
        __wt_errx(session, "TIERED_OPEN: after switch meta conf %s %s", dhandle->name, metaconf);
        __wt_free(session, dhandle->cfg[1]);
        dhandle->cfg[1] = metaconf;
    }
    WT_ASSERT(session, tiered->ntiers != 0);
    WT_ERR(__wt_config_gets(session, dhandle->cfg, "tiers", &tierconf));
    WT_ERR(__wt_scr_alloc(session, 0, &buf));
    WT_ERR(__wt_calloc_def(session, tiered->ntiers, &tiered->tiers));

    __wt_config_subinit(session, &cparser, &tierconf);
    tiered->flags = 0;
    /*
     * Open the dhandle for each element in the tiered.tiers entry.
     *   XXX: Maybe this should be in the tiered switch function.
     */
    for (i = 0; i < tiered->ntiers; i++) {
        WT_ERR(__wt_config_next(&cparser, &ckey, &cval));
        WT_ERR(__wt_buf_fmt(session, buf, "%.*s", (int)ckey.len, ckey.str));
        __wt_verbose(session, WT_VERB_TIERED, "Open tiered URI dhandle %s", (char *)buf->data);
        WT_ERR(__wt_session_get_dhandle(session, (const char *)buf->data, NULL, cfg, 0));
        if (session->dhandle->type == WT_DHANDLE_TYPE_BTREE)
            F_SET(tiered, WT_TIERED_LOCAL);
        else if (session->dhandle->type == WT_DHANDLE_TYPE_TIERED)
            F_SET(tiered, WT_TIERED_SHARED);
        else {
            type = (uint32_t)session->dhandle->type;
            WT_TRET(__wt_session_release_dhandle(session));
            WT_ERR_MSG(session, EINVAL, "Unknown or unsupported tiered dhandle type %d", type);
        }
        (void)__wt_atomic_addi32(&session->dhandle->session_inuse, 1);
	__wt_errx(session, "TIERED_OPEN: DHANDLE %s inuse %d", session->dhandle->name, session->dhandle->session_inuse);
        /*
         * This is the ordered list of tiers in the table. The order would be approximately the
         * local file, then the shared tiered objects. There could be other items in there such as
         * an archive storage or multiple tiers to search for the data.
         */
        tiered->tiers[i] = session->dhandle;
        WT_ERR(__wt_session_release_dhandle(session));
    }
    /* Restore dhandle after modifying dhandles for the tiers. */
    session->dhandle = dhandle;
    if (0) {
        /* Temp code to keep s_all happy. */
        FLD_SET(unused, WT_TIERED_OBJ_LOCAL | WT_TIERED_TREE_UNUSED);
    }

    if (0) {
err:
        __wt_free(session, tiered->tiers);
    }
    __wt_errx(session, "TIERED_OPEN: Done ret %d dh %p", ret, (void *)session->dhandle);
    __wt_free(session, config);
    __wt_scr_free(session, &buf);
    return (ret);
}

/*
 * __wt_tiered_open --
 *     Open a tiered data handle.
 */
int
__wt_tiered_open(WT_SESSION_IMPL *session, const char *cfg[])
{
    WT_DECL_RET;

    WT_WITH_TXN_ISOLATION(session, WT_ISO_READ_UNCOMMITTED, ret = __tiered_open(session, cfg));

    return (ret);
}

/*
 * __wt_tiered_close --
 *     Close a tiered data handle.
 */
int
__wt_tiered_close(WT_SESSION_IMPL *session, WT_TIERED *tiered)
{
    WT_DECL_RET;
    WT_DATA_HANDLE *dhandle;
    u_int i;

    ret = 0;
    __wt_free(session, tiered->key_format);
    __wt_free(session, tiered->value_format);
    __wt_errx(session, "TIERED_CLOSE: have %d tiers", (int)tiered->ntiers);
    if (tiered->tiers != NULL) {
        for (i = 0; i < tiered->ntiers; i++) {
            dhandle = tiered->tiers[i];
	    WT_ASSERT(session, dhandle != NULL);
	    WT_ASSERT(session, dhandle->name != NULL);
            __wt_errx(session, "TIERED_CLOSE: DHANDLE %s inuse %d", dhandle->name, dhandle->session_inuse);
#if 0
            (void)__wt_atomic_subi32(&tiered->tiers[i]->session_inuse, 1);
#else
	    if (dhandle->session_inuse > 0)
                (void)__wt_atomic_subi32(&dhandle->session_inuse, 1);
#endif
        }
        __wt_free(session, tiered->tiers);
    }

    return (ret);
}

/*
 * __wt_tiered_tree_open --
 *     Open a tiered tree data handle.
 */
int
__wt_tiered_tree_open(WT_SESSION_IMPL *session, const char *cfg[])
{
    WT_DECL_RET;

    WT_UNUSED(session);
    WT_UNUSED(cfg);
    WT_UNUSED(ret);
    /*
     * Set dhandle->handle with tiered tree structure, initialized.
     */

    return (ret);
}

/*
 * __wt_tiered_tree_close --
 *     Close a tiered tree data handle.
 */
int
__wt_tiered_tree_close(WT_SESSION_IMPL *session, WT_TIERED_TREE *tiered_tree)
{
    WT_DECL_RET;

    ret = 0;
    __wt_free(session, tiered_tree->key_format);
    __wt_free(session, tiered_tree->value_format);

    return (ret);
}
