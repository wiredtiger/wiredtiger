/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __tiered_confchk --
 *     Check for a valid tiered storage source.
 */
static int
__tiered_confchk(
  WT_SESSION_IMPL *session, WT_CONFIG_ITEM *name, WT_NAMED_STORAGE_SOURCE **nstoragep)
{
    WT_CONNECTION_IMPL *conn;
    WT_NAMED_STORAGE_SOURCE *nstorage;

    *nstoragep = NULL;

    if (name->len == 0 || WT_STRING_MATCH("none", name->str, name->len))
        return (0);

    conn = S2C(session);
    TAILQ_FOREACH (nstorage, &conn->storagesrcqh, q)
        if (WT_STRING_MATCH(nstorage->name, name->str, name->len)) {
            *nstoragep = nstorage;
            return (0);
        }
    WT_RET_MSG(session, EINVAL, "unknown storage source '%.*s'", (int)name->len, name->str);
}

/*
 * __tiered_common_config --
 *     Parse configuration options common to connection and btrees.
 */
static int
__tiered_common_config(WT_SESSION_IMPL *session, const char **cfg, WT_BUCKET_STORAGE *bstorage)
{
    WT_CONFIG_ITEM cval;

    WT_RET(__wt_config_gets(session, cfg, "tiered_storage.local_retention", &cval));
    bstorage->retain_secs = (uint64_t)cval.val;

    WT_RET(__wt_config_gets(session, cfg, "tiered_storage.object_target_size", &cval));
    bstorage->object_size = (uint64_t)cval.val;

    WT_RET(__wt_config_gets(session, cfg, "tiered_storage.auth_token", &cval));
    /*
     * This call is purposely the last configuration processed so we don't need memory management
     * code and an error label to free it. Note this if any code is added after this line.
     */
    WT_RET(__wt_strndup(session, cval.str, cval.len, &bstorage->auth_token));
    return (0);
}

/*
 * __wt_tiered_bucket_config --
 *     Given a configuration, (re)configure the bucket storage and return that structure.
 */
int
__wt_tiered_bucket_config(
  WT_SESSION_IMPL *session, const char *cfg[], WT_BUCKET_STORAGE **bstoragep)
{
    WT_BUCKET_STORAGE *bstorage, *new;
    WT_CONFIG_ITEM bucket, name, prefix;
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_NAMED_STORAGE_SOURCE *nstorage;
#if 0
    WT_STORAGE_SOURCE *custom, *storage;
#else
    WT_STORAGE_SOURCE *storage;
#endif
    uint64_t hash_bucket, hash;

    *bstoragep = NULL;

    WT_RET(__wt_config_gets(session, cfg, "tiered_storage.name", &name));
    bstorage = new = NULL;
    conn = S2C(session);

    __wt_spin_lock(session, &conn->storage_lock);

    WT_ERR(__tiered_confchk(session, &name, &nstorage));
    if (nstorage == NULL) {
        WT_ERR(__wt_config_gets(session, cfg, "tiered_storage.bucket", &bucket));
        if (bucket.len != 0)
            WT_ERR_MSG(
              session, EINVAL, "tiered_storage.bucket requires tiered_storage.name to be set");
        goto done;
    }
    /*
     * Check if tiered storage is set on the connection. If someone wants tiered storage on a table,
     * it needs to be configured on the database as well.
     */
    if (conn->bstorage == NULL && bstoragep != &conn->bstorage)
        WT_ERR_MSG(
          session, EINVAL, "table tiered storage requires connection tiered storage to be set");
    /* A bucket and bucket_prefix are required. */
    WT_ERR(__wt_config_gets(session, cfg, "tiered_storage.bucket", &bucket));
    if (bucket.len == 0)
        WT_ERR_MSG(session, EINVAL, "table tiered storage requires bucket to be set");
    WT_ERR(__wt_config_gets(session, cfg, "tiered_storage.bucket_prefix", &prefix));
    if (prefix.len == 0)
        WT_ERR_MSG(session, EINVAL, "table tiered storage requires bucket_prefix to be set");

    hash = __wt_hash_city64(bucket.str, bucket.len);
    hash_bucket = hash & (conn->hash_size - 1);
    TAILQ_FOREACH (bstorage, &nstorage->buckethashqh[hash_bucket], q) {
        if (WT_STRING_MATCH(bstorage->bucket, bucket.str, bucket.len) &&
          (WT_STRING_MATCH(bstorage->bucket_prefix, prefix.str, prefix.len))) {
            *bstoragep = bstorage;
            goto done;
        }
    }

    WT_ERR(__wt_calloc_one(session, &new));
    WT_ERR(__wt_strndup(session, bucket.str, bucket.len, &new->bucket));
    WT_ERR(__wt_strndup(session, prefix.str, prefix.len, &new->bucket_prefix));
    storage = nstorage->storage_source;
#if 0
    if (storage->customize != NULL) {
        custom = NULL;
        WT_ERR(storage->customize(storage, &session->iface, cfg_arg, &custom));
        if (custom != NULL) {
            bstorage->owned = 1;
            storage = custom;
        }
    }
#endif
    new->storage_source = storage;
    /* If we're creating a new bucket storage, parse the other settings into it.  */
    TAILQ_INSERT_HEAD(&nstorage->bucketqh, new, q);
    TAILQ_INSERT_HEAD(&nstorage->buckethashqh[hash_bucket], new, hashq);
    F_SET(new, WT_BUCKET_FREE);
    WT_ERR(__tiered_common_config(session, cfg, new));
    *bstoragep = new;

done:
err:
    __wt_spin_unlock(session, &conn->storage_lock);
    return (ret);
}

/*
 * __wt_tiered_conn_config --
 *     Parse and setup the storage server options for the connection.
 */
int
__wt_tiered_conn_config(WT_SESSION_IMPL *session, const char **cfg, bool reconfig)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;

    conn = S2C(session);

    if (!reconfig)
        WT_RET(__wt_tiered_bucket_config(session, cfg, &conn->bstorage));

    /* If the connection is not set up for tiered storage there is nothing more to do. */
    if (conn->bstorage == NULL)
        return (0);
    __wt_verbose(session, WT_VERB_TIERED, "TIERED_CONFIG: bucket %s", conn->bstorage->bucket);
    __wt_verbose(
      session, WT_VERB_TIERED, "TIERED_CONFIG: prefix %s", conn->bstorage->bucket_prefix);

    /*
     * If reconfiguring, see if the other settings have changed on the system bucket storage.
     */
    WT_ASSERT(session, conn->bstorage != NULL);
    if (reconfig)
        WT_ERR(__tiered_common_config(session, cfg, conn->bstorage));

    WT_STAT_CONN_SET(session, tiered_object_size, conn->bstorage->object_size);
    WT_STAT_CONN_SET(session, tiered_retention, conn->bstorage->retain_secs);

    return (0);

err:
    __wt_free(session, conn->bstorage->auth_token);
    __wt_free(session, conn->bstorage->bucket);
    __wt_free(session, conn->bstorage->bucket_prefix);
    __wt_free(session, conn->bstorage);
    return (ret);
}
