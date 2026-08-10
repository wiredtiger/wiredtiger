/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __schema_publish_disagg_schema_epoch --
 *     Publish the object with the given schema epoch for disaggregated storage.
 */
static int
__schema_publish_disagg_schema_epoch(
  WT_SESSION_IMPL *session, const char *uri, wt_timestamp_t schema_epoch)
{
    WT_DECL_RET;
    WT_TXN_GLOBAL *txn_global;
    wt_timestamp_t stable_schema_epoch, step_down_epoch;
    char epoch_string[2][WT_TS_INT_STRING_SIZE];
    bool locked;

    WT_ASSERT_SPINLOCK_OWNED(session, &S2C(session)->schema_lock);

    locked = false;
    txn_global = &S2C(session)->txn_global;

    if (schema_epoch == WT_SCHEMA_EPOCH_NONE)
        WT_ERR_MSG(session, EINVAL, "Cannot publish with a zero schema epoch");

    if (!__wt_conn_is_disagg(session))
        WT_ERR_MSG(session, ENOTSUP,
          "Publishing based on disaggregated schema epoch is only supported for disaggregated "
          "storage");

    /* Ensure that publishing is synchronized with advancing the stable schema epoch. */
    __wt_readlock(
      session, &txn_global->rwlock); /* Lock ordering: Acquire this inside the table lock. */
    locked = true;

    stable_schema_epoch = __wt_get_stable_disaggregated_schema_epoch(session);
    if (stable_schema_epoch == WT_SCHEMA_EPOCH_NONE)
        WT_ERR_PANIC(
          session, EINVAL, "publish requires the stable disaggregated schema epoch to be set");
    if (schema_epoch <= stable_schema_epoch)
        WT_ERR_MSG(session, EINVAL,
          "Cannot publish with a schema epoch that is older than the stable disaggregated schema "
          "epoch %" PRIu64 " %s",
          stable_schema_epoch, __wt_timestamp_to_string(stable_schema_epoch, epoch_string[0]));

    /*
     * While the step-down boundary is set, everything published belongs to this leader era and must
     * land at or below the step-down epoch. Operations above the boundary belong to the next leader
     * and are published after the step-up. The rwlock held here orders the check against setting
     * the boundary.
     */
    step_down_epoch =
      __wt_atomic_load_uint64_relaxed(&txn_global->step_down_disaggregated_schema_epoch);
    if (step_down_epoch != WT_SCHEMA_EPOCH_NONE && schema_epoch > step_down_epoch)
        WT_ERR_MSG(session, EINVAL,
          "Cannot publish with a schema epoch %s that is newer than the step down disaggregated "
          "schema epoch %s",
          __wt_timestamp_to_string(schema_epoch, epoch_string[0]),
          __wt_timestamp_to_string(step_down_epoch, epoch_string[1]));

    if (WT_PREFIX_MATCH(uri, "layered:"))
        WT_ERR(__wt_disagg_shared_metadata_queue_publish(
          session, uri + strlen("layered:"), schema_epoch));
    else if (WT_PREFIX_MATCH(uri, "table:"))
        WT_ERR(
          __wt_disagg_shared_metadata_queue_publish(session, uri + strlen("table:"), schema_epoch));
    else
        WT_ERR_MSG(session, ENOTSUP,
          "Publishing based on disaggregated schema epoch is only supported for table: and "
          "layered: objects");

err:
    if (locked)
        __wt_readunlock(session, &txn_global->rwlock);
    return (ret);
}

/*
 * __wt_schema_publish --
 *     Publish an object.
 */
int
__wt_schema_publish(WT_SESSION_IMPL *session, const char *uri, const char *cfg[])
{
    WT_CONFIG_ITEM cval;
    WT_DECL_RET;
    wt_timestamp_t schema_epoch;

    WT_ASSERT_SPINLOCK_OWNED(session, &S2C(session)->schema_lock);

    WT_ASSERT_NO_SCHEMA_OP_DURING_STEP_UP(session);

    WT_ERR(__wt_config_gets(session, cfg, "disaggregated.schema_epoch", &cval));
    if (cval.len > 0) {
        WT_ERR(__wt_txn_parse_timestamp(session, "schema epoch", &schema_epoch, &cval));
        WT_ERR(__schema_publish_disagg_schema_epoch(session, uri, schema_epoch));
    }

err:
    return (ret);
}
