/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __conn_load_control_init --
 *     Initialize the load thresholds and limits.
 */
static void
__conn_load_control_init(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    WT_CONNECTION_LOAD_CONTROL *load_control;
    WT_EVICT *evict;

    uint64_t bytes_max, load_min, load_max, load_threshold = 0;
    uint8_t load_range;

    conn = S2C(session);
    bytes_max = conn->cache_size + 1;
    evict = conn->evict;

    load_control = conn->load_control;

    /*
     * Load range is mapped to cache thresholds (target, trigger).
     * Read load is mapped to (eviction_target, eviction_trigger) and
     * write load is mapped to (eviction_dirty_target).
     *
     * For eg:- if eviction_target is 80 and eviction_trigger is 95, then
     * Cache management will start when cache fill ratio reaches eviction_target (80%).
     * Goal of cache management is to keep the cache fill ratio under eviction_trigger (95%).
     * As cache management range is between eviction_target and eviction_trigger,
     * read load will be mapped to this range.
     * load_control_threshold specifies when the load management will be activated within a
     * a load range.
     * !!!
     * For default cache fill thresholds, target is 80 and trigger is 95.
     *    read load range is (80% - 95%), with a load control threshold of 50%,
     * read load control will start at cache fill ratio of 87.5% and
     * reaches max at cache fill ratio of 95%.
     *
     * For default cache dirty thresholds, target is 5 and trigger is 20.
     *     write load range is (5% - 20%), with a load control threshold of 50%
     * write load control will start at cache dirty fill ratio of 12.5% and
     * reaches max at cache dirty fill ratio of 20%.
     */

    /* Calculate read load activation controls */
    load_max = (uint64_t)(bytes_max * evict->eviction_trigger / 100.0);
    load_min = (uint64_t)(bytes_max * evict->eviction_target / 100.0);
    load_range = (uint8_t)(evict->eviction_trigger - evict->eviction_target);
    load_threshold = bytes_max * ((load_range * load_threshold) / 100);
    load_control->read_load_max = load_max;
    load_control->read_load_threshold = load_min + load_threshold;

    /* Calculate write load activation controls */
    load_max = (uint64_t)(bytes_max * evict->eviction_dirty_trigger / 100.0);
    load_min = (uint64_t)(bytes_max * evict->eviction_dirty_target / 100.0);
    load_range = (uint8_t)(evict->eviction_dirty_trigger - evict->eviction_dirty_target);
    load_threshold = bytes_max * ((load_range * load_threshold) / 100);
    load_control->write_load_max = load_max;
    load_control->write_load_threshold = load_min + load_threshold;

    return;
}

/*
 * __wti_conn_load_control_config --
 *     Configure or reconfigure the load control.
 */
int
__wti_conn_load_control_config(WT_SESSION_IMPL *session, const char *cfg[], bool reconfig)
{
    WT_CONFIG_ITEM cval;
    WT_CONNECTION_LOAD_CONTROL *load_control;
    WT_UNUSED(reconfig);

    load_control = S2C(session)->load_control;

    WT_RET(__wt_config_gets(session, cfg, "load_control.enable", &cval));
    if (cval.val != 0)
        F_SET(load_control, WT_CONNECTION_LOAD_CONTROL_CONTROL);

    /* load control threshold determines when the load control will be activated */

    WT_RET(__wt_config_gets(session, cfg, "load_control.control_threshold", &cval));
    __wt_atomic_store_uint8_relaxed(
      &load_control->load_control_threshold, (((uint8_t)cval.val / 10) * 10));

    /*
     * Load control thresholds are calculated based on the configuration settings of load control as
     * well as eviction. Hence they should be adjusted whenever the configuration of either eviction
     * or load control is changed.
     */
    __conn_load_control_init(session);

    return (0);
}

/*
 * __wti_conn_load_control_init --
 *     Initialize the connection load subsystem.
 */
int
__wti_conn_load_control_init(WT_SESSION_IMPL *session, const char *cfg[])
{
    WT_CONNECTION_IMPL *conn;

    conn = S2C(session);
    WT_RET(__wt_calloc_one(session, &conn->load_control));

    WT_RET(__wti_conn_load_control_config(session, cfg, false));

    return (0);
}

/*
 * __wti_conn_load_control_destroy --
 *     Destroy the connection load subsystem.
 */
void
__wti_conn_load_control_destroy(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;

    conn = S2C(session);
    if (conn->load_control != NULL)
        __wt_free(session, conn->load_control);

    return;
}
