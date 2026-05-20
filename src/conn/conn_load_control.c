/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __conn_load_control_configure --
 *     Configure the load control constructs.
 */
static void
__conn_load_control_configure(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    WT_CONNECTION_LOAD_CONTROL *load_control;
    WT_EVICT *evict;

    uint64_t bytes_max;

    conn = S2C(session);
    bytes_max = conn->cache_size + 1;
    evict = conn->evict;

    load_control = &conn->load_control;

    /*
     * Load range is mapped to cache thresholds. Read load of 100% is mapped to eviction_trigger,
     * configured max cache fill ratio, and write load of 100% is mapped to eviction_dirty_trigger,
     * configured max dirty cache fill ratio.
     *
     * Load control subsystem will start rejecting the work based on the configured load control
     * threshold. Default load control threshold is 100%, which means load control will start
     * rejecting the work when cache fill ratio reaches eviction_trigger (i.e., 95%) for read and
     * eviction_dirty_trigger (i.e., 20%) for write.
     */

    /* Calculate max accepted for both read and write */
    __wt_atomic_store_uint64_relaxed(
      &load_control->read_load_max, (uint64_t)(bytes_max * evict->eviction_trigger / 100.0));
    __wt_atomic_store_uint64_relaxed(
      &load_control->write_load_max, (uint64_t)(bytes_max * evict->eviction_dirty_trigger / 100.0));

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

    load_control = &S2C(session)->load_control;

    WT_RET(__wt_config_gets(session, cfg, "load_control.enable", &cval));
    if (cval.val != 0)
        F_SET(load_control, WT_CONN_LOAD_CONTROL);

    /* load control threshold determines when the load control will be activated */

    WT_RET(__wt_config_gets(session, cfg, "load_control.control_threshold", &cval));
    __wt_atomic_store_uint8_relaxed(
      &load_control->control_threshold, (((uint8_t)cval.val > 200) ? 200 : (uint8_t)cval.val));

    /*
     * Load control thresholds are calculated based on the configuration settings of load control as
     * well as eviction. Hence they should be adjusted whenever the configuration of either eviction
     * or load control is changed.
     */
    __conn_load_control_configure(session);

    return (0);
}
