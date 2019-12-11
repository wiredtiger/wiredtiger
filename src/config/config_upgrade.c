/*-
 * Copyright (c) 2014-2019 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_config_upgrade --
 *     Upgrade a configuration string by appended the replacement version.
 */
int
__wt_config_upgrade(WT_SESSION_IMPL *session, WT_ITEM *buf)
{
    // Currently not used and included in the func_ok list of s_void
    WT_UNUSED(buf);
    WT_UNUSED(session);
    return (0);
}
