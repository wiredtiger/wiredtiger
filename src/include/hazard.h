/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * WT_HAZARD_COOKIE --
 *   State passed through to callbacks during the session walk logic when
 *   looking for active hazard pointers.
 */
struct __wt_hazard_cookie {
    WT_HAZARD *ret_hp;
    WT_SESSION_IMPL *original_session;
    uint32_t walk_cnt;
    uint32_t max;
    WT_SESSION_IMPL **session_ret;
    WT_REF *search_ref;
};
