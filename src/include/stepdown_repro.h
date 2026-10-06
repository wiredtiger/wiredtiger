/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

#ifdef HAVE_DIAGNOSTIC
#define WT_REPRO_EVENT(session, event, ref, upd, context, detail) \
    (__wt_process.stepdown_repro_trace == NULL ?                  \
        0 :                                                       \
        __wt_process.stepdown_repro_trace(session, event, ref, upd, context, (uint64_t)(detail)))
#else
#define WT_REPRO_EVENT(session, event, ref, upd, context, detail) 0
#endif
