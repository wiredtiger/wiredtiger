/*-
 * Copyright (c) 2025-present MongoDB, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_checkpoint_reset_handle_stats --
 *     Reset handle-related stats.
 */
void
__wt_checkpoint_reset_handle_stats(WT_SESSION_IMPL *session, WT_CKPT_CONNECTION *ckpt)
{
    WT_UNUSED(session);

    ckpt->handle_stats.apply = ckpt->handle_stats.drop = ckpt->handle_stats.lock =
      ckpt->handle_stats.meta_check = ckpt->handle_stats.skip = 0;
    ckpt->handle_stats.apply_time = ckpt->handle_stats.drop_time = ckpt->handle_stats.lock_time =
      ckpt->handle_stats.meta_check_time = ckpt->handle_stats.skip_time = 0;
}
