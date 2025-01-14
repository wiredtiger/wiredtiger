/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 * All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

#define WTI_CHECKPOINT_SESSION_FLAGS (WT_SESSION_CAN_WAIT | WT_SESSION_IGNORE_CACHE_SIZE)
#define WTI_CKPT_FOREACH_NAME_OR_ORDER(ckptbase, ckpt) \
    for ((ckpt) = (ckptbase); (ckpt)->name != NULL || (ckpt)->order != 0; ++(ckpt))

/*
 * Inactive should always be 0. Other states are roughly ordered by appearance in the checkpoint
 * life cycle.
 */
typedef enum {
    WTI_CHECKPOINT_STATE_INACTIVE,
    WTI_CHECKPOINT_STATE_APPLY_META,
    WTI_CHECKPOINT_STATE_APPLY_BTREE,
    WTI_CHECKPOINT_STATE_UPDATE_OLDEST,
    WTI_CHECKPOINT_STATE_SYNC_FILE,
    WTI_CHECKPOINT_STATE_EVICT_FILE,
    WTI_CHECKPOINT_STATE_BM_SYNC,
    WTI_CHECKPOINT_STATE_RESOLVE,
    WTI_CHECKPOINT_STATE_POSTPROCESS,
    WTI_CHECKPOINT_STATE_HS,
    WTI_CHECKPOINT_STATE_HS_SYNC,
    WTI_CHECKPOINT_STATE_COMMIT,
    WTI_CHECKPOINT_STATE_META_CKPT,
    WTI_CHECKPOINT_STATE_META_SYNC,
    WTI_CHECKPOINT_STATE_ROLLBACK,
    WTI_CHECKPOINT_STATE_LOG,
    WTI_CHECKPOINT_STATE_CKPT_TREE,
    WTI_CHECKPOINT_STATE_ACTIVE,
    WTI_CHECKPOINT_STATE_ESTABLISH,
    WTI_CHECKPOINT_STATE_START_TXN
} WTI_CHECKPOINT_STATE;

/* DO NOT EDIT: automatically built by prototypes.py: BEGIN */

/* DO NOT EDIT: automatically built by prototypes.py: END */
