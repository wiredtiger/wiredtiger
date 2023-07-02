/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * Don't hijack the session checkpoint thread for eviction.
 *
 * Application threads are not generally available for potentially slow operations, but checkpoint
 * does enough I/O it may be called upon to perform slow operations for the block manager.
 *
 * Application checkpoints wait until the checkpoint lock is available, compaction checkpoints
 * don't.
 *
 * Checkpoints should always use a separate session for history store updates, otherwise those
 * updates are pinned until the checkpoint commits. Also, there are unfortunate interactions between
 * the special rules for history store eviction and the special handling of the checkpoint
 * transaction.
 */
#define WT_CHECKPOINT_SESSION_FLAGS (WT_SESSION_CAN_WAIT | WT_SESSION_IGNORE_CACHE_SIZE)

/*
 * TODO enum this? TODO explain that i added the state to stuff that needs to do counting outside
 * our subsystem - don't want to make other stuff pay. TODO revise all "txn_checkpoint_*" stats -
 * move to checkpoint stats? TODO WT_STAT_SET -> WT_STAT_CONN_SET?
 */
#define WT_CHECKPOINT_STATE_INACTIVE 0
#define WT_CHECKPOINT_STATE_RESET_CURSORS 1
#define WT_CHECKPOINT_STATE_FLUSH_TIER_WAIT 2
#define WT_CHECKPOINT_STATE_APPLY_META 3
#define WT_CHECKPOINT_STATE_APPLY_BTREE 4
#define WT_CHECKPOINT_STATE_UPDATE_OLDEST 5
#define WT_CHECKPOINT_STATE_SYNC_FILE 6
#define WT_CHECKPOINT_STATE_EVICT_FILE 7
#define WT_CHECKPOINT_STATE_BM_SYNC 8
#define WT_CHECKPOINT_STATE_RESOLVE 9
#define WT_CHECKPOINT_STATE_POSTPROCESS 10
#define WT_CHECKPOINT_STATE_HS 11
#define WT_CHECKPOINT_STATE_HS_SYNC 12
#define WT_CHECKPOINT_STATE_COMMIT 13
#define WT_CHECKPOINT_STATE_META_CKPT 14
#define WT_CHECKPOINT_STATE_META_SYNC 15
#define WT_CHECKPOINT_STATE_ROLLBACK 16
#define WT_CHECKPOINT_STATE_LOG 17
