/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#define WT_CHECKPOINT_SESSION_FLAGS (WT_SESSION_CAN_WAIT | WT_SESSION_IGNORE_CACHE_SIZE)

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
#define WT_CHECKPOINT_STATE_RUNNING 18
#define WT_CHECKPOINT_STATE_ESTABLISH 19
#define WT_CHECKPOINT_STATE_START_TXN 20

/*
+ * WT_CKPT_WORK_UNIT --
+ *	A definition of maintenance that a Checkpoint tree needs done.
+ */
struct __wt_ckpt_work_unit {
    TAILQ_ENTRY(__wt_ckpt_work_unit) q; /* Worker unit queue */
    WT_DATA_HANDLE *handle;
    const char **config;
    WT_TXN_SNAPSHOT *snapshot;
};

/* Checkpoint threads information. */
struct __wt_ckpt_threads {
    WT_CONDVAR *cond; /* Checkpoint thread condition */
    WT_THREAD_GROUP thread_group;
    uint32_t threads; /* Checkpoint threads */
    uint32_t push;
    uint32_t pop;

    /* Locked: checkpoint system work queue. */
    TAILQ_HEAD(__wt_ckpt_qh, __wt_ckpt_work_unit) qh;
    WT_SPINLOCK lock; /* Checkpoint work queue spinlock */
};
