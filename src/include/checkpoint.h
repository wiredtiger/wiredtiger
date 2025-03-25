/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

#define WT_CHECKPOINT_SESSION_FLAGS (WT_SESSION_CAN_WAIT | WT_SESSION_IGNORE_CACHE_SIZE)

#define WT_CHECKPOINT_STATE_INACTIVE 0
#define WT_CHECKPOINT_STATE_APPLY_META 1
#define WT_CHECKPOINT_STATE_APPLY_BTREE 2
#define WT_CHECKPOINT_STATE_UPDATE_OLDEST 3
#define WT_CHECKPOINT_STATE_SYNC_FILE 4
#define WT_CHECKPOINT_STATE_EVICT_FILE 5
#define WT_CHECKPOINT_STATE_BM_SYNC 6
#define WT_CHECKPOINT_STATE_RESOLVE 7
#define WT_CHECKPOINT_STATE_POSTPROCESS 8
#define WT_CHECKPOINT_STATE_HS 9
#define WT_CHECKPOINT_STATE_HS_SYNC 10
#define WT_CHECKPOINT_STATE_COMMIT 11
#define WT_CHECKPOINT_STATE_META_CKPT 12
#define WT_CHECKPOINT_STATE_META_SYNC 13
#define WT_CHECKPOINT_STATE_ROLLBACK 14
#define WT_CHECKPOINT_STATE_LOG 15
#define WT_CHECKPOINT_STATE_RUNNING 16
#define WT_CHECKPOINT_STATE_ESTABLISH 17
#define WT_CHECKPOINT_STATE_START_TXN 18
#define WT_CHECKPOINT_STATE_CKPT_TREE 19

#define WT_DISAGG_CHECKPOINT_ID_NONE 0
#define WT_DISAGG_CHECKPOINT_ID_FIRST 1

struct __wt_checkpoint_cleanup {
    WT_SESSION_IMPL *session; /* checkpoint cleanup session */
    wt_thread_t tid;          /* checkpoint cleanup thread */
    int tid_set;              /* checkpoint cleanup thread set */
    WT_CONDVAR *cond;         /* checkpoint cleanup wait mutex */
    uint64_t interval;        /* Checkpoint cleanup interval */
};

/*
 * WT_CHECKPOINT_PAGE_TO_RECONCILE --
 *     A work item for reconciling a page.
 */
struct __wt_checkpoint_page_to_reconcile {
    TAILQ_ENTRY(__wt_checkpoint_page_to_reconcile) q; /* Worker unit queue */

    WT_DATA_HANDLE *dhandle;
    //WT_TXN_ISOLATION isolation;
    WT_TXN_SNAPSHOT *snapshot;

    WT_REF *ref;
    uint32_t reconcile_flags;
    uint32_t release_flags;

    int ret; /* Result - will be filled out later. */
};

// XXX
#include <semaphore.h>

/*
 * WT_CHECKPOINT_RECONCILE_THREADS --
 *     Information about threads for parallel page reconciliation during a checkpoint.
 */
struct __wt_checkpoint_reconcile_threads {
    WT_THREAD_GROUP thread_group;
    uint32_t num_threads;

    TAILQ_HEAD(__wt_checkpoint_reconcile_work_qh, __wt_checkpoint_page_to_reconcile) work_qh;
    WT_CONDVAR *work_cond; /* Signal that work is available. */
    WT_SPINLOCK work_lock;
    wt_shared uint64_t work_pushed;
    sem_t work_sem;

    TAILQ_HEAD(__wt_checkpoint_reconcile_done_qh, __wt_checkpoint_page_to_reconcile) done_qh;
    WT_CONDVAR *done_cond; /* Signal that the work is done (not just that the queue has stuff). */
    WT_SPINLOCK done_lock;
    wt_shared uint64_t done_pushed;
};
