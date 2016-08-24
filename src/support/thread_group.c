/*-
 * Copyright (c) 2014-2016 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_util_thread_run --
 *	General wrapper for any eviction thread.
 */
WT_THREAD_RET
__wt_util_thread_run(void *arg)
{
	WT_DECL_RET;
	WT_SESSION_IMPL *session;
	WT_WORKER_THREAD *worker;

	worker = (WT_WORKER_THREAD*)arg;
	session = worker->session;

	ret = worker->run_func(session, worker);

	if (ret != 0 && F_ISSET(worker, WT_WORKER_PANIC_FAIL))
		WT_PANIC_MSG(session, ret,
		    "Unrecoverable utility worker thread error");

	/*
	 * The only two cases when eviction workers are expected to stop are
	 * when recovery is finished or when the connection is closing. Check
	 * otherwise fewer eviction worker threads may be running than
	 * expected.
	 */
	WT_ASSERT(session, !F_ISSET(worker, WT_WORKER_THREAD_RUN) ||
	    F_ISSET(S2C(session), WT_CONN_CLOSING | WT_CONN_RECOVERING));

	return (WT_THREAD_RET_VALUE);
}

/*
 * __util_thread_group_grow --
 *	Increase the number of running threads in the group.
 */
static int
__util_thread_group_grow(
    WT_SESSION_IMPL *session, WT_WORKER_THREAD_GROUP *group, uint32_t new_count)
{
	WT_WORKER_THREAD *worker;

	WT_ASSERT(session,
	    __wt_rwlock_islocked(session, group->lock));

	while (group->current_workers < new_count) {
		worker = group->workers[group->current_workers++];
		__wt_verbose(session, WT_VERB_UTIL_THREAD,
		    "Starting utility worker: %p:%"PRIu32"\n",
		    group, worker->id);
		F_SET(worker, WT_WORKER_THREAD_RUN);
		WT_ASSERT(session, worker->session != NULL);
		WT_RET(__wt_thread_create(worker->session,
		    &worker->tid, __wt_util_thread_run, worker));
	}
	return (0);
}

/*
 * __util_thread_group_shrink --
 *	Decrease the number of running threads in the group.
 */
static int
__util_thread_group_shrink(WT_SESSION_IMPL *session,
    WT_WORKER_THREAD_GROUP *group, uint32_t new_count, bool free_worker)
{
	WT_DECL_RET;
	WT_SESSION *wt_session;
	WT_WORKER_THREAD *worker;

	WT_ASSERT(session,
	    __wt_rwlock_islocked(session, group->lock));

	while (group->current_workers > new_count) {
		/*
		 * The current workers is a counter not an array index, so
		 * adjust it before finding the last worker in the group.
		 */
		worker = group->workers[--group->current_workers];
		__wt_verbose(session, WT_VERB_UTIL_THREAD,
		    "Stopping utility worker: %p:%"PRIu32"\n",
		    group, worker->id);
		F_CLR(worker, WT_WORKER_THREAD_RUN);
		/* Wake threads to ensure they notice the state change */
		__wt_cond_signal(session, group->wait_cond);
		WT_TRET(__wt_thread_join(session, worker->tid));
		worker->tid = 0;
		/*
		 * Worker thread sessions are only freed when shrinking the
		 * pool or shutting down the connection.
		 */
		if (free_worker) {
			wt_session = (WT_SESSION *)worker->session;
			WT_TRET(wt_session->close(wt_session, NULL));
			worker->session = NULL;
			__wt_free(session, worker);
		}
	}
	return (0);
}

/*
 * __util_thread_group_resize --
 *	Resize an array of utility workers already holding the lock.
 */
static int
__util_thread_group_resize(
    WT_SESSION_IMPL *session, WT_WORKER_THREAD_GROUP *group,
    uint32_t new_min, uint32_t new_max, uint32_t flags)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_WORKER_THREAD *worker;
	size_t alloc;
	uint32_t i, session_flags;

	conn = S2C(session);

	WT_ASSERT(session,
	    group->current_workers <= group->alloc &&
	    __wt_rwlock_islocked(session, group->lock));

	if (new_min == group->min && new_max == group->max)
		return (0);

	if (group->current_workers > new_max)
		WT_RET(__util_thread_group_shrink(
		    session, group, new_max, true));

	/*
	 * Only reallocate the worker array if it is the largest ever, since
	 * our realloc doesn't support shrinking the allocated size.
	 */
	if (group->alloc < new_max) {
		alloc = group->alloc * sizeof(*group->workers);
		WT_RET(__wt_realloc(session, &alloc,
		    new_max * sizeof(*group->workers), &group->workers));
		group->alloc = new_max;
	}

	/*
	 * Initialize the structures based on the previous group size, not
	 * the previous allocated size.
	 */
	for (i = group->max; i < new_max; i++) {
		WT_ERR(__wt_calloc_one(session, &worker));
		/*
		 * Utility worker threads have their own session.
		 *
		 * Utility worker threads get their own lookaside table cursor
		 * if the lookaside table is open.  Note that utility threads
		 * are started during recovery, before the lookaside table is
		 * created.
		 */
		if (LF_ISSET(WT_WORKER_CAN_WAIT))
			session_flags = WT_SESSION_CAN_WAIT;
		if (F_ISSET(conn, WT_CONN_LAS_OPEN))
			FLD_SET(session_flags, WT_SESSION_LOOKASIDE_CURSOR);
		WT_ERR(__wt_open_internal_session(conn, "utility-worker",
		    false, session_flags, &worker->session));
		if (LF_ISSET(WT_WORKER_PANIC_FAIL))
			F_SET(worker, WT_WORKER_PANIC_FAIL);
		worker->id = i;
		worker->run_func = group->run_func;
		group->workers[i] = worker;
	}

	if (group->current_workers < new_min)
		WT_ERR(__util_thread_group_grow(session, group, new_min));

err:	group->max = new_max;
	group->min = new_min;
	return (ret);
}

/*
 * __wt_util_thread_group_resize --
 *	Resize an array of utility workers taking the lock.
 */
int
__wt_util_thread_group_resize(
    WT_SESSION_IMPL *session, WT_WORKER_THREAD_GROUP *group,
    uint32_t new_min, uint32_t new_max, uint32_t flags)
{
	WT_DECL_RET;

	__wt_verbose(session, WT_VERB_UTIL_THREAD,
	    "Resize thread group: %p, from min: %" PRIu32 " -> %" PRIu32
	    " from max: %" PRIu32 " -> %" PRIu32 "\n",
	    group, group->min, new_min, group->max, new_max);

	WT_ASSERT(session,
	    !__wt_rwlock_islocked(session, group->lock));

	__wt_writelock(session, group->lock);
	WT_TRET(__util_thread_group_resize(
	    session, group, new_min, new_max, flags));
	__wt_writeunlock(session, group->lock);
	return (ret);
}

/*
 * __wt_util_thread_group_create --
 *	Create a new thread group, assumes incoming group structure is
 *	zero initialized.
 */
int
__wt_util_thread_group_create(
    WT_SESSION_IMPL *session, WT_WORKER_THREAD_GROUP *group,
    uint32_t min, uint32_t max, uint32_t flags,
    int (*run_func)(WT_SESSION_IMPL *session, WT_WORKER_THREAD *context))
{
	WT_DECL_RET;

	__wt_verbose(session, WT_VERB_UTIL_THREAD,
	    "Creating thread group: %p\n", group);

	WT_RET(__wt_rwlock_alloc(session, &group->lock, "Thread group"));
	WT_RET(__wt_cond_alloc(
	    session, "Thread group cond", false, &group->wait_cond));

	__wt_writelock(session, group->lock);
	group->run_func = run_func;

	WT_TRET(__util_thread_group_resize(session, group, min, max, flags));
	__wt_writeunlock(session, group->lock);
	return (ret);
}

/*
 * __wt_util_thread_group_destroy --
 *	Shut down a thread group.
 */
int
__wt_util_thread_group_destroy(
    WT_SESSION_IMPL *session, WT_WORKER_THREAD_GROUP *group)
{
	WT_DECL_RET;

	__wt_verbose(session, WT_VERB_UTIL_THREAD,
	    "Destroying thread group: %p\n", group);

	/* Shut down all worker threads. */
	WT_TRET(__util_thread_group_shrink(session, group, 0, true));

	__wt_free(session, group->workers);

	WT_TRET(__wt_cond_destroy(session, &group->wait_cond));
	__wt_rwlock_destroy(session, &group->lock);

	return (ret);
}

/*
 * __wt_util_thread_group_start_one --
 *	Start a new worker if possible
 */
int
__wt_util_thread_group_start_one(
    WT_SESSION_IMPL *session, WT_WORKER_THREAD_GROUP *group, bool wait)
{
	WT_DECL_RET;

	if (group->current_workers >= group->max)
		return (0);

	if (!wait) {
		if (__wt_try_writelock(session, group->lock) != 0)
			return (0);
	} else
		__wt_writelock(session, group->lock);

	/* Recheck the bounds now that we hold the lock */
	if (group->current_workers < group->max)
		WT_TRET(__util_thread_group_grow(
		    session, group, group->current_workers + 1));
	__wt_writeunlock(session, group->lock);

	return (ret);
}

/*
 * __wt_util_thread_group_stop_one --
 *	Stop a running worker if possible
 */
int
__wt_util_thread_group_stop_one(
    WT_SESSION_IMPL *session, WT_WORKER_THREAD_GROUP *group)
{
	WT_DECL_RET;

	if (group->current_workers <= group->min)
		return (0);

	__wt_writelock(session, group->lock);
	WT_TRET(__util_thread_group_shrink(
	    session, group, group->current_workers - 1, false));
	__wt_writeunlock(session, group->lock);

	return (ret);
}
