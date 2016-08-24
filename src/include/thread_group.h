/*-
 * Copyright (c) 2014-2016 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * WT_WORKER_THREAD --
 *	Encapsulation of a utility worker thread.
 */
struct __wt_worker_thread {
	WT_SESSION_IMPL *session;
	u_int id;
	wt_thread_t tid;
#define	WT_WORKER_THREAD_RUN		0x01
	uint32_t flags;

	/* The runner function used by all workers. */
	int (*run_func)(WT_SESSION_IMPL *session, WT_WORKER_THREAD *context);
};

#define	WT_WORKER_CAN_WAIT		0x01
#define	WT_WORKER_PANIC_FAIL		0x02

/*
 * WT_WORKER_THREAD_GROUP --
 *	Encapsulation of a group of utility worker threads.
 */
struct __wt_worker_thread_group {
	uint32_t	 alloc;    /* Size of allocated group */
	uint32_t	 max;      /* Max threads in group */
	uint32_t	 min;      /* Min threads in group */
	uint32_t	 current_workers;	/* Number of active workers */

	WT_RWLOCK	*lock;     /* Protects group changes */

	/*
	 * Condition signalled when wanting to wake up threads that are
	 * part of the group - for example when shutting down. This condition
	 * can also be used by group owners to ensure state changes are noticed.
	 */
	WT_CONDVAR      *wait_cond;

	/*
	 * The worker threads need to be held in an array of arrays, not an
	 * array of structures because the array is reallocated as it grows,
	 * which causes threads to loose track of their context is realloc
	 * moves the memory.
	 */
	WT_WORKER_THREAD **workers;

	/* The runner function used by all workers. */
	int (*run_func)(WT_SESSION_IMPL *session, WT_WORKER_THREAD *context);
};
