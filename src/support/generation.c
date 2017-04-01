/*-
 * Copyright (c) 2014-2016 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * WiredTiger uses generations to manage various resources. Threads publish an
 * a current generation before accessing a resource, and clear it when they are
 * done. For example, a thread wanting to replace an object in memory replaces
 * the object and increments the object's generation. Once no threads have the
 * previous generation published, it is safe to discard the previous version of
 * the object.
 */

/*
 * __wt_gen_init --
 *	Initialize the connection's generations.
 */
void
__wt_gen_init(WT_SESSION_IMPL *session)
{
	int i;

	/*
	 * All generations start at 1, a session with a generation of 0 isn't
	 * using the resource.
	 */
	for (i = 0; i < WT_GENERATIONS; ++i)
		S2C(session)->generations[i] = 1;

	/* Ensure threads see the state change. */
	WT_WRITE_BARRIER();
}

/*
 * __wt_gen --
 *	Return the resource's generation.
 */
uint64_t
__wt_gen(WT_SESSION_IMPL *session, int which)
{
	return (S2C(session)->generations[which]);
}

/*
 * __wt_gen_next --
 *	Switch the resource to its next generation.
 */
uint64_t
__wt_gen_next(WT_SESSION_IMPL *session, int which)
{
	return (__wt_atomic_addv64(&S2C(session)->generations[which], 1));
}

/*
 * __wt_gen_next_drain --
 *	Switch the resource to its next generation, then wait for it to drain.
 */
uint64_t
__wt_gen_next_drain(WT_SESSION_IMPL *session, int which)
{
	uint64_t v;

	v = __wt_atomic_addv64(&S2C(session)->generations[which], 1);

	__wt_gen_drain(session, which, v);

	return (v);
}

/*
 * __wt_gen_drain --
 *	Wait for the resource to drain.
 */
void
__wt_gen_drain(WT_SESSION_IMPL *session, int which, uint64_t generation)
{
	WT_CONNECTION_IMPL *conn;
	WT_SESSION_IMPL *s;
	uint64_t v;
	uint32_t i, session_cnt;
	int pause_cnt;

	conn = S2C(session);

	/*
	 * No lock is required because the session array is fixed size, but it
	 * may contain inactive entries. We must review any active session, so
	 * insert a read barrier after reading the active session count. That
	 * way, no matter what sessions come or go, we'll check the slots for
	 * all of the sessions that could have been active when we started our
	 * check.
	 */
	WT_ORDERED_READ(session_cnt, conn->session_cnt);
	for (pause_cnt = 0,
	    s = conn->sessions, i = 0; i < session_cnt; ++s, ++i) {
		if (!s->active)
			continue;

		for (;;) {
			/* Ensure we only read the value once. */
			WT_ORDERED_READ(v, s->generations[which]);

			if (v == 0 || generation <= v)
				break;

			/*
			 * The pause count is cumulative, quit spinning if it's
			 * not doing us any good, that can happen in generations
			 * that don't move quickly.
			 */
			if (++pause_cnt < WT_THOUSAND)
				WT_PAUSE();
			else
				__wt_sleep(0, 10);
		}
	}
}

/*
 * __wt_gen_oldest --
 *	Return the oldest generation in use for the resource.
 */
uint64_t
__wt_gen_oldest(WT_SESSION_IMPL *session, int which)
{
	WT_CONNECTION_IMPL *conn;
	WT_SESSION_IMPL *s;
	uint64_t oldest, v;
	uint32_t i, session_cnt;

	conn = S2C(session);

	/*
	 * No lock is required because the session array is fixed size, but it
	 * may contain inactive entries. We must review any active session, so
	 * insert a read barrier after reading the active session count. That
	 * way, no matter what sessions come or go, we'll check the slots for
	 * all of the sessions that could have been active when we started our
	 * check.
	 */
	WT_ORDERED_READ(session_cnt, conn->session_cnt);
	for (oldest = conn->generations[which] + 1,
	    s = conn->sessions, i = 0; i < session_cnt; ++s, ++i) {
		if (!s->active)
			continue;

		/* Ensure we only read the value once. */
		WT_ORDERED_READ(v, s->generations[which]);
		if (v != 0 && v < oldest)
			oldest = v;
	}

	return (oldest);
}

/*
 * __wt_session_gen --
 *	Return the thread's resource generation.
 */
uint64_t
__wt_session_gen(WT_SESSION_IMPL *session, int which)
{
	return (session->generations[which]);
}

/*
 * __wt_session_gen_publish_next --
 *	Switch the resource to a new generation, then publish a thread's
 * resource generation.
 */
uint64_t
__wt_session_gen_publish_next(WT_SESSION_IMPL *session, int which)
{
	session->generations[which] = __wt_gen_next(session, which);

	/* Ensure threads waiting on a resource to drain see the new value. */
	WT_FULL_BARRIER();

	return (session->generations[which]);
}

/*
 * __wt_session_gen_publish --
 *	Publish a thread's resource generation.
 */
uint64_t
__wt_session_gen_publish(WT_SESSION_IMPL *session, int which)
{
	session->generations[which] = __wt_gen(session, which);

	/* Ensure threads waiting on a resource to drain see the new value. */
	WT_FULL_BARRIER();

	return (session->generations[which]);
}

/*
 * __wt_session_gen_clear --
 *	Clear a thread's resource generation.
 */
void
__wt_session_gen_clear(WT_SESSION_IMPL *session, int which)
{
	/* Ensure writes made by this thread are visible. */
	WT_PUBLISH(session->generations[which], 0);

	/* Let threads waiting for the resource to drain proceed quickly. */
	WT_FULL_BARRIER();
}
