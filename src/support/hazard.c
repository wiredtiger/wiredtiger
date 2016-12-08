/*-
 * Copyright (c) 2014-2016 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

#ifdef HAVE_DIAGNOSTIC
static void __hazard_dump(WT_SESSION_IMPL *);
#endif

/*
 * hazard_grow --
 *	Grow a hazard pointer array.
 */
static int
hazard_grow(WT_SESSION_IMPL *session)
{
	WT_HAZARD *nhazard;
	size_t size;

	/*
	 * Allocate a new, larger hazard pointer array and copy the contents of
	 * the original into place.
	 */
	size = session->hazard_size;
	WT_RET(__wt_calloc_def(session, size * 2, &nhazard));
	memcpy(nhazard, session->hazard, size * sizeof(WT_HAZARD));

	/*
	 * Swap the new hazard pointer array into place after initialization
	 * is complete (initialization must complete before eviction can see
	 * the new hazard pointer array).
	 */
	WT_PUBLISH(session->hazard, nhazard);

	/*
	 * Increase the size of the session's pointer array after swapping it
	 * into place (the session's reference must be updated before eviction
	 * can see the new size).
	 */
	WT_PUBLISH(session->hazard_size, (uint32_t)(size * 2));

	return (0);
}

/*
 * __wt_hazard_set --
 *	Set a hazard pointer.
 */
int
__wt_hazard_set(WT_SESSION_IMPL *session, WT_REF *ref, bool *busyp
#ifdef HAVE_DIAGNOSTIC
    , const char *file, int line
#endif
    )
{
	WT_BTREE *btree;
	WT_HAZARD *hp;
	int restarts;

	*busyp = false;

	btree = S2BT(session);
	restarts = 0;

	/* If a file can never be evicted, hazard pointers aren't required. */
	if (F_ISSET(btree, WT_BTREE_IN_MEMORY))
		return (0);

	/*
	 * If there isn't a valid page, we're done. This read can race with
	 * eviction and splits, we re-check it after a barrier to make sure
	 * we have a valid reference.
	 */
	if (ref->state != WT_REF_MEM) {
		*busyp = true;
		return (0);
	}

	/*
	 * Do the dance:
	 *
	 * The memory location which makes a page "real" is the WT_REF's state
	 * of WT_REF_MEM, which can be set to WT_REF_LOCKED at any time by the
	 * page eviction server.
	 *
	 * Add the WT_REF reference to the session's hazard list and flush the
	 * write, then see if the page's state is still valid.  If so, we can
	 * use the page because the page eviction server will see our hazard
	 * pointer before it discards the page (the eviction server sets the
	 * state to WT_REF_LOCKED, then flushes memory and checks the hazard
	 * pointers).
	 *
	 * For sessions with many active hazard pointers, skip most of the
	 * active slots: there may be a free slot in there, but checking is
	 * expensive.  Most hazard pointers are released quickly: optimize
	 * for that case.
	 */
	for (hp = session->hazard + session->nhazard;; ++hp) {
		/*
		 * We start in the middle of the array, past the count of active
		 * hazard pointers to avoid skipping over lots of in-use slots.
		 * If we get to the end of the array:
		 * 1. If there are free slots in the array and this is the first
		 *    time through the array, continue the search from the
		 *    start so we keep the list compact. Don't actually continue
		 *    the loop because that will skip the first slot.
		 * 2. If there is a slot not currently in-use, increment the
		 *    in-use value to make the slot visible. The slot we are on
		 *    should now be available.
		 * 3. Grow the array.
		 */
		if (hp >= session->hazard + session->hazard_inuse) {
			if (session->nhazard < session->hazard_inuse &&
			    restarts++ == 0)
				hp = session->hazard;
			else if (session->hazard_inuse < session->hazard_size)
				++session->hazard_inuse;
			else {
				WT_RET(hazard_grow(session));
				hp = &session->hazard[session->hazard_inuse++];
			}
		}

		if (hp->ref != NULL)
			continue;

		hp->ref = ref;
#ifdef HAVE_DIAGNOSTIC
		hp->file = file;
		hp->line = line;
#endif
		/* Publish the hazard pointer before reading page's state. */
		WT_FULL_BARRIER();

		/*
		 * Check if the page state is still valid, where valid means a
		 * state of WT_REF_MEM.
		 */
		if (ref->state == WT_REF_MEM) {
			++session->nhazard;

			/*
			 * Callers require a barrier here so operations holding
			 * the hazard pointer see consistent data.
			 */
			WT_READ_BARRIER();
			return (0);
		}

		/*
		 * The page isn't available, it's being considered for eviction
		 * (or being evicted, for all we know).  If the eviction server
		 * sees our hazard pointer before evicting the page, it will
		 * return the page to use, no harm done, if it doesn't, it will
		 * go ahead and complete the eviction.
		 *
		 * We don't bother publishing this update: the worst case is we
		 * prevent some random page from being evicted.
		 */
		hp->ref = NULL;
		*busyp = true;
		return (0);
	}
}

/*
 * __wt_hazard_clear --
 *	Clear a hazard pointer.
 */
int
__wt_hazard_clear(WT_SESSION_IMPL *session, WT_REF *ref)
{
	WT_BTREE *btree;
	WT_HAZARD *hp;

	btree = S2BT(session);

	/* If a file can never be evicted, hazard pointers aren't required. */
	if (F_ISSET(btree, WT_BTREE_IN_MEMORY))
		return (0);

	/*
	 * Clear the caller's hazard pointer.
	 * The common pattern is LIFO, so do a reverse search.
	 */
	for (hp = session->hazard + session->hazard_inuse - 1;
	    hp >= session->hazard;
	    --hp)
		if (hp->ref == ref) {
			/*
			 * We don't publish the hazard pointer clear in the
			 * general case.  It's not required for correctness;
			 * it gives an eviction thread faster access to the
			 * page were the page selected for eviction, but the
			 * generation number was just set, it's unlikely the
			 * page will be selected for eviction.
			 */
			hp->ref = NULL;

			/*
			 * If this was the last hazard pointer in the session,
			 * reset the size so that checks can skip this session.
			 */
			if (--session->nhazard == 0)
				WT_PUBLISH(session->hazard_inuse, 0);
			return (0);
		}

	/*
	 * A serious error, we should always find the hazard pointer.  Panic,
	 * because using a page we didn't have pinned down implies corruption.
	 */
	WT_PANIC_RET(session, EINVAL,
	    "session %p: clear hazard pointer: %p: not found",
	    (void *)session, (void *)ref);
}

/*
 * __wt_hazard_close --
 *	Verify that no hazard pointers are set.
 */
void
__wt_hazard_close(WT_SESSION_IMPL *session)
{
	WT_HAZARD *hp;
	bool found;

	/*
	 * Check for a set hazard pointer and complain if we find one.  We could
	 * just check the session's hazard pointer count, but this is a useful
	 * diagnostic.
	 */
	for (found = false, hp = session->hazard;
	    hp < session->hazard + session->hazard_inuse; ++hp)
		if (hp->ref != NULL) {
			found = true;
			break;
		}
	if (session->nhazard == 0 && !found)
		return;

	__wt_errx(session,
	    "session %p: close hazard pointer table: table not empty",
	    (void *)session);

#ifdef HAVE_DIAGNOSTIC
	__hazard_dump(session);
#endif

	/*
	 * Clear any hazard pointers because it's not a correctness problem
	 * (any hazard pointer we find can't be real because the session is
	 * being closed when we're called). We do this work because session
	 * close isn't that common that it's an expensive check, and we don't
	 * want to let a hazard pointer lie around, keeping a page from being
	 * evicted.
	 *
	 * We don't panic: this shouldn't be a correctness issue (at least, I
	 * can't think of a reason it would be).
	 */
	for (hp = session->hazard;
	    hp < session->hazard + session->hazard_inuse; ++hp)
		if (hp->ref != NULL) {
			hp->ref = NULL;
			--session->nhazard;
		}

	if (session->nhazard != 0)
		__wt_errx(session,
		    "session %p: close hazard pointer table: count didn't "
		    "match entries",
		    (void *)session);
}

/*
 * __wt_hazard_count --
 *	Count how many hazard pointers this session has on the given page.
 */
u_int
__wt_hazard_count(WT_SESSION_IMPL *session, WT_REF *ref)
{
	WT_HAZARD *hp;
	u_int count;

	for (count = 0, hp = session->hazard + session->hazard_inuse - 1;
	    hp >= session->hazard;
	    --hp)
		if (hp->ref == ref)
			++count;

	return (count);
}

#ifdef HAVE_DIAGNOSTIC
/*
 * __hazard_dump --
 *	Display the list of hazard pointers.
 */
static void
__hazard_dump(WT_SESSION_IMPL *session)
{
	WT_HAZARD *hp;

	for (hp = session->hazard;
	    hp < session->hazard + session->hazard_inuse; ++hp)
		if (hp->ref != NULL)
			__wt_errx(session,
			    "session %p: hazard pointer %p: %s, line %d",
			    (void *)session,
			    (void *)hp->ref, hp->file, hp->line);
}
#endif
