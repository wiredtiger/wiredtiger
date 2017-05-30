/*-
 * Public Domain 2014-2017 MongoDB, Inc.
 * Public Domain 2008-2014 WiredTiger, Inc.
 *
 * This is free and unencumbered software released into the public domain.
 *
 * Anyone is free to copy, modify, publish, use, compile, sell, or
 * distribute this software, either in source code form or as a compiled
 * binary, for any purpose, commercial or non-commercial, and by any
 * means.
 *
 * In jurisdictions that recognize copyright laws, the author or authors
 * of this software dedicate any and all copyright interest in the
 * software to the public domain. We make this dedication for the benefit
 * of the public at large and to the detriment of our heirs and
 * successors. We intend this dedication to be an overt act of
 * relinquishment in perpetuity of all present and future rights to this
 * software under copyright law.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

/*
 * Inspired by "Spinlocks and Read-Write Locks" by Dr. Steven Fuerst:
 *	http://locklessinc.com/articles/locks/
 *
 * Dr. Fuerst further credits:
 *	There exists a form of the ticket lock that is designed for read-write
 * locks. An example written in assembly was posted to the Linux kernel mailing
 * list in 2002 by David Howells from RedHat. This was a highly optimized
 * version of a read-write ticket lock developed at IBM in the early 90's by
 * Joseph Seigh. Note that a similar (but not identical) algorithm was published
 * by John Mellor-Crummey and Michael Scott in their landmark paper "Scalable
 * Reader-Writer Synchronization for Shared-Memory Multiprocessors".
 *
 * The following is an explanation of this code. First, the underlying lock
 * structure.
 *
 *	struct {
 *		uint16_t writers;	Now serving for writers
 *		uint16_t readers;	Now serving for readers
 *		uint16_t next;		Next available ticket number
 *		uint16_t __notused;	Padding
 *	}
 *
 * First, imagine a store's 'take a number' ticket algorithm. A customer takes
 * a unique ticket number and customers are served in ticket order. In the data
 * structure, 'writers' is the next writer to be served, 'readers' is the next
 * reader to be served, and 'next' is the next available ticket number.
 *
 * Next, consider exclusive (write) locks. The 'now serving' number for writers
 * is 'writers'. To lock, 'take a number' and wait until that number is being
 * served; more specifically, atomically copy and increment the current value of
 * 'next', and then wait until 'writers' equals that copied number.
 *
 * Shared (read) locks are similar. Like writers, readers atomically get the
 * next number available. However, instead of waiting for 'writers' to equal
 * their number, they wait for 'readers' to equal their number.
 *
 * This has the effect of queuing lock requests in the order they arrive
 * (incidentally avoiding starvation).
 *
 * Each lock/unlock pair requires incrementing both 'readers' and 'writers'.
 * In the case of a reader, the 'readers' increment happens when the reader
 * acquires the lock (to allow read-lock sharing), and the 'writers' increment
 * happens when the reader releases the lock. In the case of a writer, both
 * 'readers' and 'writers' are incremented when the writer releases the lock.
 *
 * For example, consider the following read (R) and write (W) lock requests:
 *
 *						writers	readers	next
 *						0	0	0
 *	R: ticket 0, readers match	OK	0	1	1
 *	R: ticket 1, readers match	OK	0	2	2
 *	R: ticket 2, readers match	OK	0	3	3
 *	W: ticket 3, writers no match	block	0	3	4
 *	R: ticket 2, unlock			1	3	4
 *	R: ticket 0, unlock			2	3	4
 *	R: ticket 1, unlock			3	3	4
 *	W: ticket 3, writers match	OK	3	3	4
 *
 * Note the writer blocks until 'writers' equals its ticket number and it does
 * not matter if readers unlock in order or not.
 *
 * Readers or writers entering the system after the write lock is queued block,
 * and the next ticket holder (reader or writer) will unblock when the writer
 * unlocks. An example, continuing from the last line of the above example:
 *
 *						writers	readers	next
 *	W: ticket 3, writers match	OK	3	3	4
 *	R: ticket 4, readers no match	block	3	3	5
 *	R: ticket 5, readers no match	block	3	3	6
 *	W: ticket 6, writers no match	block	3	3	7
 *	W: ticket 3, unlock			4	4	7
 *	R: ticket 4, readers match	OK	4	5	7
 *	R: ticket 5, readers match	OK	4	6	7
 *
 * The 'next' field is a 2-byte value so the available ticket number wraps at
 * 64K requests. If a thread's lock request is not granted until the 'next'
 * field cycles and the same ticket is taken by another thread, we could grant
 * a lock to two separate threads at the same time, and bad things happen: two
 * writer threads or a reader thread and a writer thread would run in parallel,
 * and lock waiters could be skipped if the unlocks race. This is unlikely, it
 * only happens if a lock request is blocked by 64K other requests. The fix is
 * to grow the lock structure fields, but the largest atomic instruction we have
 * is 8 bytes, the structure has no room to grow.
 */

#include "wt_internal.h"

/*
 * __wt_rwlock_init --
 *	Initialize a read/write lock.
 */
void
__wt_rwlock_init(WT_SESSION_IMPL *session, WT_RWLOCK *l)
{
	WT_DECL_RET;

	l->u.v = 0;

	/* XXX needs real error handling. */
	ret = __wt_cond_alloc(session, "rwlock wait", &l->cond_readers);
	WT_ASSERT(session, ret == 0);
	ret = __wt_cond_alloc(session, "rwlock wait", &l->cond_writers);
	WT_ASSERT(session, ret == 0);
	WT_UNUSED(ret);
}

/*
 * __wt_rwlock_destroy --
 *	Destroy a read/write lock.
 */
void
__wt_rwlock_destroy(WT_SESSION_IMPL *session, WT_RWLOCK *l)
{
	WT_DECL_RET;

	l->u.v = 0;

	/* XXX needs real error handling. */
	ret = __wt_cond_destroy(session, &l->cond_readers);
	WT_ASSERT(session, ret == 0);
	ret = __wt_cond_destroy(session, &l->cond_writers);
	WT_ASSERT(session, ret == 0);
	WT_UNUSED(ret);
}

/*
 * __wt_try_readlock --
 *	Try to get a shared lock, fail immediately if unavailable.
 */
int
__wt_try_readlock(WT_SESSION_IMPL *session, WT_RWLOCK *l)
{
	WT_RWLOCK new, old;

	WT_STAT_CONN_INCR(session, rwlock_read);

	new.u.v = old.u.v = l->u.v;

	/* This read lock can only be granted if there are no active writers. */
	if (old.u.s.current != old.u.s.next)
		return (EBUSY);

	/* The replacement lock value is a result of adding an active reader. */
	new.u.s.readers_active++;
	return (__wt_atomic_casv64(&l->u.v, old.u.v, new.u.v) ? 0 : EBUSY);
}

/*
 * __read_blocked --
 *	Check whether the current read lock request should keep waiting.
 */
static bool
__read_blocked(WT_SESSION_IMPL *session) {
	WT_RWLOCK old;
	uint8_t ticket = session->current_rwticket;

	old.u.v = session->current_rwlock->u.v;
	return (ticket != old.u.s.current);
}

/*
 * __wt_readlock --
 *	Get a shared lock.
 */
void
__wt_readlock(WT_SESSION_IMPL *session, WT_RWLOCK *l)
{
	WT_RWLOCK new, old;
	int pause_cnt;
	uint8_t ticket;

	WT_STAT_CONN_INCR(session, rwlock_read);

	WT_DIAGNOSTIC_YIELD;

	pause_cnt = 0;
	do {
restart:	/*
		 * Fast path: if there is no active writer, join the current
		 * group.
		 */
		for (old.u.v = l->u.v;
		    old.u.s.current == old.u.s.next;
		    old.u.v = l->u.v) {
			new.u.v = old.u.v;
			new.u.s.readers_active++;
			if (__wt_atomic_casv64(&l->u.v, old.u.v, new.u.v))
				return;
			WT_PAUSE();
		}

		/*
		 * There is an active writer: join the next group.
		 * Check for wrapping: if the maximum number of readers are
		 * already queued, wait until we can get a valid ticket.
		 * If we are the first reader to queue, set the next read
		 * group.
		 * Note: don't re-read from the lock or we could race with a
		 * writer unlocking.
		 */
		if (old.u.s.readers_queued == UINT16_MAX ||
		    (old.u.s.next != old.u.s.current &&
		    old.u.s.readers_queued > old.u.s.next - old.u.s.current)) {
			__wt_cond_wait(
			    session, l->cond_readers, WT_THOUSAND, NULL);
			goto restart;
		}
		new.u.v = old.u.v;
		if (new.u.s.readers_queued++ == 0)
			new.u.s.reader = new.u.s.next;
		ticket = new.u.s.reader;

		/*
		 * Check for wrapping: if we have more than 64K lockers
		 * waiting, the ticket value will wrap and two lockers will
		 * simultaneously be granted the lock.
		 */
		WT_ASSERT(session, new.u.s.readers_queued != 0);
	} while (!__wt_atomic_casv64(&l->u.v, old.u.v, new.u.v));

	/* Wait for our group to start. */
	for (pause_cnt = 0; ticket != l->u.s.current; pause_cnt++) {
		if (pause_cnt < 1000)
			WT_PAUSE();
		else if (pause_cnt < 1200)
			__wt_yield();
		else {
			session->current_rwlock = l;
			session->current_rwticket = ticket;
			__wt_cond_wait(
			    session, l->cond_readers, 0, __read_blocked);
			pause_cnt = 0;
		}
	}

	WT_ASSERT(session, l->u.s.readers_active > 0);

	/*
	 * Applications depend on a barrier here so that operations holding the
	 * lock see consistent data.
	 */
	WT_READ_BARRIER();
}

/*
 * __wt_readunlock --
 *	Release a shared lock.
 */
void
__wt_readunlock(WT_SESSION_IMPL *session, WT_RWLOCK *l)
{
	WT_RWLOCK new, old;

	do {
		old.u.v = l->u.v;
		WT_ASSERT(session, old.u.s.readers_active > 0);

		/*
		 * Decrement the active reader count (other readers are doing
		 * the same, make sure we don't race).
		 */
		new.u.v = old.u.v;
		--new.u.s.readers_active;
	} while (!__wt_atomic_casv64(&l->u.v, old.u.v, new.u.v));

	if (new.u.s.readers_active == 0 && new.u.s.current != new.u.s.next)
		__wt_cond_signal(session, l->cond_writers);
}

/*
 * __wt_try_writelock --
 *	Try to get an exclusive lock, fail immediately if unavailable.
 */
int
__wt_try_writelock(WT_SESSION_IMPL *session, WT_RWLOCK *l)
{
	WT_RWLOCK new, old;

	WT_STAT_CONN_INCR(session, rwlock_write);

	/*
	 * This write lock can only be granted if no readers or writers blocked
	 * on the lock, that is, if this thread's ticket would be the next
	 * ticket granted.  Check if this can possibly succeed (and confirm the
	 * lock is in the correct state to grant this write lock).
	 */
	old.u.v = l->u.v;
	if (old.u.s.current != old.u.s.next || old.u.s.readers_active != 0)
		return (EBUSY);

	WT_ASSERT(session, old.u.s.readers_queued == 0);

	/* The replacement lock value is a result of allocating a new ticket. */
	new.u.v = old.u.v;
	new.u.s.next++;
	return (__wt_atomic_casv64(&l->u.v, old.u.v, new.u.v) ? 0 : EBUSY);
}

/*
 * __write_blocked --
 *	Check whether the current write lock request should keep waiting.
 */
static bool
__write_blocked(WT_SESSION_IMPL *session) {
	WT_RWLOCK old;
	uint8_t ticket = session->current_rwticket;

	old.u.v = session->current_rwlock->u.v;
	return (ticket != old.u.s.current || old.u.s.readers_active != 0);
}

/*
 * __wt_writelock --
 *	Wait to get an exclusive lock.
 */
void
__wt_writelock(WT_SESSION_IMPL *session, WT_RWLOCK *l)
{
	WT_RWLOCK new, old;
	int pause_cnt;
	uint8_t ticket;

	WT_STAT_CONN_INCR(session, rwlock_write);

	do {
restart:	new.u.v = old.u.v = l->u.v;
		ticket = new.u.s.next++;

		/*
		 * Avoid wrapping: if we allocate more than 256 tickets, two
		 * lockers will simultaneously be granted the lock.
		 */
		if (new.u.s.next == new.u.s.current) {
			__wt_cond_wait(
			    session, l->cond_writers, WT_THOUSAND, NULL);
			goto restart;
		}
	} while (!__wt_atomic_casv64(&l->u.v, old.u.v, new.u.v));

	/* Wait for our group to start and any readers to drain. */
	for (pause_cnt = 0;
	    ticket != l->u.s.current || l->u.s.readers_active != 0;
	    pause_cnt++) {
		if (pause_cnt < 1000)
			WT_PAUSE();
		else if (pause_cnt < 1200)
			__wt_yield();
		else {
			session->current_rwlock = l;
			session->current_rwticket = ticket;
			__wt_cond_wait(
			    session, l->cond_writers, 0, __write_blocked);
		}
	}

	/*
	 * Applications depend on a barrier here so that operations holding the
	 * lock see consistent data.
	 */
	WT_READ_BARRIER();
}

/*
 * __wt_writeunlock --
 *	Release an exclusive lock.
 */
void
__wt_writeunlock(WT_SESSION_IMPL *session, WT_RWLOCK *l)
{
	WT_RWLOCK new, old;

	do {
		new.u.v = old.u.v = l->u.v;

		/*
		 * We're holding the lock exclusive, there shouldn't be any
		 * active readers.
		 */
		WT_ASSERT(session, old.u.s.readers_active == 0);

		/*
		 * Allow the next batch to start.
		 *
		 * If there are readers in the next group, swap queued readers
		 * to active: this could race with new readlock requests, so we
		 * have to spin.
		 */
		if (++new.u.s.current == new.u.s.reader) {
			new.u.s.readers_active = new.u.s.readers_queued;
			new.u.s.readers_queued = 0;
		}
	} while (!__wt_atomic_casv64(&l->u.v, old.u.v, new.u.v));

	WT_DIAGNOSTIC_YIELD;

	if (new.u.s.readers_active != 0)
		__wt_cond_signal(session, l->cond_readers);
	else if (new.u.s.current != new.u.s.next)
		__wt_cond_signal(session, l->cond_writers);
}

#ifdef HAVE_DIAGNOSTIC
/*
 * __wt_rwlock_islocked --
 *	Return if a read/write lock is currently locked for reading or writing.
 */
bool
__wt_rwlock_islocked(WT_SESSION_IMPL *session, WT_RWLOCK *l)
{
	WT_UNUSED(session);

	return (l->u.s.current != l->u.s.next || l->u.s.readers_active != 0);
}
#endif
