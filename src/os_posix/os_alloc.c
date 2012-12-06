/*-
 * Copyright (c) 2008-2012 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * There's no malloc interface, WiredTiger never calls malloc.
 *
 * The problem is an application might allocate memory, write secret stuff in
 * it, free the memory, then WiredTiger allocates the memory and uses it for a
 * file page or log record, then writes it to disk, without having overwritten
 * it fully.  That results in the secret stuff being protected by WiredTiger's
 * permission mechanisms, potentially inappropriate for the secret stuff.
 */

/*
 * __wt_calloc --
 *	ANSI calloc function.
 */
int
__wt_calloc(WT_SESSION_IMPL *session, size_t number, size_t size, void *retp)
{
	void *p;

	/*
	 * !!!
	 * This function MUST handle a NULL WT_SESSION_IMPL handle.
	 */
	WT_ASSERT(session, number != 0 && size != 0);

	if (session != NULL && S2C(session)->stats != NULL)
		WT_CSTAT_INCR(session, memory_allocation);

	if ((p = calloc(number, size)) == NULL)
		WT_RET_MSG(session, __wt_errno(), "memory allocation");

	*(void **)retp = p;
	return (0);
}

/*
 * __wt_realloc --
 *	ANSI realloc function.
 */
int
__wt_realloc(WT_SESSION_IMPL *session,
    size_t *bytes_allocated_ret, size_t bytes_to_allocate, void *retp)
{
	void *p;
	size_t bytes_allocated;

	/*
	 * !!!
	 * This function MUST handle a NULL WT_SESSION_IMPL handle.
	 */
	WT_ASSERT(session, bytes_to_allocate != 0);

	/*
	 * Sometimes we're allocating memory and we don't care about the
	 * final length -- bytes_allocated_ret may be NULL.
	 */
	bytes_allocated = (bytes_allocated_ret == NULL) ?
	    0 : *bytes_allocated_ret;
	WT_ASSERT(session, bytes_allocated < bytes_to_allocate);

	p = *(void **)retp;

	if (p == NULL && session != NULL && S2C(session)->stats != NULL)
		WT_CSTAT_INCR(session, memory_allocation);

	if ((p = realloc(p, bytes_to_allocate)) == NULL)
		WT_RET_MSG(session, __wt_errno(), "memory allocation");

	/*
	 * Clear the allocated memory -- an application might: allocate memory,
	 * write secret stuff into it, free the memory, then we re-allocate the
	 * memory and use it for a file page or log record, and then write it to
	 * disk.  That would result in the secret stuff being protected by the
	 * WiredTiger permission mechanisms, potentially inappropriate for the
	 * secret stuff.
	 */
	memset((uint8_t *)
	    p + bytes_allocated, 0, bytes_to_allocate - bytes_allocated);

	/* Update caller's bytes allocated value. */
	if (bytes_allocated_ret != NULL)
		*bytes_allocated_ret = bytes_to_allocate;

	*(void **)retp = p;
	return (0);
}

/*
 * __wt_realloc_aligned --
 *	ANSI realloc function that aligns to buffer boundaries, configured with
 *	the "buffer_alignment" key to wiredtiger_open.
 */
int
__wt_realloc_aligned(WT_SESSION_IMPL *session,
    size_t *bytes_allocated_ret, size_t bytes_to_allocate, void *retp)
{
#if defined(HAVE_POSIX_MEMALIGN)
	WT_DECL_RET;

	/*
	 * !!!
	 * This function MUST handle a NULL WT_SESSION_IMPL handle.
	 */
	if (session != NULL && S2C(session)->buffer_alignment > 0) {
		void *p, *newp;
		size_t bytes_allocated;

		WT_ASSERT(session, bytes_to_allocate != 0);

		/*
		 * Sometimes we're allocating memory and we don't care about the
		 * final length -- bytes_allocated_ret may be NULL.
		 */
		bytes_allocated = (bytes_allocated_ret == NULL) ?
		    0 : *bytes_allocated_ret;
		WT_ASSERT(session, bytes_allocated < bytes_to_allocate);

		p = *(void **)retp;

		WT_ASSERT(session, p == NULL || bytes_allocated != 0);

		if (p == NULL && session != NULL && S2C(session)->stats != NULL)
			WT_CSTAT_INCR(session, memory_allocation);

		if ((ret = posix_memalign(&newp,
		    S2C(session)->buffer_alignment,
		    bytes_to_allocate)) != 0)
			WT_RET_MSG(session, ret, "memory allocation");

		if (p != NULL)
			memcpy(newp, p, bytes_allocated);
		__wt_free(session, p);
		p = newp;

		/* Clear the allocated memory (see above). */
		memset((uint8_t *)p + bytes_allocated, 0,
		    bytes_to_allocate - bytes_allocated);

		/* Update caller's bytes allocated value. */
		if (bytes_allocated_ret != NULL)
			*bytes_allocated_ret = bytes_to_allocate;

		*(void **)retp = p;
		return (0);
	}
#endif
	/*
	 * If there is no posix_memalign function, or no alignment configured,
	 * fall back to realloc.
	 */
	return (__wt_realloc(
	    session, bytes_allocated_ret, bytes_to_allocate, retp));
}

/*
 * __wt_strndup --
 *	Duplicate a string of a given length (and NUL-terminate).
 */
int
__wt_strndup(WT_SESSION_IMPL *session, const char *str, size_t len, void *retp)
{
	void *p;

	if (str == NULL) {
		*(void **)retp = NULL;
		return (0);
	}

	WT_RET(__wt_calloc(session, len + 1, 1, &p));

	/*
	 * Don't change this to strncpy, we rely on this function to duplicate
	 * "strings" that contain nul bytes.
	 */
	memcpy(p, str, len);

	*(void **)retp = p;
	return (0);
}

/*
 * __wt_strdup --
 *	ANSI strdup function.
 */
int
__wt_strdup(WT_SESSION_IMPL *session, const char *str, void *retp)
{
	return (__wt_strndup(session,
	    str, (str == NULL) ? 0 : strlen(str), retp));
}

/*
 * __wt_free_int --
 *	ANSI free function.
 */
void
__wt_free_int(WT_SESSION_IMPL *session, void *p_arg)
{
	void *p;

	/*
	 * !!!
	 * This function MUST handle a NULL WT_SESSION_IMPL handle.
	 */
	if (session != NULL && S2C(session)->stats != NULL)
		WT_CSTAT_INCR(session, memory_free);

	/*
	 * If there's a serialization bug we might race with another thread.
	 * We can't avoid the race (and we aren't willing to flush memory),
	 * but we minimize the window by clearing the free address atomically,
	 * hoping a racing thread will see, and won't free, a NULL pointer.
	 */
	p = *(void **)p_arg;
	*(void **)p_arg = NULL;

	if (p != NULL)			/* ANSI C free semantics */
		free(p);
}
