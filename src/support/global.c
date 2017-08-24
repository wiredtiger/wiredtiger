/*-
 * Copyright (c) 2014-2017 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

WT_PROCESS __wt_process;			/* Per-process structure */
static int __wt_pthread_once_failed;		/* If initialization failed */

/*
 * __wt_endian_check --
 *	Check the build matches the machine.
 */
static int
__wt_endian_check(void)
{
	uint64_t v;
	bool big;
	const char *e;

	v = 1;
	big = *((uint8_t *)&v) == 0;

#ifdef WORDS_BIGENDIAN
	if (big)
		return (0);
	e = "big-endian";
#else
	if (!big)
		return (0);
	e = "little-endian";
#endif
	fprintf(stderr,
	    "This is a %s build of the WiredTiger data engine, incompatible "
	    "with this system\n", e);
	return (EINVAL);
}

/*
 * __wt_global_once --
 *	Global initialization, run once.
 */
static void
__wt_global_once(void)
{
	WT_DECL_RET;

	if ((ret =
	    __wt_spin_init(NULL, &__wt_process.spinlock, "global")) != 0) {
		__wt_pthread_once_failed = ret;
		return;
	}

	__wt_checksum_init();

	TAILQ_INIT(&__wt_process.connqh);

#ifdef HAVE_DIAGNOSTIC
	/* Load debugging code the compiler might optimize out. */
	__wt_breakpoint();
#endif
}

/*
 * __wt_library_init --
 *	Some things to do, before we do anything else.
 */
int
__wt_library_init(void)
{
	static bool first = true;
	WT_DECL_RET;

	/* Check the build matches the machine. */
	WT_RET(__wt_endian_check());

	/*
	 * Do per-process initialization once, before anything else, but only
	 * once.  I don't know how heavy-weight the function (pthread_once, in
	 * the POSIX world), might be, so I'm front-ending it with a local
	 * static and only using that function to avoid a race.
	 */
	if (first) {
		if ((ret = __wt_once(__wt_global_once)) != 0)
			__wt_pthread_once_failed = ret;
		first = false;
	}
	return (__wt_pthread_once_failed);
}

#ifdef HAVE_DIAGNOSTIC
/*
 * __wt_breakpoint --
 *	A simple place to put a breakpoint, if you need one.
 */
void
__wt_breakpoint(void)
{
	/*
	 * Yield the processor (just to keep the compiler from optimizing the
	 * function out).
	 */
	__wt_yield();
}
#endif
