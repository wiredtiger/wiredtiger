/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * __wt_cond_wait --
 *	Wait on a mutex, optionally timing out.
 */
static inline int
__wt_cond_wait(WT_SESSION_IMPL *session, WT_CONDVAR *cond, uint64_t usecs)
{
	bool notused;

	return (__wt_cond_wait_signal(session, cond, usecs, &notused));
}

/*
 * __wt_strdup --
 *	ANSI strdup function.
 */
static inline int
__wt_strdup(WT_SESSION_IMPL *session, const char *str, void *retp)
{
	return (__wt_strndup(
	    session, str, (str == NULL) ? 0 : strlen(str), retp));
}

/*
 * __wt_seconds --
 *	Return the seconds since the Epoch.
 */
static inline int
__wt_seconds(WT_SESSION_IMPL *session, time_t *timep)
{
	struct timespec t;

	WT_RET(__wt_epoch(session, &t));

	*timep = t.tv_sec;

	return (0);
}

/*
 * __wt_verbose --
 * 	Verbose message.
 */
static inline int
__wt_verbose(WT_SESSION_IMPL *session, int flag, const char *fmt, ...)
    WT_GCC_FUNC_ATTRIBUTE((format (printf, 2, 3)))
{
#ifdef HAVE_VERBOSE
	WT_DECL_RET;
	va_list ap;

	if (WT_VERBOSE_ISSET(session, flag)) {
		va_start(ap, fmt);
		ret = __wt_eventv(session, true, 0, NULL, 0, fmt, ap);
		va_end(ap);
	}
	return (ret);
#else
	WT_UNUSED(session);
	WT_UNUSED(fmt);
	WT_UNUSED(flag);
	return (0);
#endif
}

/*
 * __wt_compression_skip_tracking --
 *	Track compression failures, then skip compression attempts if the data
 * isn't compressing.
 */
static inline void
__wt_compression_skip_tracking(
    uint32_t *fail_cnt, uint32_t *fail_skip, bool failed)
{
	/*
	 * Compression may be configured on a file that doesn't compress well
	 * (but that may be transitory). Compression calls are expensive, and
	 * compression algorithms get more expensive the worse they're doing.
	 * A simple back-off strategy: if there are at least 10 compression
	 * failures in a row, skip the next 10% attempts, until we're only
	 * attempting to compress roughly 1 in every 100 block writes. When
	 * any compression attempt succeeds, reset and start over. To restate,
	 * be aggressive about skipping compression attempts, but err on the
	 * side of compression if there's any evidence it's doing useful work.
	 *
	 * Avoid atomic operations and updating shared variables; we really care
	 * about the count of operations to be skipped (we're in trouble if that
	 * underflows), but other races don't matter.
	 */
	if (failed) {
		if (*fail_cnt < 1000)
			++(*fail_cnt);
		if (*fail_cnt > 10 && *fail_skip == 0)
			*fail_skip = *fail_cnt / 10;
	} else {
		if (*fail_cnt)
			*fail_cnt = 0;
		if (*fail_skip)
			*fail_skip = 0;
	}
}

/*
 * __wt_compression_skip_next --
 *	Return if we should skip the next compression attempt.
 */
static inline bool
__wt_compression_skip_next(uint32_t *fail_skip)
{
	uint32_t v;

	if (*fail_skip == 0)
		return (false);

	while ((v = *fail_skip) > 0)
		if (__wt_atomic_casv32(fail_skip, v, v - 1))
			break;
	return (true);
}
