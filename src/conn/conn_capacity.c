/*
 * Copyright (c) 2014-2018 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * If we're being asked to sleep a short amount of time, ignore it.
 * A non-zero value means there may be a temporary violation of the
 * capacity limitation, but one that would even out. That is, possibly
 * fewer sleeps with the risk of more choppy behavior as this number
 * is larger.
 */
#define	WT_CAPACITY_PCT			10
#define	WT_CAPACITY_SLEEP_CUTOFF_US	100

/*
 * When given a total capacity, divide it up for each subsystem. We allow
 * and expect the sum of the subsystems to exceed 100.
 * We aim for:
 *    checkpoint: 10% of total
 *    eviction: 50% of total
 *    log:      25% of total
 *    reads:    50% of total
 */
#define	WT_CAPACITY(total, pct)	((total) * (pct) / 100)

#define	WT_CAP_CKPT		10
#define	WT_CAP_EVICT		50
#define	WT_CAP_LOG		25
#define	WT_CAP_READ		50

#define	WT_CAPACITY_CHK(v, str)	do {				\
	if ((v) != 0 && (v) < WT_THROTTLE_MIN)			\
		WT_RET_MSG(session, EINVAL,			\
		    "%s I/O capacity value %" PRId64		\
		    " below minimum %d",			\
		    str, v, WT_THROTTLE_MIN);			\
} while (0)

/*
 * __capacity_config --
 *	Set I/O capacity configuration.
 */
static int
__capacity_config(WT_SESSION_IMPL *session, const char *cfg[])
{
	WT_CONFIG_ITEM cval;
	WT_CONNECTION_IMPL *conn;
	uint64_t allocation, share, total;
	int64_t excess;
	uint32_t excess_shares;
	bool eviction_use_excess, log_use_excess, read_use_excess;

	conn = S2C(session);

	WT_RET(__wt_config_gets(session, cfg, "io_capacity.checkpoint", &cval));
	WT_CAPACITY_CHK(cval.val, "checkpoint");
	conn->capacity_ckpt = (uint64_t)cval.val;
	WT_RET(__wt_config_gets(session, cfg, "io_capacity.eviction", &cval));
	WT_CAPACITY_CHK(cval.val, "eviction");
	conn->capacity_evict = (uint64_t)cval.val;
	WT_RET(__wt_config_gets(session, cfg, "io_capacity.log", &cval));
	WT_CAPACITY_CHK(cval.val, "log");
	conn->capacity_log = (uint64_t)cval.val;
	WT_RET(__wt_config_gets(session, cfg, "io_capacity.read", &cval));
	WT_CAPACITY_CHK(cval.val, "read");
	conn->capacity_read = (uint64_t)cval.val;
	WT_RET(__wt_config_gets(session, cfg, "io_capacity.total", &cval));
	WT_CAPACITY_CHK(cval.val, "total");
	conn->capacity_total = total = (uint64_t)cval.val;

	if (total != 0) {
		eviction_use_excess = log_use_excess = read_use_excess = false;
		excess = 0;
		excess_shares = 0;
		/*
		 * If we've been given a total capacity, then set the
		 * capacity of any subsystem that hasn't been set.
		 *
		 * If we've been given a total capacity, as well as subsystem
		 * capacities, and a subsystem is not configured up to the
		 * percentage we planned to allocate, then we'll track any
		 * excess that we can later divide up.
		 */
		allocation = WT_CAPACITY(total, WT_CAP_CKPT);
		if (conn->capacity_ckpt == 0)
			conn->capacity_ckpt = allocation;
		else
			excess += (int64_t)allocation -
			    (int64_t)conn->capacity_ckpt;

		allocation = WT_CAPACITY(total, WT_CAP_EVICT);
		if (conn->capacity_evict == 0) {
			conn->capacity_evict = allocation;
			eviction_use_excess = true;
			excess_shares += WT_CAP_EVICT;
		} else
			excess += (int64_t)allocation -
			    (int64_t)conn->capacity_evict;

		allocation = WT_CAPACITY(total, WT_CAP_LOG);
		if (conn->capacity_log == 0) {
			conn->capacity_log = allocation;
			log_use_excess = true;
			excess_shares += WT_CAP_LOG;
		} else
			excess += (int64_t)allocation -
			    (int64_t)conn->capacity_log;

		allocation = WT_CAPACITY(total, WT_CAP_READ);
		if (conn->capacity_read == 0) {
			conn->capacity_read = allocation;
			read_use_excess = true;
			excess_shares += WT_CAP_READ;
		} else
			excess += (int64_t)allocation -
			    (int64_t)conn->capacity_read;

		/*
		 * Now we've set up the allocations, but we may have
		 * excess we can spread around.  We don't give checkpoint
		 * any extra, we keep it at 10% or whatever was specified.
		 * The other subsystems, if they were not constrained, get
		 * extra shares in proportion to the general goals above.
		 */
		if (excess_shares > 0 && excess > 0) {
			share = (uint64_t)excess / excess_shares;
			if (eviction_use_excess)
				conn->capacity_evict += share * WT_CAP_EVICT;
			if (read_use_excess)
				conn->capacity_read += share * WT_CAP_READ;
			if (log_use_excess)
				conn->capacity_log += share * WT_CAP_LOG;
		}
	}

	/*
	 * Set the threshold to the percent of our capacity to periodically
	 * asynchronously flush what we've written.
	 */
	conn->capacity_threshold = (conn->capacity_ckpt +
	    conn->capacity_evict + conn->capacity_log) / 100 *
	    WT_CAPACITY_PCT;
	WT_STAT_CONN_SET(session, capacity_threshold, conn->capacity_threshold);

	return (0);
}

/*
 * __capacity_server_run_chk --
 *	Check to decide if the capacity server should continue running.
 */
static bool
__capacity_server_run_chk(WT_SESSION_IMPL *session)
{
	return (F_ISSET(S2C(session), WT_CONN_SERVER_CAPACITY));
}

/*
 * __capacity_server --
 *	The capacity server thread.
 */
static WT_THREAD_RET
__capacity_server(void *arg)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	session = arg;
	conn = S2C(session);
	for (;;) {
		/*
		 * Wait until signalled but check once per second in case
		 * the signal was missed.
		 */
		__wt_cond_wait(session, conn->capacity_cond,
		    WT_MILLION, __capacity_server_run_chk);

		/* Check if we're quitting or being reconfigured. */
		if (!__capacity_server_run_chk(session))
			break;

		conn->capacity_signalled = false;
		if (conn->capacity_written > conn->capacity_threshold)
			WT_ERR(__wt_fsync_all_background(session));
	}

	if (0) {
err:		WT_PANIC_MSG(session, ret, "capacity server error");
	}
	return (WT_THREAD_RET_VALUE);
}

/*
 * __capacity_server_start --
 *	Start the capacity server thread.
 */
static int
__capacity_server_start(WT_CONNECTION_IMPL *conn)
{
	WT_SESSION_IMPL *session;

	/* Nothing to do if the server is already running. */
	if (conn->capacity_session != NULL)
		return (0);

	F_SET(conn, WT_CONN_SERVER_CAPACITY);

	/*
	 * The capacity server gets its own session.
	 */
	WT_RET(__wt_open_internal_session(conn,
	    "capacity-server", false, 0, &conn->capacity_session));
	session = conn->capacity_session;

	WT_RET(__wt_cond_alloc(session,
	    "capacity server", &conn->capacity_cond));

	/*
	 * Start the thread.
	 */
	WT_RET(__wt_thread_create(
	    session, &conn->capacity_tid, __capacity_server, session));
	conn->capacity_tid_set = true;

	return (0);
}

/*
 * __wt_capacity_server_create --
 *	Configure and start the capacity server.
 */
int
__wt_capacity_server_create(WT_SESSION_IMPL *session, const char *cfg[])
{
	WT_CONNECTION_IMPL *conn;

	conn = S2C(session);

	/*
	 * If it is a read only connection there is nothing to do.
	 */
	if (F_ISSET(conn, WT_CONN_READONLY))
		return (0);

	/*
	 * Stop any server that is already running. This means that each time
	 * reconfigure is called we'll bounce the server even if there are no
	 * configuration changes. This makes our life easier as the underlying
	 * configuration routine doesn't have to worry about freeing objects
	 * in the connection structure (it's guaranteed to always start with a
	 * blank slate), and we don't have to worry about races where a running
	 * server is reading configuration information that we're updating, and
	 * it's not expected that reconfiguration will happen a lot.
	 */
	if (conn->capacity_session != NULL)
		WT_RET(__wt_capacity_server_destroy(session));

	WT_RET(__capacity_config(session, cfg));
	if (conn->capacity_threshold != 0)
		WT_RET(__capacity_server_start(conn));

	return (0);
}

/*
 * __wt_capacity_server_destroy --
 *	Destroy the capacity server thread.
 */
int
__wt_capacity_server_destroy(WT_SESSION_IMPL *session)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_SESSION *wt_session;

	conn = S2C(session);

	F_CLR(conn, WT_CONN_SERVER_CAPACITY);
	if (conn->capacity_tid_set) {
		__wt_cond_signal(session, conn->capacity_cond);
		WT_TRET(__wt_thread_join(session, &conn->capacity_tid));
		conn->capacity_tid_set = false;
	}
	__wt_cond_destroy(session, &conn->capacity_cond);

	/* Close the server thread's session. */
	if (conn->capacity_session != NULL) {
		wt_session = &conn->capacity_session->iface;
		WT_TRET(wt_session->close(wt_session, NULL));
	}

	/*
	 * Ensure capacity settings are cleared - so that reconfigure doesn't
	 * get confused.
	 */
	conn->capacity_session = NULL;
	conn->capacity_tid_set = false;
	conn->capacity_cond = NULL;
	conn->capacity_usecs = 0;

	return (ret);
}

/*
 * __wt_capacity_signal --
 *	Signal the capacity thread if sufficient data has been written.
 */
void
__wt_capacity_signal(WT_SESSION_IMPL *session)
{
	WT_CONNECTION_IMPL *conn;

	conn = S2C(session);
	if (conn->capacity_written >= conn->capacity_threshold &&
	    !conn->capacity_signalled) {
		__wt_cond_signal(session, conn->capacity_cond);
		conn->capacity_signalled = true;
		conn->capacity_written = 0;
	}
}

/*
 * __wt_capacity_throttle --
 *	Reserve a time to perform a write operation for the subsystem,
 * and wait until that time.
 *
 * The concept is that each write to a subsystem reserves a time slot
 * to do its write, and atomically adjusts the reservation marker to
 * point past the reserved slot. The size of the adjustment (i.e. the
 * length of time represented by the slot in nanoseconds) is chosen to
 * be proportional to the number of bytes to be written, and the
 * proportion is a simple calculation so that we can fit reservations for
 * exactly the configured capacity in a second. Reservation times are
 * in nanoseconds since the epoch.
 */
void
__wt_capacity_throttle(WT_SESSION_IMPL *session, uint64_t bytes,
    WT_THROTTLE_TYPE type)
{
	struct timespec now;
	WT_CONNECTION_IMPL *conn;
	uint64_t capacity, ckpt, now_ns, sleep_us, total;
	uint64_t new_res_len, new_res_value, res_len, res_value;
	uint64_t *reservation;

	capacity = 0;
	reservation = NULL;
	conn = S2C(session);

	switch (type) {
	case WT_THROTTLE_CKPT:
		capacity = conn->capacity_ckpt;
		reservation = &conn->reservation_ckpt;
		WT_STAT_CONN_INCR(session, capacity_ckpt_calls);
		break;
	case WT_THROTTLE_EVICT:
		capacity = conn->capacity_evict;
		reservation = &conn->reservation_evict;
		WT_STAT_CONN_INCR(session, capacity_evict_calls);
		break;
	case WT_THROTTLE_LOG:
		capacity = conn->capacity_log;
		reservation = &conn->reservation_log;
		WT_STAT_CONN_INCR(session, capacity_log_calls);
		break;
	case WT_THROTTLE_READ:
		capacity = conn->capacity_read;
		reservation = &conn->reservation_read;
		WT_STAT_CONN_INCR(session, capacity_read_calls);
		break;
	}

	__wt_verbose(session, WT_VERB_TEMPORARY,
	    "THROTTLE: type %d bytes %" PRIu64 " capacity %" PRIu64
	    "  reservation %" PRIu64,
	    (int)type, bytes, capacity, *reservation);
	if (capacity == 0 || F_ISSET(conn, WT_CONN_RECOVERING))
		return;

	/*
	 * There may in fact be some reads done under the umbrella of log
	 * I/O, but they are mostly done under recovery. And if we are
	 * recovering, we don't reach this code.
	 */
	if (type != WT_THROTTLE_READ) {
		/* Sizes larger than this may overflow */
		conn->capacity_written += bytes;
		WT_STAT_CONN_INCRV(session, capacity_bytes_written, bytes);
		__wt_capacity_signal(session);
	} else
		WT_STAT_CONN_INCRV(session, capacity_bytes_read, bytes);
	WT_ASSERT(session, bytes < 16 * (uint64_t)WT_GIGABYTE);

	res_len = (bytes * WT_BILLION) / capacity;
	res_value = __wt_atomic_add64(reservation, res_len);
	__wt_epoch(session, &now);

	/* Convert the current time to nanoseconds since the epoch. */
	now_ns = (uint64_t)now.tv_sec * WT_BILLION + (uint64_t)now.tv_nsec;
	__wt_verbose(session, WT_VERB_TEMPORARY,
	    "THROTTLE: len %" PRIu64 " reservation %" PRIu64 " now %" PRIu64,
	    res_len, res_value, now_ns);

	/*
	 * If the reservation time we got is far enough in the future, see if
	 * stealing a reservation from the checkpoint subsystem makes sense.
	 * This is allowable if there is not currently a checkpoint and
	 * the checkpoint system is configured to have a capacity.
	 */
	if (res_value > now_ns && res_value - now_ns > 100000 &&
	    type != WT_THROTTLE_LOG && !conn->txn_global.checkpoint_running &&
	    (ckpt = conn->capacity_ckpt) != 0) {
		new_res_len = (bytes * WT_BILLION) / ckpt;
		new_res_value = __wt_atomic_add64(
		    &conn->reservation_ckpt, new_res_len);

		/*
		 * If the checkpoint reservation is a better deal (that is,
		 * if we'll sleep for less time), shuffle values so it is
		 * used instead. In either case, we 'return' the reservation
		 * that we aren't using.
		 */
		if (new_res_value < res_value) {
			res_value = new_res_value;
			reservation = &conn->reservation_ckpt;
			capacity = ckpt;
			res_value = __wt_atomic_sub64(reservation, res_len);
		} else
			(void)__wt_atomic_sub64(
			    &conn->reservation_ckpt, new_res_len);
	}
	if (res_value > now_ns) {
		sleep_us = (res_value - now_ns) / WT_THOUSAND;
		__wt_verbose(session, WT_VERB_TEMPORARY,
		    "THROTTLE: SLEEP sleep us %" PRIu64,
		    sleep_us);
		if (type == WT_THROTTLE_CKPT) {
			WT_STAT_CONN_INCR(session, capacity_ckpt_throttles);
			WT_STAT_CONN_INCRV(session,
			    capacity_ckpt_time, sleep_us);
		} else if (type == WT_THROTTLE_EVICT) {
			WT_STAT_CONN_INCR(session, capacity_evict_throttles);
			WT_STAT_CONN_INCRV(session,
			    capacity_evict_time, sleep_us);
		} else if (type == WT_THROTTLE_LOG) {
			WT_STAT_CONN_INCR(session, capacity_log_throttles);
			WT_STAT_CONN_INCRV(session,
			    capacity_log_time, sleep_us);
		} else if (type == WT_THROTTLE_READ) {
			WT_STAT_CONN_INCR(session, capacity_read_throttles);
			WT_STAT_CONN_INCRV(session,
			    capacity_read_time, sleep_us);
		}
		if (sleep_us > WT_CAPACITY_SLEEP_CUTOFF_US) {
			/* Sleep handles large usec values. */
			__wt_sleep(0, sleep_us);

			/* Adjust our idea of 'now', we'll be using it again. */
			now_ns = res_value;
		}
	} else if (now_ns - res_value > WT_BILLION) {
		/*
		 * If it looks like the reservation clock is out of date by more
		 * than a second, bump it up within a second of the current
		 * time. Basically we don't allow a lot of current bandwidth to
		 * 'make up' for long lulls in the past.  Note that this
		 * may race with other threads, with the last one 'winning'.
		 * But it doesn't really matter, the one second catch up
		 * is approximate.
		 */
		__wt_verbose(session, WT_VERB_TEMPORARY,
		    "THROTTLE: ADJ available %" PRIu64 " capacity %" PRIu64
		    " adjustment %" PRIu64,
		    now_ns - res_value, capacity,
		    now_ns - capacity + res_value);
		__wt_atomic_store64(reservation, now_ns - WT_BILLION + res_len);
	}

	/*
	 * Now, see if we fit under the total capacity given to the
	 * connection.  To do this, repeat the steps above, but using
	 * the total reservation counter and total capacity.
	 */
	if ((total = conn->capacity_total) != 0) {
		res_len = (bytes * WT_BILLION) / total;
		res_value = __wt_atomic_add64(&conn->reservation_total,
		    res_len);

		__wt_verbose(session, WT_VERB_TEMPORARY,
		    "THROTTLE: TOTAL: len %" PRIu64 " reservation %" PRIu64
		    " now %" PRIu64, res_len, res_value, now_ns);

		if (res_value > now_ns) {
			sleep_us = (res_value - now_ns) / WT_THOUSAND;
			__wt_verbose(session, WT_VERB_TEMPORARY,
			    "THROTTLE: TOTAL: SLEEP sleep us %" PRIu64,
			    sleep_us);
			WT_STAT_CONN_INCR(session, capacity_total_throttles);
			WT_STAT_CONN_INCRV(session,
			    capacity_total_time, sleep_us);
			if (sleep_us > WT_CAPACITY_SLEEP_CUTOFF_US)
				/* Sleep handles large usec values. */
				__wt_sleep(0, sleep_us);
		} else if (now_ns - res_value > WT_BILLION) {
			/*
			 * If the total reservation clock is out of date,
			 * bring it to within a second of a current time.
			 */
			__wt_verbose(session, WT_VERB_TEMPORARY,
			    "THROTTLE: TOTAL: ADJ available %" PRIu64
			    " capacity %" PRIu64 " adjustment %" PRIu64,
			    now_ns - res_value, total,
			    now_ns - total + res_value);
			__wt_atomic_store64(&conn->reservation_total,
			    now_ns - WT_BILLION + res_len);
		}
	}

	__wt_verbose(session, WT_VERB_TEMPORARY,
	    "THROTTLE: DONE reservation %" PRIu64, *reservation);
}
