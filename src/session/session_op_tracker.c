/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_session_op_tracker_create_entry --
 *	Setup an entry in the operation tracker queue.
 */
int
__wt_session_op_tracker_create_entry(
    WT_SESSION_IMPL *session, uint32_t type,
    uint32_t api_boundary, WT_OP_TRACKER_ENTRY **entryp)
{
	WT_OP_TRACKER_ENTRY *entry, *prev_entry;

	*entryp = NULL;
	if (F_ISSET(session, WT_SESSION_INTERNAL))
		return (0);

	/* Don't capture tracing unless we are recording for the API */
	if (!api_boundary && TAILQ_EMPTY(&session->op_trackerq))
		return (0);

	/*
	 * Don't capture WT_CONNECTION methods (they don't have a reliable
	 * session handle). It would be nice to have a different way to capture
	 * this. An alternative would be to have an exclude list built via
	 * op_track.py. In the mean time this works.
	 */
	if (type >= WT_OP_TYPE_WT_CONNECTION_async_flush &&
	    type <= WT_OP_TYPE_WT_CONNECTION_get_extension_api)
		return (0);

	WT_ASSERT(session, session->api_call_depth == 0 ||
	    !TAILQ_EMPTY(&session->op_trackerq));

	/*
	 * If we are entering the first time via a public API clear any
	 * tracked operations from the previous API call. Track the call depth
	 * since WiredTiger uses API calls internally.
	 */
	if (api_boundary && session->api_call_depth == 0)
		WT_RET(__wt_session_op_tracker_clear(session));

	if ((entry = TAILQ_FIRST(&session->op_tracker_availq)) != NULL) {
		TAILQ_REMOVE(&session->op_tracker_availq, entry, aq);
	} else
		WT_RET(__wt_calloc_one(session, &entry));

	/* We have a new entry object now, start setting it up. */
	WT_RET(__wt_epoch(session, &entry->start));
	memcpy(&entry->last_start, &entry->start, sizeof(struct timespec));
	entry->api_boundary = api_boundary;
	entry->depth = 1;
	TAILQ_INSERT_TAIL(&session->op_trackerq, entry, q);

	/*
	 * Find the parent for this entry. It is the last entry in the queue
	 * that hasn't been finished.
	 */
	for (prev_entry = TAILQ_PREV(entry, __op_tracker, q);
	    prev_entry != NULL;
	    prev_entry = TAILQ_PREV(prev_entry, __op_tracker, q)) {
		if (prev_entry->done)
			continue;
		WT_ASSERT(session, type != 1009 || prev_entry->type != 11);
		entry->start_offset_ns =
		    WT_TIMEDIFF(entry->start, prev_entry->start);
		entry->depth = prev_entry->depth + 1;
		prev_entry->self_time_ns +=
		    WT_TIMEDIFF(entry->start, prev_entry->last_start);
		break;
	}

	/* Create and insert the first entry into the slow op tracker. */
	entry->type = type;
	*entryp = entry;

	if (api_boundary)
		++session->api_call_depth;

	WT_ASSERT(session,
	    !TAILQ_EMPTY(&session->op_trackerq));

	return (0);
}
/*
 * __wt_session_op_tracker_finish_entry --
 *	Finalize an entry.
 */
int
__wt_session_op_tracker_finish_entry(
    WT_SESSION_IMPL *session, WT_OP_TRACKER_ENTRY *entry)
{
	WT_OP_TRACKER_ENTRY *prev_entry;

	/*
	 * Don't track operations completed by internal sessions. It's also
	 * OK to ignore calls with a NULL entry - they are operations that
	 * weren't fully created, and we handle finish with a NULL entry to
	 * avoid complicating callers code.
	 */
	if (F_ISSET(session, WT_SESSION_INTERNAL) || entry == NULL)
		return (0);

	/*
	 * Special case for connection close - skip handles that are no longer
	 * valid. It would be nice to capture this elsewhere, but automatically
	 * capturing all API calls makes that non-trivial.
	 */
	if (session->iface.connection == NULL)
		return (0);

	if (entry->api_boundary)
		--session->api_call_depth;

	WT_RET(__wt_epoch(session, &entry->end));
	entry->done = 1;
	/*
	 * Set a timer in the parent if this operation is nested to facilitate
	 * tracking self time.
	 */
	for (prev_entry = TAILQ_PREV(entry, __op_tracker, q);
	    prev_entry != NULL;
	    prev_entry = TAILQ_PREV(prev_entry, __op_tracker, q)) {
		if (prev_entry->done)
			continue;
		memcpy(&prev_entry->last_start,
		    &entry->end, sizeof(struct timespec));
		break;
	}

	/* Reporting is done as we are returning from the API call. */
	if (entry->api_boundary && session->api_call_depth == 0) {
		/* Update the self timer since this is the end of the trace. */
		entry->self_time_ns +=
		    WT_TIMEDIFF(entry->end, entry->last_start);
		WT_RET(__wt_session_op_tracker_dump(
		    session, session->op_trace_min));
	}

	WT_ASSERT(session, session->api_call_depth == 0 ||
	    !TAILQ_EMPTY(&session->op_trackerq));
	return (0);
}

/*
 * __wt_session_op_tracker_clear --
 *	Clear the slow op tracker. That is move all entries from the
 *	active queue to the available queue.
 */
int
__wt_session_op_tracker_clear(WT_SESSION_IMPL *session)
{
	WT_OP_TRACKER_ENTRY *entry;

	WT_ASSERT(session, session->api_call_depth == 0);
	while ((entry = TAILQ_FIRST(&session->op_trackerq)) != NULL) {
		TAILQ_REMOVE(&session->op_trackerq, entry, q);
		if (entry->msg != NULL)
			__wt_scr_free(session, &entry->msg);
		memset(entry, 0, sizeof(WT_OP_TRACKER_ENTRY));
		TAILQ_INSERT_TAIL(&session->op_tracker_availq, entry, aq);
	}

	return (0);
}

/*
 * __wt_session_op_tracker_destroy --
 *	Free all memory associated with the slow operation tracker.
 */
int
__wt_session_op_tracker_destroy(WT_SESSION_IMPL *session)
{
	WT_OP_TRACKER_ENTRY *entry;

	TAILQ_FOREACH(entry, &session->op_trackerq, q) {
		if (entry->msg != NULL)
			__wt_scr_free(session, &entry->msg);
		__wt_free(session, entry);
	}
	TAILQ_FOREACH(entry, &session->op_tracker_availq, aq) {
		if (entry->msg != NULL)
			__wt_scr_free(session, &entry->msg);
		__wt_free(session, entry);
	}

	return (0);
}

/*
 * __wt_session_op_tracker_setup --
 *	Setup the slow operation tracking mechanism for this session.
 */
int
__wt_session_op_tracker_setup(WT_SESSION_IMPL *session)
{
	TAILQ_INIT(&session->op_trackerq);
	TAILQ_INIT(&session->op_tracker_availq);

	return (0);
}

/*
 * __wt_session_op_tracker_dump --
 *	Write the tracking information for the last operation to the configured
 *	message log.
 */
int
__wt_session_op_tracker_dump(WT_SESSION_IMPL *session, uint64_t min_time)
{
	WT_DECL_RET;
	WT_ITEM *buffer;
	WT_OP_TRACKER_ENTRY *entry;
	uint64_t cumulative_skipped_time, optime_ns, self_time_ns;

	cumulative_skipped_time = 0;

	if ((entry = TAILQ_FIRST(&session->op_trackerq)) == NULL)
		return (0);

	/*
	 * The first entry records the entire time for the operation. Only
	 * proceed if the operation was slow enough.
	 */
	optime_ns = WT_TIMEDIFF(entry->end, entry->start);
	if (min_time != 0 && optime_ns < min_time * WT_MILLION)
		return (0);

	WT_RET(__wt_scr_alloc(session, 1024, &buffer));
	WT_ERR(__wt_buf_catfmt(session, buffer,
	    "{\n\"slow_op\" : [\n"));
	TAILQ_FOREACH(entry, &session->op_trackerq, q) {
		WT_ASSERT(session, entry->done);
		optime_ns = WT_TIMEDIFF(entry->end, entry->start);
		self_time_ns = entry->self_time_ns + cumulative_skipped_time;
		cumulative_skipped_time = 0;
		WT_ERR(__wt_buf_catfmt(session, buffer, "{\n"));
		WT_ERR(__wt_buf_catfmt(session, buffer,
		    "\"elapsed\" : %" PRIu64 ",\n", optime_ns / WT_MILLION));
		WT_ERR(__wt_buf_catfmt(session, buffer,
		    "\"self_time\" : %" PRIu64 ",\n",
		    self_time_ns / WT_MILLION));
		WT_ERR(__wt_buf_catfmt(session, buffer,
		    "\"parent_offset\" : %" PRIu64 ",\n",
		    entry->start_offset_ns / WT_MILLION));
		WT_ERR(__wt_buf_catfmt(session, buffer,
		    "\"nesting\" : %" PRIu32 ",\n", entry->depth));
		WT_ERR(__wt_buf_catfmt(session, buffer,
		    "\"type\" : %" PRIu32 ",\n", entry->type));
		if (entry->msg != NULL)
			WT_ERR(__wt_buf_catfmt(session,
			    buffer, "\"msg\" : %.*s\n",
			    (int)entry->msg->size,
			    (const char *)entry->msg->mem));
		WT_ERR(__wt_buf_catfmt(session, buffer, "},\n"));
	}
	WT_ERR(__wt_buf_catfmt(session, buffer, "]\n}\n"));

	WT_ERR(__wt_msg(session, "%s", (const char *)buffer->mem));
err:	__wt_scr_free(session, &buffer);
	return (ret);
}
