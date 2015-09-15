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
    WT_SESSION_IMPL *session, uint32_t type, WT_OP_TRACKER_ENTRY **entryp)
{
	WT_OP_TRACKER_ENTRY *entry, *prev_entry;

	if (F_ISSET(session, WT_SESSION_INTERNAL))
		return (0);

	if ((entry = TAILQ_FIRST(&session->op_tracker_availq)) != NULL) {
		TAILQ_REMOVE(&session->op_tracker_availq, entry, aq);
	} else
		WT_RET(__wt_calloc_one(session, &entry));

	WT_RET(__wt_epoch(session, &entry->start));
	TAILQ_INSERT_TAIL(&session->op_trackerq, entry, q);
	/*
	 * Update the self time of the previous entry.
	 * TODO: This is hopefully going to track self time - that will require
	 * tracking a depth in the queue. For now assume the entries aren't
	 * nested.
	 */
	if ((prev_entry = TAILQ_PREV(entry, __op_tracker, q)) != NULL) {
		prev_entry->self_time_us +=
		    WT_TIMEDIFF(prev_entry->last_stop, entry->start);
	}

	/* Create and insert the first entry into the slow op tracker. */
	entry->type = type;
	*entryp = entry;

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

	if (F_ISSET(session, WT_SESSION_INTERNAL))
		return (0);

	/*
	 * Special case for connection close - skip handles that are no longer
	 * valid. It would be nice to capture this elsewhere, but automatically
	 * capturing all API calls makes that non-trivial.
	 */
	if (session->iface.connection == NULL)
		return (0);

	WT_RET(__wt_epoch(session, &entry->end));
	/*
	 * Restart the self timer if there is a previous entry.
	 * TODO: This is hopefully going to track self time - that will require
	 * tracking a depth in the queue. For now assume the entries aren't
	 * nested.
	 */
	if ((prev_entry = TAILQ_PREV(entry, __op_tracker, q)) != NULL) {
		memcpy(&prev_entry->last_stop,
		    &entry->end, sizeof(struct timespec));
	}

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

	while ((entry = TAILQ_FIRST(&session->op_trackerq)) != NULL) {
		TAILQ_REMOVE(&session->op_trackerq, entry, q);
		if (entry->msg != NULL)
			__wt_scr_free(session, &entry->msg);
		memset(entry, 0, sizeof(entry));
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
 *	Wrte the tracking information for the last operation to the configured
 *	message log.
 */
int
__wt_session_op_tracker_dump(WT_SESSION_IMPL *session, uint64_t min_time)
{
	WT_DECL_RET;
	WT_ITEM *buffer;
	WT_OP_TRACKER_ENTRY *entry;
	uint64_t optime;

	if ((entry = TAILQ_FIRST(&session->op_trackerq)) == NULL)
		return (WT_NOTFOUND);

	/*
	 * The first entry records the entire time for the operation. Only
	 * proceed if the operation was slow enough.
	 */
	optime = WT_TIMEDIFF(entry->end, entry->start) / WT_MILLION;
	if (min_time != 0 && optime > min_time)
		return (0);

	WT_RET(__wt_scr_alloc(session, 1024, &buffer));
	WT_ERR(__wt_buf_catfmt(session, buffer, "<Slow operation>"));
	TAILQ_FOREACH(entry, &session->op_trackerq, q) {
		WT_ERR(__wt_buf_catfmt(session, buffer, "<entry>"));
		WT_ERR(__wt_buf_catfmt(session, buffer,
		    "<elapsed>%" PRIu64 "</elapsed>", optime));
		WT_ERR(__wt_buf_catfmt(session, buffer,
		    "<self_time>%" PRIu64 "</self_time>", optime));
		WT_ERR(__wt_buf_catfmt(session, buffer,
		    "<type>%" PRIu32 "</type>", entry->type));
		if (entry->msg != NULL)
			WT_ERR(__wt_buf_catfmt(session,
			    buffer, "<msg>%.*s</msg>",
			    (int)entry->msg->size,
			    (const char *)entry->msg->mem));
		WT_ERR(__wt_buf_catfmt(session, buffer, "</entry>"));
	}
	WT_ERR(__wt_buf_catfmt(session, buffer, "</Slow operation>"));

	WT_ERR(__wt_msg(session, "%s", (const char *)buffer->mem));
err:	__wt_scr_free(session, &buffer);
	return (ret);
}
