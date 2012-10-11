/*-
 * Copyright (c) 2008-2012 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "format.h"

static int
handle_message(WT_EVENT_HANDLER *handler, const char *message)
{
	UNUSED(handler);

	if (g.logfp != NULL)
		return (fprintf(g.logfp, "%s\n", message) < 0 ? -1 : 0);

	return (printf("%s\n", message) < 0 ? -1 : 0);
}

/*
 * __handle_progress_default --
 *	Default WT_EVENT_HANDLER->handle_progress implementation: ignore.
 */
static int
handle_progress(
    WT_EVENT_HANDLER *handler, const char *operation, uint64_t progress)
{
	UNUSED(handler);

	track(operation, progress, NULL);
	return (0);
}

static WT_EVENT_HANDLER event_handler = {
	NULL,
	handle_message,
	handle_progress
};

void
wts_open(void)
{
	WT_CONNECTION *conn;
	WT_SESSION *session;
	uint32_t maxintlpage, maxintlitem, maxleafpage, maxleafitem;
	int ret;
	const char *ext1, *ext2;
	char config[512], *end, *p;

	/* If the bzip2 compression module has been built, use it. */
#define	EXTPATH	"../../ext"
	ext1 = EXTPATH "compressors/bzip2_compress/.libs/bzip2_compress.so";
	if (access(ext1, R_OK) != 0) {
		ext1 = "";
		g.c_bzip = 0;
	}
	ext2 = EXTPATH "/collators/reverse/.libs/reverse_collator.so";

	/*
	 * Open configuration -- put command line configuration options at the
	 * end so they can override "standard" configuration.
	 */
	snprintf(config, sizeof(config),
	    "create,error_prefix=\"%s\",cache_size=%" PRIu32 "MB,sync=false,"
	    "extensions=[\"%s\",\"%s\"],%s",
	    g.progname, g.c_cache, ext1, ext2,
	    g.config_open == NULL ? "" : g.config_open);

	if ((ret =
	    wiredtiger_open("RUNDIR", &event_handler, config, &conn)) != 0)
		die(ret, "wiredtiger_open");

	if ((ret = conn->open_session(conn, NULL, NULL, &session)) != 0)
		die(ret, "connection.open_session");

	maxintlpage = 1U << g.c_intl_page_max;
	/* Make sure at least 2 internal page per thread can fit in cache. */
	while (2 * g.c_threads * maxintlpage > g.c_cache << 20)
		maxintlpage >>= 1;
	maxintlitem = MMRAND(maxintlpage / 50, maxintlpage / 40);
	if (maxintlitem < 40)
		maxintlitem = 40;
	maxleafpage = 1U << g.c_leaf_page_max;
	/* Make sure at least one leaf page per thread can fit in cache. */
	while (g.c_threads * (maxintlpage + maxleafpage) > g.c_cache << 20)
		maxleafpage >>= 1;
	maxleafitem = MMRAND(maxleafpage / 50, maxleafpage / 40);
	if (maxleafitem < 40)
		maxleafitem = 40;

	p = config;
	end = config + sizeof(config);
	p += snprintf(p, (size_t)(end - p),
	    "key_format=%s,"
	    "internal_page_max=%d,internal_item_max=%d,"
	    "leaf_page_max=%d,leaf_item_max=%d",
	    (g.type == ROW) ? "u" : "r",
	    maxintlpage, maxintlitem, maxleafpage, maxleafitem);

	if (g.c_bzip)
		p += snprintf(p, (size_t)(end - p),
		    ",block_compressor=\"bzip2_compress\"");

	switch (g.type) {
	case FIX:
		p += snprintf(p, (size_t)(end - p),
		    ",value_format=%dt", g.c_bitcnt);
		break;
	case ROW:
		if (g.c_huffman_key)
			p += snprintf(p, (size_t)(end - p),
			    ",huffman_key=english");
		if (g.c_reverse)
			p += snprintf(p, (size_t)(end - p),
			    ",collator=reverse");
		/* FALLTHROUGH */
	case VAR:
		if (g.c_huffman_value)
			p += snprintf(p, (size_t)(end - p),
			    ",huffman_value=english");
		if (g.c_dictionary)
			p += snprintf(p, (size_t)(end - p),
			    ",dictionary=%d", MMRAND(123, 517));
		break;
	}

	if ((ret = session->create(session, g.uri, config)) != 0)
		die(ret, "session.create: %s", g.uri);

	if ((ret = session->close(session, NULL)) != 0)
		die(ret, "session.close");

	g.wts_conn = conn;
}

void
wts_close()
{
	WT_CONNECTION *conn;
	int ret;

	conn = g.wts_conn;

	if ((ret = conn->close(conn, NULL)) != 0)
		die(ret, "connection.close");
}

void
wts_dump(const char *tag, int dump_bdb)
{
	int offset, ret;
	char cmd[256];

	track("dump files and compare", 0ULL, NULL);
	offset = snprintf(cmd, sizeof(cmd), "sh s_dumpcmp");
	if (dump_bdb)
		offset += snprintf(cmd + offset,
		    sizeof(cmd) - (size_t)offset, " -b");
	if (g.type == FIX || g.type == VAR)
		offset += snprintf(cmd + offset,
		    sizeof(cmd) - (size_t)offset, " -c");

	if (g.uri != NULL)
		offset += snprintf(cmd + offset,
		    sizeof(cmd) - (size_t)offset, " -n %s", g.uri);
	if ((ret = system(cmd)) != 0)
		die(ret, "%s: dump comparison failed", tag);
}

void
wts_salvage(void)
{
	WT_CONNECTION *conn;
	WT_SESSION *session;
	int ret;

	conn = g.wts_conn;

	track("salvage", 0ULL, NULL);

	/*
	 * Save a copy of the interesting files so we can replay the salvage
	 * step as necessary.
	 */
	if ((ret = system(
	    "cd RUNDIR && "
	    "rm -rf slvg.copy && "
	    "mkdir slvg.copy && "
	    "cp WiredTiger* wt* slvg.copy/")) != 0)
		die(ret, "salvage copy step failed");

	if ((ret = conn->open_session(conn, NULL, NULL, &session)) != 0)
		die(ret, "connection.open_session");
	if ((ret = session->salvage(session, g.uri, NULL)) != 0)
		die(ret, "session.salvage: %s", g.uri);
	if ((ret = session->close(session, NULL)) != 0)
		die(ret, "session.close");
}

void
wts_verify(const char *tag)
{
	WT_CONNECTION *conn;
	WT_SESSION *session;
	int ret;

	conn = g.wts_conn;

	track("verify", 0ULL, NULL);

	if ((ret = conn->open_session(conn, NULL, NULL, &session)) != 0)
		die(ret, "connection.open_session");
	if (g.logging != 0)
		(void)session->msg_printf(session,
		    "=============== verify start ===============");
	if ((ret = session->verify(session, g.uri, NULL)) != 0)
		die(ret, "session.verify: %s: %s", g.uri, tag);
	if (g.logging != 0)
		(void)session->msg_printf(session,
		    "=============== verify stop ===============");
	if ((ret = session->close(session, NULL)) != 0)
		die(ret, "session.close");
}

/*
 * wts_stats --
 *	Dump the run's statistics.
 */
void
wts_stats(void)
{
	WT_CONNECTION *conn;
	WT_CURSOR *cursor;
	WT_SESSION *session;
	FILE *fp;
	char *stat_name;
	const char *pval, *desc;
	uint64_t v;
	int ret;

	track("stat", 0ULL, NULL);

	conn = g.wts_conn;
	if ((ret = conn->open_session(conn, NULL, NULL, &session)) != 0)
		die(ret, "connection.open_session");

	if ((fp = fopen("RUNDIR/stats", "w")) == NULL)
		die(errno, "fopen: RUNDIR/stats");

	/* Connection statistics. */
	fprintf(fp, "====== Connection statistics:\n");
	if ((ret = session->open_cursor(session,
	    "statistics:", NULL, NULL, &cursor)) != 0)
		die(ret, "session.open_cursor");

	while ((ret = cursor->next(cursor)) == 0 &&
	    (ret = cursor->get_value(cursor, &desc, &pval, &v)) == 0)
		if (fprintf(fp, "%s=%s\n", desc, pval) < 0)
			die(errno, "fprintf");

	if (ret != WT_NOTFOUND)
		die(ret, "cursor.next");
	if ((ret = cursor->close(cursor)) != 0)
		die(ret, "cursor.close");

	/*
	 * XXX
	 * WiredTiger only supports file object statistics.
	 */
	if (strcmp(g.c_data_source, "file") != 0)
		goto skip;

	/* File statistics. */
	fprintf(fp, "\n\n====== File statistics:\n");
	if ((stat_name =
	    malloc(strlen("statistics:") + strlen(g.uri) + 1)) == NULL)
		syserr("malloc");
	sprintf(stat_name, "statistics:%s", g.uri);
	if ((ret = session->open_cursor(
	    session, stat_name, NULL, NULL, &cursor)) != 0)
		die(ret, "session.open_cursor");
	free(stat_name);

	while ((ret = cursor->next(cursor)) == 0 &&
	    (ret = cursor->get_value(cursor, &desc, &pval, &v)) == 0)
		if (fprintf(fp, "%s=%s\n", desc, pval) < 0)
			die(errno, "fprintf");

	if (ret != WT_NOTFOUND)
		die(ret, "cursor.next");
	if ((ret = cursor->close(cursor)) != 0)
		die(ret, "cursor.close");

skip:	if ((ret = fclose(fp)) != 0)
		die(ret, "fclose");

	if ((ret = session->close(session, NULL)) != 0)
		die(ret, "session.close");
}
