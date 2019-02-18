/*-
 * Copyright (c) 2014-2019 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "util.h"

static int list_get_allocsize(WT_SESSION *, const char *, size_t *);
static int list_print(WT_SESSION *, const char *, bool, bool);
static int list_print_checkpoint(WT_SESSION *, const char *);
static int usage(void);

int
util_list(WT_SESSION *session, int argc, char *argv[])
{
	WT_DECL_RET;
	int ch;
	char *uri;
	bool cflag, vflag;

	cflag = vflag = false;
	uri = NULL;
	while ((ch = __wt_getopt(progname, argc, argv, "cv")) != EOF)
		switch (ch) {
		case 'c':
			cflag = true;
			break;
		case 'v':
			vflag = true;
			break;
		case '?':
		default:
			return (usage());
		}
	argc -= __wt_optind;
	argv += __wt_optind;

	switch (argc) {
	case 0:
		break;
	case 1:
		if ((uri = util_uri(session, *argv, "table")) == NULL)
			return (1);
		break;
	default:
		return (usage());
	}

	ret = list_print(session, uri, cflag, vflag);

	free(uri);
	return (ret);
}

/*
 * list_get_allocsize --
 *	Get the allocation size for this file from the metadata.
 */
static int
list_get_allocsize(WT_SESSION *session, const char *key, size_t *allocsize)
{
	WT_CONFIG_ITEM szvalue;
	WT_CONFIG_PARSER *parser;
	WT_DECL_RET;
	WT_EXTENSION_API *wt_api;
	int tret;
	char *config;

	*allocsize = 0;

	wt_api = session->connection->get_extension_api(session->connection);
	if ((ret = wt_api->metadata_search(wt_api, session, key, &config)) != 0)
		return (util_err(
		    session, ret, "%s: WT_EXTENSION_API.metadata_search", key));
	if ((ret = wt_api->config_parser_open(wt_api, session, config,
	    strlen(config), &parser)) != 0)
		return (util_err(
		    session, ret, "WT_EXTENSION_API.config_parser_open"));
	if ((ret = parser->get(parser, "allocation_size", &szvalue)) != 0) {
		if (ret == WT_NOTFOUND)
			ret = 0;
		else
			ret = util_err(session, ret, "WT_CONFIG_PARSER.get");
		if ((tret = parser->close(parser)) != 0)
			(void)util_err(session, tret, "WT_CONFIG_PARSER.close");
		return (ret);
	}
	if ((ret = parser->close(parser)) != 0)
		return (util_err(session, ret, "WT_CONFIG_PARSER.close"));
	*allocsize = (size_t)szvalue.val;
	return (0);
}

/*
 * list_print --
 *	List the high-level objects in the database.
 */
static int
list_print(WT_SESSION *session, const char *uri, bool cflag, bool vflag)
{
	WT_CURSOR *cursor;
	WT_DECL_RET;
	const char *key, *value;
	bool found;

	/* Open the metadata file. */
	if ((ret = session->open_cursor(
	    session, WT_METADATA_URI, NULL, NULL, &cursor)) != 0) {
		/*
		 * If there is no metadata (yet), this will return ENOENT.
		 * Treat that the same as an empty metadata.
		 */
		if (ret == ENOENT)
			return (0);

		return (util_err(session,
		    ret, "%s: WT_SESSION.open_cursor", WT_METADATA_URI));
	}

	found = uri == NULL;
	while ((ret = cursor->next(cursor)) == 0) {
		/* Get the key. */
		if ((ret = cursor->get_key(cursor, &key)) != 0)
			return (util_cerr(cursor, "get_key", ret));

		/*
		 * If a name is specified, only show objects that match.
		 */
		if (uri != NULL) {
			if (!WT_PREFIX_MATCH(key, uri))
				continue;
			found = true;
		}

		/*
		 * !!!
		 * We don't normally say anything about the WiredTiger metadata
		 * and lookaside tables, they're not application/user "objects"
		 * in the database.  I'm making an exception for the checkpoint
		 * and verbose options. However, skip over the metadata system
		 * information for anything except the verbose option.
		 */
		if (!vflag && WT_PREFIX_MATCH(key, WT_SYSTEM_PREFIX))
			continue;
		if (cflag || vflag ||
		    (strcmp(key, WT_METADATA_URI) != 0 &&
		    strcmp(key, WT_LAS_URI) != 0))
			printf("%s\n", key);

		if (!cflag && !vflag)
			continue;

		if (cflag && (ret = list_print_checkpoint(session, key)) != 0)
			return (ret);
		if (vflag) {
			if ((ret = cursor->get_value(cursor, &value)) != 0)
				return (util_cerr(cursor, "get_value", ret));
			printf("%s\n", value);
		}
	}
	if (ret != WT_NOTFOUND)
		return (util_cerr(cursor, "next", ret));
	if (!found) {
		fprintf(stderr, "%s: %s: not found\n", progname, uri);
		return (1);
	}

	return (0);
}

/*
 * list_print_checkpoint --
 *	List the checkpoint information.
 */
static int
list_print_checkpoint(WT_SESSION *session, const char *key)
{
	WT_BLOCK_CKPT ci;
	WT_CKPT *ckpt, *ckptbase;
	WT_DECL_RET;
	size_t allocsize, len;
	time_t t;
	uint64_t v;

	/*
	 * We may not find any checkpoints for this file, in which case we don't
	 * report an error, and continue our caller's loop.  Otherwise, read the
	 * list of checkpoints and print each checkpoint's name and time.
	 */
	if ((ret = __wt_metadata_get_ckptlist(session, key, &ckptbase)) != 0)
		return (ret == WT_NOTFOUND ? 0 : ret);

	/* We need the allocation size for decoding the checkpoint addr */
	if ((ret = list_get_allocsize(session, key, &allocsize)) != 0)
		return (ret);

	/* Find the longest name, so we can pretty-print. */
	len = 0;
	WT_CKPT_FOREACH(ckptbase, ckpt)
		if (strlen(ckpt->name) > len)
			len = strlen(ckpt->name);
	++len;

	memset(&ci, 0, sizeof(ci));
	WT_CKPT_FOREACH(ckptbase, ckpt) {
		if (allocsize != 0 && (ret = __wt_block_ckpt_decode(
		    session, allocsize, ckpt->raw.data, &ci)) != 0) {
			(void)util_err(session, ret, "__wt_block_ckpt_decode");
			/* continue if damaged */
			ci.root_size = 0;
		}
		/*
		 * Call ctime, not ctime_r; ctime_r has portability problems,
		 * the Solaris version is different from the POSIX standard.
		 */
		t = (time_t)ckpt->sec;
		printf("\t%*s: %.24s", (int)len, ckpt->name, ctime(&t));

		v = ckpt->size;
		if (v >= WT_PETABYTE)
			printf(" (%" PRIu64 " PB)\n", v / WT_PETABYTE);
		else if (v >= WT_TERABYTE)
			printf(" (%" PRIu64 " TB)\n", v / WT_TERABYTE);
		else if (v >= WT_GIGABYTE)
			printf(" (%" PRIu64 " GB)\n", v / WT_GIGABYTE);
		else if (v >= WT_MEGABYTE)
			printf(" (%" PRIu64 " MB)\n", v / WT_MEGABYTE);
		else if (v >= WT_KILOBYTE)
			printf(" (%" PRIu64 " KB)\n", v / WT_KILOBYTE);
		else
			printf(" (%" PRIu64 " B)\n", v);
		if (ci.root_size != 0) {
			printf("\t\t" "root offset: %" PRIuMAX
			    " (0x%" PRIxMAX ")\n",
			    (uintmax_t)ci.root_offset,
			    (uintmax_t)ci.root_offset);
			printf("\t\t" "root size: %" PRIu32
			    " (0x%" PRIx32 ")\n",
			    ci.root_size, ci.root_size);
			printf("\t\t" "root checksum: %" PRIu32
			    " (0x%" PRIx32 ")\n",
			    ci.root_checksum, ci.root_checksum);
		}
	}

	__wt_metadata_free_ckptlist(session, ckptbase);
	return (0);
}

static int
usage(void)
{
	(void)fprintf(stderr,
	    "usage: %s %s "
	    "list [-cv] [uri]\n",
	    progname, usage_prefix);
	return (1);
}
