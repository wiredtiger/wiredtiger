/*-
 * Copyright (c) 2014-2019 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "util.h"

/*
 * load_metadata --
 *	Retrieve the file's metadata information.
 */
static int
load_metadata(WT_SESSION *wt_session, const char *path, char **metadatap)
{
	WT_DECL_RET;
	WT_FH *fh;
	WT_SESSION_IMPL *session;

	session = (WT_SESSION_IMPL *)wt_session;
	fh = NULL;

	WT_ERR(__wt_open(session, path,
	   WT_FS_OPEN_FILE_TYPE_REGULAR,
	   WT_FS_OPEN_FIXED | WT_FS_OPEN_READONLY | WT_FS_OPEN_ACCESS_SEQ,
	   &fh));

	WT_ERR(__wt_desc_read(session, fh, 0, path, metadatap));

	if (*metadatap == NULL)
		WT_ERR(util_err(wt_session, 0,
		    "%s: no metadata information available", path));

err:	WT_TRET(__wt_close(session, &fh));

	return (ret);
}

/*
 * insert_metadata --
 *	Insert the metadata into the database.
 */
static int
insert_metadata(
    WT_SESSION *session, const char *path, const char *filename, char *metadata)
{
	WT_CURSOR *cursor;
	WT_DECL_RET;
	size_t len;
	int tret;
	char *key, *value;

	cursor = NULL;
	key = value = NULL;

	/* Build a key/value pair. */
	len = strlen(filename) + 100;
	if ((key = malloc(len)) == NULL)
		return (util_err(session, errno, NULL));
	if ((ret = __wt_snprintf(key, len, "file:%s", filename)) != 0)
		WT_ERR(util_err(session, ret, NULL));

	len = strlen(metadata) + strlen(filename) + 100;
	if ((value = malloc(len)) == NULL)
		return (util_err(session, errno, NULL));
	if ((ret = __wt_snprintf(
	    value, len, "%s,source=%s", metadata, path)) != 0)
		WT_ERR(util_err(session, ret, NULL));

	/* Open a metadata cursor and insert an entry for the file. */
	if ((ret = session->open_cursor(session,
	    "metadata:create", NULL, "readonly=false", &cursor)) != 0)
		WT_ERR(util_err(session, ret,
		    "WT_SESSION.open_cursor: metadata:create"));
	cursor->set_key(cursor, key);
	cursor->set_value(cursor, value);
	if ((ret = cursor->insert(cursor)) != 0)
		WT_ERR(util_err(session, ret, "WT_CURSOR.insert"));

err:	if (cursor != NULL && (tret = cursor->close(cursor)) != 0)
		ret = util_err(session, tret, "WT_CURSOR.close");
	free(key);
	free(value);
	return (ret);
}

/*
 * print_metadata --
 *	Print out the final metadata.
 */
static int
print_metadata(WT_SESSION *session, const char *uri)
{
	WT_CURSOR *cursor;
	WT_DECL_RET;
	int tret;
	char *value;

	cursor = NULL;

	/* Open a metadata cursor. */
	if ((ret = session->open_cursor(session,
	    "metadata:", NULL, NULL, &cursor)) != 0)
		WT_ERR(util_err(session, ret,
		    "WT_SESSION.open_cursor: metadata"));
	cursor->set_key(cursor, uri);
	if ((ret = cursor->search(cursor)) != 0)
		WT_ERR(util_err(session, ret, "WT_CURSOR.search"));
	if ((ret = cursor->get_value(cursor, &value)) != 0)
		WT_ERR(util_err(session, ret, "WT_CURSOR.get_value"));
	printf("%s: %s\n", uri, value);

err:	if (cursor != NULL && (tret = cursor->close(cursor)) != 0)
		ret = util_err(session, tret, "WT_CURSOR.close");
	return (ret);
}

static int
usage(void)
{
	(void)fprintf(stderr,
	    "usage: %s %s "
	    "analyze %s\n",
	    progname, usage_prefix, "path");
	return (1);
}

int
util_analyze(WT_SESSION *session, int argc, char *argv[])
{
	WT_DECL_RET;
	size_t len;
	int ch;
	char *filename, *metadata, *path, *uri;

	metadata = uri = NULL;

	while ((ch = __wt_getopt(progname, argc, argv, "")) != EOF)
		switch (ch) {
		case '?':
		default:
			return (usage());
		}
	argc -= __wt_optind;
	argv += __wt_optind;

	/*
	 * The remaining argument is the file name, and it must currently be
	 * an absolute path.
	 */
	if (argc != 1)
		return (usage());
	path = *argv;
	if (!__wt_absolute_path(path))
		WT_RET(util_err(
		    session, EINVAL, "%s: must be an absolute path", path));
	if ((filename = strrchr(path, '/')) == NULL)
		filename = path;
	else
		++filename;

	WT_RET(load_metadata(session, path, &metadata));
	WT_ERR(insert_metadata(session, path, filename, metadata));

	/* Build the uri and verify the object. */
	len = strlen(filename) + strlen("file:") + 10;
	if ((uri = malloc(len)) == NULL)
		WT_ERR(util_err(session, errno, NULL));
	if ((ret = __wt_snprintf(uri, len, "file:%s", filename)) != 0)
		WT_ERR(util_err(session, ret, NULL));
	if ((ret = session->verify(session, uri, "load_checkpoints")) != 0)
		WT_ERR(util_err(session, ret, "session.verify: %s", filename));

	WT_ERR(print_metadata(session, uri));

err:	free(metadata);
	free(uri);
	return (ret);
}
