/*-
 * Copyright (c) 2014-2019 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "util.h"

/*
 * read_metadata --
 *	Retrieve the file's metadata information.
 */
static int
read_metadata(WT_SESSION *wt_session, const char *path, char **metadatap)
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
    WT_SESSION *wt_session, const char *path, const char *uri, char *metadata)
{
	WT_DECL_RET;
	WT_SESSION_IMPL *session;
	size_t len;
	const char *filecfg[] = {
	    WT_CONFIG_BASE(
	    (WT_SESSION_IMPL *)wt_session, file_meta), NULL, NULL, NULL };
	char *fileconf, *source;

	session = (WT_SESSION_IMPL *)wt_session;
	fileconf = source = NULL;

	/* Build the source entry. */
	len = strlen("source=") + strlen(path) + 10;
	if ((source = malloc(len)) == NULL)
		WT_ERR(util_err(wt_session, errno, NULL));
	if ((ret = __wt_snprintf(source, len, "source=%s", path)) != 0)
		WT_ERR(util_err(wt_session, ret, NULL));

	/*
	 * Add metadata read from the file to the default configuration, where
	 * read metadata overrides the defaults, flatten it and insert it.
	 */
	filecfg[1] = metadata;
	filecfg[2] = source;
	if ((ret = __wt_config_collapse(session, filecfg, &fileconf)) != 0)
		WT_ERR(util_err(wt_session, ret, NULL));
	if ((ret = __wt_metadata_insert(session, uri, fileconf)) != 0)
		WT_ERR(util_err(wt_session, ret, NULL));

err:	free(fileconf);
	free(source);
	return (ret);
}

/*
 * report_metadata --
 *	Report the final database metadata.
 */
static int
report_metadata(WT_SESSION *wt_session, const char *uri)
{
	WT_SESSION_IMPL *session;
	char *value;

	session = (WT_SESSION_IMPL *)wt_session;

	WT_RET(__wt_metadata_search(session, uri, &value));
	printf("%s\n%s\n", uri, value);
	free(value);
	return (0);
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
	char *metadata, *name, *path, *uri;

	metadata = uri = NULL;

	while ((ch = __wt_getopt(progname, argc, argv, "")) != EOF)
		switch (ch) {
		case '?':
		default:
			return (usage());
		}
	argc -= __wt_optind;
	argv += __wt_optind;

	/* The argument is the file name, and must be an absolute path. */
	if (argc != 1)
		return (usage());
	path = *argv;
	if (!__wt_absolute_path(path))
		WT_RET(util_err(
		    session, EINVAL, "%s: must be an absolute path", path));

	/* Build the URI. */
	if ((name = strrchr(path, '/')) == NULL)
		name = path;
	else
		++name;
	len = strlen("file:") + strlen(name) + 10;
	if ((uri = malloc(len)) == NULL)
		WT_ERR(util_err(session, errno, NULL));
	if ((ret = __wt_snprintf(uri, len, "file:%s", name)) != 0)
		WT_ERR(util_err(session, ret, NULL));

	/* Read the metadata from the descriptor block. */
	WT_ERR(read_metadata(session, path, &metadata));

	/* Update the metadata. */
	WT_ERR(insert_metadata(session, path, uri, metadata));

	/* Verify the object. */
	if ((ret = session->verify(session, uri, "load_checkpoints")) != 0)
		WT_ERR(util_err(session, ret, "session.verify: %s", name));

	/* Report the final metadata. */
	WT_ERR(report_metadata(session, uri));

err:	free(metadata);
	free(uri);
	return (ret);
}
