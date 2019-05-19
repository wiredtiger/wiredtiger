/*-
 * Copyright (c) 2014-2019 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "util.h"

static int
usage(void)
{
	(void)fprintf(stderr,
	    "usage: %s %s "
	    "verify %s\n",
	    progname, usage_prefix,
	    "[-F] [-d dump_address | dump_blocks | dump_layout | "
	    "dump_offsets=#,# | dump_pages] uri");
	return (1);
}

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

	if ((ret = __wt_desc_read(
	    session, fh, 0, path, metadatap)) != 0 || *metadatap == NULL)
		WT_ERR(util_err(wt_session, ret,
		    "%s: no metadata information available", path));

err:	WT_TRET(__wt_close(session, &fh));

	return (ret);
}

/*
 * insert_metadata --
 *	Set the database metadata.
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
	 * read metadata overrides the defaults, flatten it and insert it. Use
	 * the update call, there's some non-zero chance it's already there.
	 */
	filecfg[1] = metadata;
	filecfg[2] = source;
	if ((ret = __wt_config_collapse(session, filecfg, &fileconf)) != 0)
		WT_ERR(util_err(wt_session, ret, NULL));
	if ((ret = __wt_metadata_update(session, uri, fileconf)) != 0)
		WT_ERR(util_err(wt_session, ret,
		    "%s: metadata update failed", uri));

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
	WT_DECL_RET;
	WT_SESSION_IMPL *session;
	char *value;

	session = (WT_SESSION_IMPL *)wt_session;

	if ((ret = __wt_metadata_search(session, uri, &value)) == 0)
		printf("%s\n%s\n", uri, value);
	else
		ret = util_err(wt_session, ret,
		    "%s: no object metadata found", uri);
	free(value);
	return (ret);
}

/*
 * remove_metadata --
 *	Remove the database metadata.
 */
static int
remove_metadata(WT_SESSION *wt_session, const char *uri)
{
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	session = (WT_SESSION_IMPL *)wt_session;

	if ((ret = __wt_metadata_remove(session, uri)) != 0)
		WT_RET(util_err(wt_session, ret,
		    "%s: metadata removal failed", uri));
	return (0);
}

int
util_verify(WT_SESSION *wt_session, int argc, char *argv[])
{
	WT_DECL_ITEM(config);
	WT_DECL_RET;
	WT_SESSION_IMPL *session;
	size_t len;
	int ch;
	char *dump_offsets, *metadata, *source, *uri;
	bool dump_address, dump_blocks, dump_layout, dump_pages, standalone;

	session = (WT_SESSION_IMPL *)wt_session;

	dump_address = dump_blocks = dump_layout = dump_pages = false;
	dump_offsets = metadata = source = uri = NULL;
	standalone = false;
	while ((ch = __wt_getopt(progname, argc, argv, "d:F")) != EOF)
		switch (ch) {
		case 'd':
			if (strcmp(__wt_optarg, "dump_address") == 0)
				dump_address = true;
			else if (strcmp(__wt_optarg, "dump_blocks") == 0)
				dump_blocks = true;
			else if (strcmp(__wt_optarg, "dump_layout") == 0)
				dump_layout = true;
			else if (
			    WT_PREFIX_MATCH(__wt_optarg, "dump_offsets=")) {
				if (dump_offsets != NULL) {
					fprintf(stderr,
					    "%s: only a single 'dump_offsets' "
					    "argument supported\n", progname);
					return (usage());
				}
				dump_offsets =
				    __wt_optarg + strlen("dump_offsets=");
			} else if (strcmp(__wt_optarg, "dump_pages") == 0)
				dump_pages = true;
			else
				return (usage());
			break;
		case 'F':
			standalone = true;
			break;
		case '?':
		default:
			return (usage());
		}
	argc -= __wt_optind;
	argv += __wt_optind;

	if (argc != 1)
		return (usage());

	/* Build the configuration string. */
	if ((ret = __wt_scr_alloc(session, 0, &config)) != 0)
		WT_ERR(util_err(wt_session, ret, NULL));
	if (dump_address &&
	    (ret = __wt_buf_catfmt(session, config, "dump_address,") != 0))
		WT_ERR(util_err(wt_session, ret, NULL));
	if (dump_blocks &&
	    (ret = __wt_buf_catfmt(session, config, "dump_blocks,") != 0))
		WT_ERR(util_err(wt_session, ret, NULL));
	if (dump_layout &&
	    (ret = __wt_buf_catfmt(session, config, "dump_layout,") != 0))
		WT_ERR(util_err(wt_session, ret, NULL));
	if (dump_offsets != NULL && (ret = __wt_buf_catfmt(
	    session, config, "dump_offsets=[%s],", dump_offsets) != 0))
		WT_ERR(util_err(wt_session, ret, NULL));
	if (dump_pages &&
	    (ret = __wt_buf_catfmt(session, config, "dump_pages,") != 0))
		WT_ERR(util_err(wt_session, ret, NULL));
	if (standalone &&
	    (ret = __wt_buf_catfmt(session, config, "load_checkpoints,") != 0))
		WT_ERR(util_err(wt_session, ret, NULL));

	if (standalone) {
		/* The remaining argument is a standalone source file. */
		source = *argv;
		if (!__wt_absolute_path(source))
			WT_ERR(util_err(wt_session, EINVAL,
			    "%s: must be an absolute path", source));

		/* Use a temporary URI. */
#define	WT_VRFY_TMP	"wt.verify.temporary"
		len = strlen("file:") + strlen(WT_VRFY_TMP) + 10;
		if ((uri = malloc(len)) == NULL)
			WT_ERR(util_err(wt_session, errno, NULL));
		if ((ret = __wt_snprintf(uri, len, "file:" WT_VRFY_TMP)) != 0)
			WT_ERR(util_err(wt_session, ret, NULL));

		/* Read the metadata from the descriptor block. */
		WT_ERR(read_metadata(wt_session, source, &metadata));

		/* Set the database metadata. */
		WT_ERR(insert_metadata(wt_session, source, uri, metadata));
	} else {
		/* The remaining argument is the table name. */
		if ((uri = util_uri(wt_session, *argv, "table")) == NULL)
			WT_ERR(1);
	}

	/* Verify the object. */
	ret = wt_session->verify(wt_session, uri, (char *)config->data);

	/* Verbose configures a progress counter, move to the next line. */
	if (verbose)
		printf("\n");

	if (ret != 0)
		WT_ERR(util_err(wt_session, ret, "WT_SESSION.verify: %s", uri));

	if (standalone) {
		/* Report the final metadata. */
		WT_ERR(report_metadata(wt_session, uri));

		/* Remove the database metadata. */
		WT_ERR(remove_metadata(wt_session, uri));
	}

err:	__wt_scr_free(session, &config);
	free(uri);
	return (ret);
}
