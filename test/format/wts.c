/*-
 * Public Domain 2014-2018 MongoDB, Inc.
 * Public Domain 2008-2014 WiredTiger, Inc.
 *
 * This is free and unencumbered software released into the public domain.
 *
 * Anyone is free to copy, modify, publish, use, compile, sell, or
 * distribute this software, either in source code form or as a compiled
 * binary, for any purpose, commercial or non-commercial, and by any
 * means.
 *
 * In jurisdictions that recognize copyright laws, the author or authors
 * of this software dedicate any and all copyright interest in the
 * software to the public domain. We make this dedication for the benefit
 * of the public at large and to the detriment of our heirs and
 * successors. We intend this dedication to be an overt act of
 * relinquishment in perpetuity of all present and future rights to this
 * software under copyright law.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include "format.h"

/*
 * compressor --
 *	Configure compression.
 */
static const char *
compressor(uint32_t compress_flag)
{
	const char *p;

	p = "unrecognized compressor flag";
	switch (compress_flag) {
	case COMPRESS_NONE:
		p ="none";
		break;
	case COMPRESS_LZ4:
		p ="lz4";
		break;
	case COMPRESS_LZ4_NO_RAW:
		p ="lz4-noraw";
		break;
	case COMPRESS_LZO:
		p ="LZO1B-6";
		break;
	case COMPRESS_SNAPPY:
		p ="snappy";
		break;
	case COMPRESS_ZLIB:
		p ="zlib";
		break;
	case COMPRESS_ZLIB_NO_RAW:
		p ="zlib-noraw";
		break;
	case COMPRESS_ZSTD:
		p ="zstd";
		break;
	default:
		testutil_die(EINVAL,
		    "illegal compression flag: %#" PRIx32, compress_flag);
		/* NOTREACHED */
	}
	return (p);
}

/*
 * encryptor --
 *	Configure encryption.
 */
static const char *
encryptor(uint32_t encrypt_flag)
{
	const char *p;

	p = "unrecognized encryptor flag";
	switch (encrypt_flag) {
	case ENCRYPT_NONE:
		p = "none";
		break;
	case ENCRYPT_ROTN_7:
		p = "rotn,keyid=7";
		break;
	default:
		testutil_die(EINVAL,
		    "illegal encryption flag: %#" PRIx32, encrypt_flag);
		/* NOTREACHED */
	}
	return (p);
}

static int
handle_message(WT_EVENT_HANDLER *handler,
    WT_SESSION *session, const char *message)
{
	int out;

	(void)(handler);
	(void)(session);

	/* Write and flush the message so we're up-to-date on error. */
	if (g.logfp == NULL) {
		out = printf("%p:%s\n", (void *)session, message);
		(void)fflush(stdout);
	} else {
		out = fprintf(g.logfp, "%p:%s\n", (void *)session, message);
		(void)fflush(g.logfp);
	}
	return (out < 0 ? EIO : 0);
}

/*
 * __handle_progress_default --
 *	Default WT_EVENT_HANDLER->handle_progress implementation: ignore.
 */
static int
handle_progress(WT_EVENT_HANDLER *handler,
    WT_SESSION *session, const char *operation, uint64_t progress)
{
	(void)(handler);
	(void)(session);

	track(operation, progress, NULL);
	return (0);
}

static WT_EVENT_HANDLER event_handler = {
	NULL,
	handle_message,
	handle_progress,
	NULL	/* Close handler. */
};

#define	CONFIG_APPEND(p, ...) do {					\
	size_t __len;							\
	testutil_check(							\
	    __wt_snprintf_len_set(p, max, &__len, __VA_ARGS__));	\
	if (__len > max)						\
		__len = max;						\
	p += __len;							\
	max -= __len;							\
} while (0)

/*
 * wts_open --
 *	Open a connection to a WiredTiger database.
 */
void
wts_open(const char *home, bool set_api, WT_CONNECTION **connp)
{
	WT_CONNECTION *conn;
	WT_DECL_RET;
	size_t max;
	char *config, *p, helium_config[1024];

	*connp = NULL;

	config = p = g.wiredtiger_open_config;
	max = sizeof(g.wiredtiger_open_config);

	CONFIG_APPEND(p,
	    "create=true"
	    ",cache_size=%" PRIu32 "MB"
	    ",checkpoint_sync=false"
	    ",error_prefix=\"%s\"",
	    g.c_cache, progname);

	/* In-memory configuration. */
	if (g.c_in_memory != 0)
		CONFIG_APPEND(p, ",in_memory=1");

	/* LSM configuration. */
	if (DATASOURCE("lsm"))
		CONFIG_APPEND(p,
		    ",lsm_manager=(worker_thread_max=%" PRIu32 "),",
		    g.c_lsm_worker_threads);

	if (DATASOURCE("lsm") || g.c_cache < 20)
		CONFIG_APPEND(p, ",eviction_dirty_trigger=95");

	/* Checkpoints. */
	if (g.c_checkpoint_flag == CHECKPOINT_WIREDTIGER)
		CONFIG_APPEND(p,
		    ",checkpoint=(wait=%" PRIu32 ",log_size=%" PRIu32 ")",
		    g.c_checkpoint_wait, MEGABYTE(g.c_checkpoint_log_size));

	/* Eviction worker configuration. */
	if (g.c_evict_max != 0)
		CONFIG_APPEND(p,
		    ",eviction=(threads_max=%" PRIu32 ")", g.c_evict_max);

	/* Logging configuration. */
	if (g.c_logging)
		CONFIG_APPEND(p,
		    ",log=(enabled=true,archive=%d,"
		    "prealloc=%d,file_max=%" PRIu32 ",compressor=\"%s\")",
		    g.c_logging_archive ? 1 : 0,
		    g.c_logging_prealloc ? 1 : 0,
		    KILOBYTE(g.c_logging_file_max),
		    compressor(g.c_logging_compression_flag));

	/* Encryption. */
	if (g.c_encryption)
		CONFIG_APPEND(p,
		    ",encryption=(name=%s)", encryptor(g.c_encryption_flag));

	/* Miscellaneous. */
#ifdef HAVE_POSIX_MEMALIGN
	CONFIG_APPEND(p, ",buffer_alignment=512");
#endif

	CONFIG_APPEND(p, ",mmap=%d", g.c_mmap ? 1 : 0);

	if (g.c_direct_io)
		CONFIG_APPEND(p, ",direct_io=(data)");

	if (g.c_data_extend)
		CONFIG_APPEND(p, ",file_extend=(data=8MB)");

	/*
	 * Run the statistics server and/or maintain statistics in the engine.
	 * Sometimes specify a set of sources just to exercise that code.
	 */
	if (g.c_statistics_server) {
		if (mmrand(NULL, 0, 5) == 1 &&
		    memcmp(g.uri, "file:", strlen("file:")) == 0)
			CONFIG_APPEND(p,
			    ",statistics=(fast),statistics_log="
			    "(json,on_close,wait=5,sources=(\"file:\"))");
		else
			CONFIG_APPEND(p,
			    ",statistics=(fast),statistics_log="
			    "(json,on_close,wait=5)");
	} else
		CONFIG_APPEND(p,
		    ",statistics=(%s)", g.c_statistics ? "fast" : "none");

	/* Optionally stress operations. */
	CONFIG_APPEND(p, ",timing_stress_for_test=[");
	if (g.c_timing_stress_checkpoint)
		CONFIG_APPEND(p, ",checkpoint_slow");
	if (g.c_timing_stress_lookaside_sweep)
		CONFIG_APPEND(p, ",lookaside_sweep_race");
	if (g.c_timing_stress_split_1)
		CONFIG_APPEND(p, ",split_1");
	if (g.c_timing_stress_split_2)
		CONFIG_APPEND(p, ",split_2");
	if (g.c_timing_stress_split_3)
		CONFIG_APPEND(p, ",split_3");
	if (g.c_timing_stress_split_4)
		CONFIG_APPEND(p, ",split_4");
	if (g.c_timing_stress_split_5)
		CONFIG_APPEND(p, ",split_5");
	if (g.c_timing_stress_split_6)
		CONFIG_APPEND(p, ",split_6");
	if (g.c_timing_stress_split_7)
		CONFIG_APPEND(p, ",split_7");
	if (g.c_timing_stress_split_8)
		CONFIG_APPEND(p, ",split_8");
	CONFIG_APPEND(p, "]");

	/* Extensions. */
	CONFIG_APPEND(p,
	    ",extensions=["
	    "\"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\"],",
	    g.c_reverse ? REVERSE_PATH : "",
	    access(LZ4_PATH, R_OK) == 0 ? LZ4_PATH : "",
	    access(LZO_PATH, R_OK) == 0 ? LZO_PATH : "",
	    access(ROTN_PATH, R_OK) == 0 ? ROTN_PATH : "",
	    access(SNAPPY_PATH, R_OK) == 0 ? SNAPPY_PATH : "",
	    access(ZLIB_PATH, R_OK) == 0 ? ZLIB_PATH : "",
	    access(ZSTD_PATH, R_OK) == 0 ? ZSTD_PATH : "",
	    DATASOURCE("kvsbdb") ? KVS_BDB_PATH : "");

	/*
	 * Put configuration file configuration options second to last. Put
	 * command line configuration options at the end. Do this so they
	 * override the standard configuration.
	 */
	if (g.c_config_open != NULL)
		CONFIG_APPEND(p, ",%s", g.c_config_open);
	if (g.config_open != NULL)
		CONFIG_APPEND(p, ",%s", g.config_open);

	if (max == 0)
		testutil_die(ENOMEM,
		    "wiredtiger_open configuration buffer too small");

	/*
	 * Direct I/O may not work with backups, doing copies through the buffer
	 * cache after configuring direct I/O in Linux won't work.  If direct
	 * I/O is configured, turn off backups. This isn't a great place to do
	 * this check, but it's only here we have the configuration string.
	 */
	if (strstr(config, "direct_io") != NULL)
		g.c_backups = 0;

	testutil_checkfmt(
	    wiredtiger_open(home, &event_handler, config, &conn), "%s", home);

	if (set_api)
		g.wt_api = conn->get_extension_api(conn);

	/*
	 * Load the Helium shared library: it would be possible to do this as
	 * part of the extensions configured for wiredtiger_open, there's no
	 * difference, I am doing it here because it's easier to work with the
	 * configuration strings.
	 */
	if (DATASOURCE("helium")) {
		if (g.helium_mount == NULL)
			testutil_die(EINVAL, "no Helium mount point specified");
		testutil_check(
		    __wt_snprintf(helium_config, sizeof(helium_config),
		    "entry=wiredtiger_extension_init,config=["
		    "helium_verbose=0,"
		    "dev1=[helium_devices=\"he://./%s\","
		    "helium_o_volume_truncate=1]]",
		    g.helium_mount));
		if ((ret = conn->load_extension(
		    conn, HELIUM_PATH, helium_config)) != 0)
			testutil_die(ret,
			    "WT_CONNECTION.load_extension: %s:%s",
			    HELIUM_PATH, helium_config);
	}
	*connp = conn;
}

/*
 * wts_reopen --
 *	Re-open a connection to a WiredTiger database.
 */
void
wts_reopen(void)
{
	WT_CONNECTION *conn;

	testutil_checkfmt(wiredtiger_open(g.home, &event_handler,
	    g.wiredtiger_open_config, &conn), "%s", g.home);

	g.wt_api = conn->get_extension_api(conn);
	g.wts_conn = conn;
}

/*
 * wts_create --
 *	Create the underlying store.
 */
void
wts_init(void)
{
	WT_CONNECTION *conn;
	WT_SESSION *session;
	size_t max;
	uint32_t maxintlkey, maxleafkey, maxleafvalue;
	char config[4096], *p;

	conn = g.wts_conn;
	p = config;
	max = sizeof(config);

	CONFIG_APPEND(p,
	    "key_format=%s"
	    ",allocation_size=512"
	    ",%s"
	    ",internal_page_max=%" PRIu32
	    ",leaf_page_max=%" PRIu32
	    ",memory_page_max=%" PRIu32,
	    (g.type == ROW) ? "u" : "r",
	    g.c_firstfit ? "block_allocation=first" : "",
	    g.intl_page_max, g.leaf_page_max, MEGABYTE(g.c_memory_page_max));

	/*
	 * Configure the maximum key/value sizes, but leave it as the default
	 * if we come up with something crazy.
	 */
	maxintlkey = mmrand(NULL, g.intl_page_max / 50, g.intl_page_max / 40);
	if (maxintlkey > 20)
		CONFIG_APPEND(p, ",internal_key_max=%" PRIu32, maxintlkey);
	maxleafkey = mmrand(NULL, g.leaf_page_max / 50, g.leaf_page_max / 40);
	if (maxleafkey > 20)
		CONFIG_APPEND(p, ",leaf_key_max=%" PRIu32, maxleafkey);
	maxleafvalue = mmrand(NULL, g.leaf_page_max * 10, g.leaf_page_max / 40);
	if (maxleafvalue > 40 && maxleafvalue < 100 * 1024)
		CONFIG_APPEND(p, ",leaf_value_max=%" PRIu32, maxleafvalue);

	switch (g.type) {
	case FIX:
		CONFIG_APPEND(p, ",value_format=%" PRIu32 "t", g.c_bitcnt);
		break;
	case ROW:
		if (g.c_huffman_key)
			CONFIG_APPEND(p, ",huffman_key=english");
		if (g.c_prefix_compression)
			CONFIG_APPEND(p,
			    ",prefix_compression_min=%" PRIu32,
			    g.c_prefix_compression_min);
		else
			CONFIG_APPEND(p, ",prefix_compression=false");
		if (g.c_reverse)
			CONFIG_APPEND(p, ",collator=reverse");
		/* FALLTHROUGH */
	case VAR:
		if (g.c_huffman_value)
			CONFIG_APPEND(p, ",huffman_value=english");
		if (g.c_dictionary)
			CONFIG_APPEND(p,
			    ",dictionary=%" PRIu32, mmrand(NULL, 123, 517));
		break;
	}

	/* Configure checksums. */
	switch (g.c_checksum_flag) {
	case CHECKSUM_OFF:
		CONFIG_APPEND(p, ",checksum=\"off\"");
		break;
	case CHECKSUM_ON:
		CONFIG_APPEND(p, ",checksum=\"on\"");
		break;
	case CHECKSUM_UNCOMPRESSED:
		CONFIG_APPEND(p, ",checksum=\"uncompressed\"");
		break;
	}

	/* Configure compression. */
	if (g.c_compression_flag != COMPRESS_NONE)
		CONFIG_APPEND(p, ",block_compressor=\"%s\"",
		    compressor(g.c_compression_flag));

	/* Configure Btree internal key truncation. */
	CONFIG_APPEND(p, ",internal_key_truncate=%s",
	    g.c_internal_key_truncation ? "true" : "false");

	/* Configure Btree page key gap. */
	CONFIG_APPEND(p, ",key_gap=%" PRIu32, g.c_key_gap);

	/* Configure Btree split page percentage. */
	CONFIG_APPEND(p, ",split_pct=%" PRIu32, g.c_split_pct);

	/* Configure LSM and data-sources. */
	if (DATASOURCE("helium"))
		CONFIG_APPEND(p,
		    ",type=helium,helium_o_compress=%d,helium_o_truncate=1",
		    g.c_compression_flag == COMPRESS_NONE ? 0 : 1);

	if (DATASOURCE("kvsbdb"))
		CONFIG_APPEND(p, ",type=kvsbdb");

	if (DATASOURCE("lsm")) {
		CONFIG_APPEND(p, ",type=lsm,lsm=(");
		CONFIG_APPEND(p,
		    "auto_throttle=%s,", g.c_auto_throttle ? "true" : "false");
		CONFIG_APPEND(p, "chunk_size=%" PRIu32 "MB,", g.c_chunk_size);
		/*
		 * We can't set bloom_oldest without bloom, and we want to test
		 * with Bloom filters on most of the time anyway.
		 */
		if (g.c_bloom_oldest)
			g.c_bloom = 1;
		CONFIG_APPEND(p, "bloom=%s,", g.c_bloom ? "true" : "false");
		CONFIG_APPEND(p,
		    "bloom_bit_count=%" PRIu32 ",", g.c_bloom_bit_count);
		CONFIG_APPEND(p,
		    "bloom_hash_count=%" PRIu32 ",", g.c_bloom_hash_count);
		CONFIG_APPEND(p,
		    "bloom_oldest=%s,", g.c_bloom_oldest ? "true" : "false");
		CONFIG_APPEND(p, "merge_max=%" PRIu32 ",", g.c_merge_max);
		CONFIG_APPEND(p, ",)");
	}

	if (max == 0)
		testutil_die(ENOMEM,
		    "WT_SESSION.create configuration buffer too small");

	/*
	 * Create the underlying store.
	 */
	testutil_check(conn->open_session(conn, NULL, NULL, &session));
	testutil_checkfmt(session->create(session, g.uri, config), "%s", g.uri);
	testutil_check(session->close(session, NULL));
}

void
wts_close(void)
{
	WT_CONNECTION *conn;
	const char *config;

	conn = g.wts_conn;

	config = g.c_leak_memory ? "leak_memory" : NULL;

	testutil_check(conn->close(conn, config));
	g.wts_conn = NULL;
	g.wt_api = NULL;
}

void
wts_dump(const char *tag, int dump_bdb)
{
#ifdef HAVE_BERKELEY_DB
	size_t len;
	char *cmd;

	/*
	 * In-memory configurations and data-sources don't support dump through
	 * the wt utility.
	 */
	if (g.c_in_memory != 0)
		return;
	if (DATASOURCE("helium") || DATASOURCE("kvsbdb"))
		return;

	track("dump files and compare", 0ULL, NULL);

	len = strlen(g.home) + strlen(BERKELEY_DB_PATH) + strlen(g.uri) + 100;
	cmd = dmalloc(len);
	testutil_check(__wt_snprintf(cmd, len,
	    "sh s_dumpcmp -h %s %s %s %s %s %s",
	    g.home,
	    dump_bdb ? "-b " : "",
	    dump_bdb ? BERKELEY_DB_PATH : "",
	    g.type == FIX || g.type == VAR ? "-c" : "",
	    g.uri == NULL ? "" : "-n",
	    g.uri == NULL ? "" : g.uri));

	testutil_checkfmt(system(cmd), "%s: dump comparison failed", tag);
	free(cmd);
#else
	(void)tag;				/* [-Wunused-variable] */
	(void)dump_bdb;				/* [-Wunused-variable] */
#endif
}

void
wts_verify(const char *tag)
{
	WT_CONNECTION *conn;
	WT_DECL_RET;
	WT_SESSION *session;

	if (g.c_verify == 0)
		return;

	conn = g.wts_conn;
	track("verify", 0ULL, NULL);

	testutil_check(conn->open_session(conn, NULL, NULL, &session));
	if (g.logging != 0)
		(void)g.wt_api->msg_printf(g.wt_api, session,
		    "=============== verify start ===============");

	/*
	 * Verify can return EBUSY if the handle isn't available. Don't yield
	 * and retry, in the case of LSM, the handle may not be available for
	 * a long time.
	 */
	ret = session->verify(session, g.uri, "strict");
	testutil_assertfmt(
	    ret == 0 || ret == EBUSY, "session.verify: %s: %s", g.uri, tag);

	if (g.logging != 0)
		(void)g.wt_api->msg_printf(g.wt_api, session,
		    "=============== verify stop ===============");
	testutil_check(session->close(session, NULL));
}

/*
 * wts_stats --
 *	Dump the run's statistics.
 */
void
wts_stats(void)
{
	FILE *fp;
	WT_CONNECTION *conn;
	WT_CURSOR *cursor;
	WT_DECL_RET;
	WT_SESSION *session;
	size_t len;
	uint64_t v;
	const char *desc, *pval;
	char *stat_name;

	/* Ignore statistics if they're not configured. */
	if (g.c_statistics == 0)
		return;

	/* Some data-sources don't support statistics. */
	if (DATASOURCE("helium") || DATASOURCE("kvsbdb"))
		return;

	conn = g.wts_conn;
	track("stat", 0ULL, NULL);

	testutil_check(conn->open_session(conn, NULL, NULL, &session));

	if ((fp = fopen(g.home_stats, "w")) == NULL)
		testutil_die(errno, "fopen: %s", g.home_stats);

	/* Connection statistics. */
	fprintf(fp, "====== Connection statistics:\n");
	testutil_check(session->open_cursor(
	    session, "statistics:", NULL, NULL, &cursor));

	while ((ret = cursor->next(cursor)) == 0 &&
	    (ret = cursor->get_value(cursor, &desc, &pval, &v)) == 0)
		if (fprintf(fp, "%s=%s\n", desc, pval) < 0)
			testutil_die(errno, "fprintf");

	if (ret != WT_NOTFOUND)
		testutil_die(ret, "cursor.next");
	testutil_check(cursor->close(cursor));

	/* Data source statistics. */
	fprintf(fp, "\n\n====== Data source statistics:\n");
	len = strlen("statistics:") + strlen(g.uri) + 1;
	stat_name = dmalloc(len);
	testutil_check(__wt_snprintf(stat_name, len, "statistics:%s", g.uri));
	testutil_check(session->open_cursor(
	    session, stat_name, NULL, NULL, &cursor));
	free(stat_name);

	while ((ret = cursor->next(cursor)) == 0 &&
	    (ret = cursor->get_value(cursor, &desc, &pval, &v)) == 0)
		if (fprintf(fp, "%s=%s\n", desc, pval) < 0)
			testutil_die(errno, "fprintf");

	if (ret != WT_NOTFOUND)
		testutil_die(ret, "cursor.next");
	testutil_check(cursor->close(cursor));

	fclose_and_clear(&fp);

	testutil_check(session->close(session, NULL));
}
