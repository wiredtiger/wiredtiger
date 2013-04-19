/*-
 * Public Domain 2008-2013 WiredTiger, Inc.
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

#include <bzlib.h>
#include <inttypes.h>
#include <stdlib.h>
#include <string.h>

#include <wiredtiger.h>
#include <wiredtiger_ext.h>

static WT_EXTENSION_API *wt_api;

static int
bzip2_compress(WT_COMPRESSOR *, WT_SESSION *,
    uint8_t *, size_t, uint8_t *, size_t, size_t *, int *);
static int
bzip2_decompress(WT_COMPRESSOR *, WT_SESSION *,
    uint8_t *, size_t, uint8_t *, size_t, size_t *);
#ifdef WIREDTIGER_TEST_COMPRESS_RAW
static int
bzip2_compress_raw(WT_COMPRESSOR *, WT_SESSION *, size_t, u_int,
    size_t, uint8_t *, uint32_t *, uint32_t, uint8_t *, size_t, int,
    size_t *, uint32_t *);
#endif

static WT_COMPRESSOR bzip2_compressor = {
    bzip2_compress, NULL, bzip2_decompress, NULL };

/* between 0-4: set the amount of verbosity to stderr */
static int bz_verbosity = 0;

/* between 1-9: set the block size to 100k x this number (compression only) */
static int bz_blocksize100k = 1;

/*
 * between 0-250: workFactor: see bzip2 manual.  0 is a reasonable default
 * (compression only)
 */
static int bz_workfactor = 0;

/* if nonzero, decompress using less memory, but slower (decompression only) */
static int bz_small = 0;

int
wiredtiger_extension_init(WT_CONNECTION *connection, WT_CONFIG_ARG *config)
{
	(void)config;				/* Unused parameters */

						/* Find the extension API */
	wt_api = connection->get_extension_api(connection);

						/* Load the compressor */
#ifdef WIREDTIGER_TEST_COMPRESS_RAW
	bzip2_compressor.compress_raw = bzip2_compress_raw;
	return (connection->add_compressor(
	    connection, "raw", &bzip2_compressor, NULL));
#else
	return (connection->add_compressor(
	    connection, "bzip2", &bzip2_compressor, NULL));
#endif
}

/* Bzip2 WT_COMPRESSOR implementation for WT_CONNECTION::add_compressor. */
/*
 * bzip2_error --
 *	Output an error message, and return a standard error code.
 */
static int
bzip2_error(WT_SESSION *session, const char *call, int bzret)
{
	const char *msg;

	switch (bzret) {
	case BZ_MEM_ERROR:
		msg = "BZ_MEM_ERROR";
		break;
	case BZ_OUTBUFF_FULL:
		msg = "BZ_OUTBUFF_FULL";
		break;
	case BZ_SEQUENCE_ERROR:
		msg = "BZ_SEQUENCE_ERROR";
		break;
	case BZ_PARAM_ERROR:
		msg = "BZ_PARAM_ERROR";
		break;
	case BZ_DATA_ERROR:
		msg = "BZ_DATA_ERROR";
		break;
	case BZ_DATA_ERROR_MAGIC:
		msg = "BZ_DATA_ERROR_MAGIC";
		break;
	case BZ_IO_ERROR:
		msg = "BZ_IO_ERROR";
		break;
	case BZ_UNEXPECTED_EOF:
		msg = "BZ_UNEXPECTED_EOF";
		break;
	case BZ_CONFIG_ERROR:
		msg = "BZ_CONFIG_ERROR";
		break;
	default:
		msg = "unknown error";
		break;
	}

	(void)wt_api->err_printf(wt_api,
	    session, "bzip2 error: %s: %s: %d", call, msg, bzret);
	return (WT_ERROR);
}

static void *
bzalloc(void *cookie, int number, int size)
{
	return (wt_api->scr_alloc(wt_api, cookie, (size_t)(number * size)));
}

static void
bzfree(void *cookie, void *p)
{
	wt_api->scr_free(wt_api, cookie, p);
}

static int
bzip2_compress(WT_COMPRESSOR *compressor, WT_SESSION *session,
    uint8_t *src, size_t src_len,
    uint8_t *dst, size_t dst_len,
    size_t *result_lenp, int *compression_failed)
{
	bz_stream bz;
	int ret;

	(void)compressor;				/* Unused */

	memset(&bz, 0, sizeof(bz));
	bz.bzalloc = bzalloc;
	bz.bzfree = bzfree;
	bz.opaque = session;

	if ((ret = BZ2_bzCompressInit(&bz,
	    bz_blocksize100k, bz_verbosity, bz_workfactor)) != BZ_OK)
		return (bzip2_error(session, "BZ2_bzCompressInit", ret));

	bz.next_in = (char *)src;
	bz.avail_in = (uint32_t)src_len;
	bz.next_out = (char *)dst;
	bz.avail_out = (uint32_t)dst_len;
	if ((ret = BZ2_bzCompress(&bz, BZ_FINISH)) == BZ_STREAM_END) {
		*compression_failed = 0;
		*result_lenp = dst_len - bz.avail_out;
	} else
		*compression_failed = 1;

	if ((ret = BZ2_bzCompressEnd(&bz)) != BZ_OK)
		return (bzip2_error(session, "BZ2_bzCompressEnd", ret));

	return (0);
}

#ifdef WIREDTIGER_TEST_COMPRESS_RAW
/*
 * __bzip2_compress_raw_random --
 *	Return a 32-bit pseudo-random number.
 *
 * This is an implementation of George Marsaglia's multiply-with-carry pseudo-
 * random number generator.  Computationally fast, with reasonable randomness
 * properties.
 */
static uint32_t
__bzip2_compress_raw_random(void)
{
	static uint32_t m_w = 521288629;
	static uint32_t m_z = 362436069;

	m_z = 36969 * (m_z & 65535) + (m_z >> 16);
	m_w = 18000 * (m_w & 65535) + (m_w >> 16);
	return (m_z << 16) + (m_w & 65535);
}

/*
 * bzip2_compress_raw --
 *	Test function for the test/format utility.
 */
static int
bzip2_compress_raw(WT_COMPRESSOR *compressor, WT_SESSION *session,
    size_t page_max, u_int split_pct, size_t extra,
    uint8_t *src, uint32_t *offsets, uint32_t slots,
    uint8_t *dst, size_t dst_len, int final,
    size_t *result_lenp, uint32_t *result_slotsp)
{
	uint32_t take, twenty_pct;
	int compression_failed, ret;

	(void)page_max;					/* Unused */
	(void)split_pct;
	(void)extra;
	(void)final;

	/*
	 * This function is used by the test/format utility to test the
	 * WT_COMPRESSOR::compress_raw functionality.
	 *
	 * I'm trying to mimic how a real application is likely to behave: if
	 * it's a small number of slots, we're not going to take them because
	 * they aren't worth compressing.  In all likelihood, that's going to
	 * be because the btree is wrapping up a page, but that's OK, that is
	 * going to happen a lot.   In addition, add a 2% chance of not taking
	 * anything at all just because we don't want to take it.  Otherwise,
	 * select between 80 and 100% of the slots and compress them, stepping
	 * down by 5 slots at a time until something works.
	 */
	take = slots;
	if (take < 10 || __bzip2_compress_raw_random() % 100 < 2)
		take = 0;
	else {
		twenty_pct = (slots / 10) * 2;
		if (twenty_pct < slots)
			take -= __bzip2_compress_raw_random() % twenty_pct;

		for (;;) {
			if ((ret = bzip2_compress(compressor, session,
			    src, offsets[take],
			    dst, dst_len,
			    result_lenp, &compression_failed)) != 0)
				return (ret);
			if (!compression_failed)
				break;
			if (take < 10) {
				take = 0;
				break;
			}
			take -= 5;
		}
	}

	*result_slotsp = take;
	if (take == 0)
		*result_lenp = 0;

#if 0
	fprintf(stderr,
	    "bzip2_compress_raw (%s): page_max %" PRIuMAX
	    ", split_pct %u, extra %" PRIuMAX
	    ", slots %" PRIu32 ", take %" PRIu32 ": %" PRIu32 " -> %"
	    PRIuMAX "\n",
	    final ? "final" : "not final",
	    (uintmax_t)page_max, split_pct, (uintmax_t)extra,
	    slots, take, offsets[take], (uintmax_t)*result_lenp);
#endif
	return (0);
}
#endif

static int
bzip2_decompress(WT_COMPRESSOR *compressor, WT_SESSION *session,
    uint8_t *src, size_t src_len,
    uint8_t *dst, size_t dst_len,
    size_t *result_lenp)
{
	bz_stream bz;
	int ret, tret;

	(void)compressor;				/* Unused */

	memset(&bz, 0, sizeof(bz));
	bz.bzalloc = bzalloc;
	bz.bzfree = bzfree;
	bz.opaque = session;

	if ((ret = BZ2_bzDecompressInit(&bz, bz_small, bz_verbosity)) != BZ_OK)
		return (bzip2_error(session, "BZ2_bzDecompressInit", ret));

	bz.next_in = (char *)src;
	bz.avail_in = (uint32_t)src_len;
	bz.next_out = (char *)dst;
	bz.avail_out = (uint32_t)dst_len;
	if ((ret = BZ2_bzDecompress(&bz)) == BZ_STREAM_END) {
		*result_lenp = dst_len - bz.avail_out;
		ret = 0;
	} else
		(void)bzip2_error(session, "BZ2_bzDecompress", ret);

	if ((tret = BZ2_bzDecompressEnd(&bz)) != BZ_OK)
		return (bzip2_error(session, "BZ2_bzDecompressEnd", tret));

	return (ret == 0 ?
	    0 : bzip2_error(session, "BZ2_bzDecompressEnd", ret));
}
/* End Bzip2 WT_COMPRESSOR implementation for WT_CONNECTION::add_compressor. */
