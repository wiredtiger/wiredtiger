/*-
 * Public Domain 2014-2016 MongoDB, Inc.
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

#include <zstd.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include <wiredtiger.h>
#include <wiredtiger_ext.h>

/*
 * We need to include the configuration file to detect whether this extension
 * is being built into the WiredTiger library.
 */
#include "wiredtiger_config.h"
#ifdef _MSC_VER
#define	inline __inline
#endif

/* Local compressor structure. */
typedef struct {
	WT_COMPRESSOR compressor;		/* Must come first */

	WT_EXTENSION_API *wt_api;		/* Extension API */

	int	 compression_level;		/* compression level */
	uint16_t finish_reserve;		/* finish stream compression */
} ZSTD_COMPRESSOR;

/*
 * Zstd decompression requires the exact compressed byte count returned by the
 * compression functions. WiredTiger doesn't track that value, store it in the
 * destination buffer.
 *
 * Additionally, raw compression may compress into the middle of a record, and
 * after decompression we return the length to the last record successfully
 * decompressed, not the number of bytes decompressed; store that value in the
 * destination buffer as well.
 *
 * Use fixed-size, 4B values (WiredTiger never writes buffers larger than 4GB).
 *
 * The unused field is available for a mode flag if one is needed in the future,
 * we guarantee it's 0.
 */
typedef struct {
	uint32_t compressed_len;	/* True compressed length */
	uint32_t uncompressed_len;	/* True uncompressed source length */
	uint32_t useful_len;		/* Decompression return value */
	uint32_t unused;		/* Guaranteed to be 0 */
} ZSTD_PREFIX;

#ifdef WORDS_BIGENDIAN
/*
 * zstd_bswap32 --
 *	32-bit unsigned little-endian to/from big-endian value.
 */
static inline uint32_t
zstd_bswap32(uint32_t v)
{
	return (
	    ((v << 24) & 0xff000000) |
	    ((v <<  8) & 0x00ff0000) |
	    ((v >>  8) & 0x0000ff00) |
	    ((v >> 24) & 0x000000ff)
	);
}

/*
 * zstd_prefix_swap --
 *	The additional information is written in little-endian format, handle
 * the conversion.
 */
static inline void
zstd_prefix_swap(ZSTD_PREFIX *prefix)
{
	prefix->compressed_len = zstd_bswap32(prefix->compressed_len);
	prefix->uncompressed_len = zstd_bswap32(prefix->uncompressed_len);
	prefix->useful_len = zstd_bswap32(prefix->useful_len);
	prefix->unused = zstd_bswap32(prefix->unused);
}
#endif

/*
 * zstd_error --
 *	Output an error message, and return a standard error code.
 */
static int
zstd_error(WT_COMPRESSOR *compressor,
    WT_SESSION *session, const char *call, size_t *errorp)
{
	WT_EXTENSION_API *wt_api;

	wt_api = ((ZSTD_COMPRESSOR *)compressor)->wt_api;

	if (errorp == NULL)
		(void)wt_api->err_printf(wt_api, session,
		    "zstd error: %s", call);
	else
		(void)wt_api->err_printf(wt_api, session,
		    "zstd error: %s: %s", call, ZSTD_getErrorName(*errorp));
	return (WT_ERROR);
}

/*
 *  zstd_compress --
 *	WiredTiger Zstd compression.
 */
static int
zstd_compress(WT_COMPRESSOR *compressor, WT_SESSION *session,
    uint8_t *src, size_t src_len,
    uint8_t *dst, size_t dst_len,
    size_t *result_lenp, int *compression_failed)
{
	WT_EXTENSION_API *wt_api;
	ZSTD_COMPRESSOR *zcompressor;
	ZSTD_PREFIX prefix;
	size_t zstd_ret;

	(void)compressor;				/* Unused parameters */

	zcompressor = (ZSTD_COMPRESSOR *)compressor;
	wt_api = zcompressor->wt_api;

	/*
	 * Compress, starting after the prefix bytes.
	 *
	 * Zstd compression runs faster if the destination buffer is sized at
	 * the upper-bound of the buffer size needed by the compression. We
	 * aren't interested in blocks that grow, but we'd rather have faster
	 * compression than early termination by zstd, blocks that grow should
	 * be rare.
	 */
	zstd_ret = ZSTD_compress(
	    dst + sizeof(ZSTD_PREFIX), dst_len - sizeof(ZSTD_PREFIX),
	    src, src_len, zcompressor->compression_level);

	/*
	 * If compression succeeded and the compressed length is smaller than
	 * the original size, return success.
	 */
	if (!ZSTD_isError(zstd_ret) &&
	    zstd_ret + sizeof(ZSTD_PREFIX) < src_len) {
		prefix.compressed_len = (uint32_t)zstd_ret;
		prefix.uncompressed_len = (uint32_t)src_len;
		prefix.useful_len = (uint32_t)src_len;
		prefix.unused = 0;
#ifdef WORDS_BIGENDIAN
		zstd_prefix_swap(&prefix);
#endif
		memcpy(dst, &prefix, sizeof(ZSTD_PREFIX));

		*result_lenp = zstd_ret + sizeof(ZSTD_PREFIX);
		*compression_failed = 0;
		return (0);
	}

	*compression_failed = 1;
	return (ZSTD_isError(zstd_ret) ?
	    zstd_error(compressor, session, "ZSTD_compress", &zstd_ret) : 0);
}

/*
 * zstd_decompress --
 *	WiredTiger Zstd decompression.
 */
static int
zstd_decompress(WT_COMPRESSOR *compressor, WT_SESSION *session,
    uint8_t *src, size_t src_len,
    uint8_t *dst, size_t dst_len,
    size_t *result_lenp)
{
	WT_EXTENSION_API *wt_api;
	ZSTD_PREFIX prefix;
	size_t zstd_ret;
	uint8_t *dst_tmp;

	(void)src_len;					/* Unused parameters */

	wt_api = ((ZSTD_COMPRESSOR *)compressor)->wt_api;

	/*
	 * Retrieve the true length of the compressed block and source and the
	 * decompressed bytes to return from the start of the source buffer.
	 */
	memcpy(&prefix, src, sizeof(ZSTD_PREFIX));
#ifdef WORDS_BIGENDIAN
	zstd_prefix_swap(&prefix);
#endif

	/*
	 * Decompress, starting after the prefix bytes.
	 *
	 * Two code paths, one with and one without a bounce buffer. When doing
	 * raw compression, we compress to a target size irrespective of row
	 * boundaries, and return to our caller a "useful" compression length
	 * based on the last complete row that was compressed. Our caller stores
	 * that length, not the length of bytes actually compressed by Zstd. In
	 * other words, our caller doesn't know how many bytes will result from
	 * decompression, likely hasn't provided us a large enough buffer, and
	 * we have to allocate a scratch buffer.
	 */
	if (dst_len < prefix.uncompressed_len) {
		if ((dst_tmp = wt_api->scr_alloc(
		   wt_api, session, prefix.uncompressed_len)) == NULL)
			return (ENOMEM);

		zstd_ret = ZSTD_decompress(dst_tmp, prefix.uncompressed_len,
		    src + sizeof(ZSTD_PREFIX), prefix.compressed_len);
		memcpy(dst, dst_tmp, dst_len);

		wt_api->scr_free(wt_api, session, dst_tmp);
	} else
		zstd_ret = ZSTD_decompress(dst, dst_len,
		    src + sizeof(ZSTD_PREFIX), prefix.compressed_len);

	if (!ZSTD_isError(zstd_ret)) {
		*result_lenp = zstd_ret;
		return (0);
	}
	return (zstd_error(compressor, session, "ZSTD_decompress", &zstd_ret));
}

/*
 * zstd_find_slot --
 *	Find the slot containing the target offset (binary search).
 */
static inline uint32_t
zstd_find_slot(size_t target, uint32_t *offsets, uint32_t slots)
{
	uint32_t base, indx, limit;

	indx = 1;

	/* Figure out which slot we got to: binary search */
	if (target >= offsets[slots])
		indx = slots;
	else if (target > offsets[1])
		for (base = 2, limit = slots - base; limit != 0; limit >>= 1) {
			indx = base + (limit >> 1);
			if (target < offsets[indx])
				continue;
			base = indx + 1;
			--limit;
		}

	return (indx);
}

/*
 * zstd_compress_raw --
 *	Pack records into a specified on-disk page size.
 */
static int
zstd_compress_raw(WT_COMPRESSOR *compressor, WT_SESSION *session,
    size_t page_max, int split_pct, size_t extra,
    uint8_t *src, uint32_t *offsets, uint32_t slots,
    uint8_t *dst, size_t dst_len, int final,
    size_t *result_lenp, uint32_t *result_slotsp)
{
	WT_EXTENSION_API *wt_api;
	ZSTD_COMPRESSOR *zcompressor;
	ZSTD_CStream *cstream;
	ZSTD_PREFIX prefix;
	ZSTD_inBuffer input;
	ZSTD_outBuffer output;
	uint32_t slot;
	size_t zstd_ret;
	int ret;

	/*
	 * !!!
	 * Zstd has streaming APIs similar to Zlib's which means that we can
	 * implement raw compression support for WiredTiger, but we don't ever
	 * run it in production for a couple of reasons.
	 *
	 * First, the ZSTD_endStream call to finish the compression can require
	 * hundreds of bytes to be reserved in the buffer in order for us to be
	 * relatively sure ZSTD_endStream will succeed (unlike Zlib, where we
	 * reserve 24 bytes).
	 *
	 * Second, the ZSTD_createCStream/ZSTD_initCStream calls to initialize
	 * a compression run are expensive and relatively slow; the Zstd stream
	 * functions have no idea of how much data they'll compress, and they
	 * configure for a worse case, a long stream. (As an example, we have
	 * seen ZSTD_initCStream at high compression levels allocate/initialize
	 * 650MB of memory.)
	 *
	 * We could make this better.
	 *
	 * We could speed initialization up by caching per-session ZSTD_CStream
	 * cookies. Note that requires real work: there's no per-session
	 * compression structure where we can easily add a cached cookie, plus
	 * we'd also need a clean-up function so WiredTiger application threads
	 * temporarily tasked with eviction aren't left tying down big memory.
	 *
	 * Also, there are experimental (non-standard) Zstd APIs allowing the
	 * application to configure the streaming APIs with a more realistic
	 * idea of how much data will be compressed, so they won't tie down as
	 * many resources. I've never tried those APIs, so I don't know if they
	 * would be effective or not.
	 *
	 * Finally, the simpler zstd_compress significantly outperforms this
	 * function (even when the ZSTD_CStream handles are cached), so for
	 * now, the code is here and it works, but it's unlikely to be used
	 * in production.
	 */

	(void)split_pct;				/* Unused variables */
	(void)final;

	zcompressor = (ZSTD_COMPRESSOR *)compressor;
	wt_api = zcompressor->wt_api;
	ret = 0;

	if ((cstream = ZSTD_createCStream()) == NULL)
		return (zstd_error(
		    compressor, session, "ZSTD_createCStream", NULL));
	zstd_ret = ZSTD_initCStream(cstream, zcompressor->compression_level);
	if (ZSTD_isError(zstd_ret)) {
		ret = zstd_error(
		    compressor, session, "ZSTD_initCStream", &zstd_ret);
		goto fail;
	}

	input.src = src;
	input.size = (size_t)offsets[slots];
	input.pos = 0;
	output.dst = dst + sizeof(ZSTD_PREFIX);
	output.size =
	    ((dst_len - sizeof(ZSTD_PREFIX)) < page_max ?
	    (dst_len - sizeof(ZSTD_PREFIX)) : page_max) -
	    (extra + zcompressor->finish_reserve);
	output.pos = 0;
	zstd_ret = ZSTD_compressStream(cstream, &output, &input);
	if (ZSTD_isError(zstd_ret)) {
		ret = zstd_error(
		    compressor, session, "ZSTD_compressStream", &zstd_ret);
		goto fail;
	}

	/*
	 * Find the first slot we didn't compress; I can't imagine we'd ever
	 * fail entirely, but it costs nothing to be safe.
	 */
	slot = zstd_find_slot(input.pos, offsets, slots);
	if (slot == 0)
		goto fail;

	/* Add in the reserved bytes and finish up the stream compression. */
	output.size += zcompressor->finish_reserve;
	zstd_ret = ZSTD_endStream(cstream, &output);
	if (ZSTD_isError(zstd_ret)) {
		ret = zstd_error(
		    compressor, session, "ZSTD_endStream", &zstd_ret);
		goto fail;
	}

	/* Fail if there's not enough space to empty the internal buffer. */
	if (zstd_ret != 0)
		goto fail;

	/*
	 * If we didn't compress something useful, tell our caller we're giving
	 * up. The 4KB constant (the WiredTiger default allocation size), is an
	 * arbitrary measure of failure, if we didn't gain 4KB, it's not worth
	 * the effort.
	 */
	if (offsets[slot] < (4 * 1024) ||
	    output.pos >= offsets[slot] - (4 * 1024))
		goto fail;

	prefix.compressed_len = (uint32_t)output.pos;
	prefix.uncompressed_len = (uint32_t)input.pos;
	prefix.useful_len = offsets[slot];
	prefix.unused = 0;

#ifdef WORDS_BIGENDIAN
	zstd_prefix_swap(&prefix);
#endif
	memcpy(dst, &prefix, sizeof(ZSTD_PREFIX));

	*result_slotsp = slot;
	*result_lenp = output.pos + sizeof(ZSTD_PREFIX);

	if (0) {
fail:		*result_slotsp = 0;
		*result_lenp = 1;
	}
	ZSTD_freeCStream(cstream);
	return (ret);
}

/*
 * zstd_pre_size --
 *	WiredTiger Zstd destination buffer sizing for compression.
 */
static int
zstd_pre_size(WT_COMPRESSOR *compressor, WT_SESSION *session,
    uint8_t *src, size_t src_len, size_t *result_lenp)
{
	(void)compressor;				/* Unused parameters */
	(void)session;
	(void)src;

	/*
	 * Zstd compression runs faster if the destination buffer is sized at
	 * the upper-bound of the buffer size needed by the compression. Use
	 * the library calculation of that overhead (plus our overhead).
	 */
	*result_lenp = ZSTD_compressBound(src_len) + sizeof(ZSTD_PREFIX);
	return (0);
}

/*
 * zstd_terminate --
 *	WiredTiger Zstd compression termination.
 */
static int
zstd_terminate(WT_COMPRESSOR *compressor, WT_SESSION *session)
{
	(void)session;					/* Unused parameters */

	free(compressor);
	return (0);
}

/*
 * zstd_add_compressor --
 *	Add a Zstd compressor.
 */
static int
zstd_add_compressor(WT_CONNECTION *connection, int raw, const char *name)
{
	ZSTD_COMPRESSOR *zstd_compressor;

	/*
	 * There are two almost identical Zstd compressors: one using raw
	 * compression to target a specific block size, and one without.
	 */
	if ((zstd_compressor = calloc(1, sizeof(ZSTD_COMPRESSOR))) == NULL)
		return (errno);

	zstd_compressor->compressor.compress = zstd_compress;
	zstd_compressor->compressor.compress_raw = raw ?
	    zstd_compress_raw : NULL;
	zstd_compressor->compressor.decompress = zstd_decompress;
	zstd_compressor->compressor.pre_size = zstd_pre_size;
	zstd_compressor->compressor.terminate = zstd_terminate;

	zstd_compressor->wt_api = connection->get_extension_api(connection);

	/*
	 * Zstd's sweet-spot is better compression than zlib at significantly
	 * faster compression/decompression speeds. LZ4 and snappy are faster
	 * than zstd, but have worse compression ratios. Applications wanting
	 * faster compression/decompression with worse compression will select
	 * LZ4 or snappy, so we configure zstd for better compression.
	 *
	 * From the zstd github site, default measurements of the compression
	 * engines we support, listing compression ratios with compression and
	 * decompression speeds:
	 *
	 *	Name	Ratio	C.speed	D.speed
	 *			MB/s	MB/s
	 *	zstd	2.877	330	940
	 *	zlib	2.730	95	360
	 *	LZ4	2.101	620	3100
	 *	snappy	2.091	480	1600
	 *
	 * Set the zstd compression level to 3: according to the zstd web site,
	 * that reduces zstd's compression speed to around 200 MB/s, increasing
	 * the compression ratio to 3.100 (close to zlib's best compression
	 * ratio). In other words, position zstd as a zlib replacement, having
	 * similar compression at much higher compression/decompression speeds.
	 */
	zstd_compressor->compression_level = 3;

	/*
	 * Experimentally derived, reserve this many bytes for zlib to finish
	 * up a buffer. If this isn't sufficient, we don't fail but we will be
	 * inefficient.
	 */
	zstd_compressor->finish_reserve = 256;

	/* Load the compressor */
	return (connection->add_compressor(
	    connection, name, (WT_COMPRESSOR *)zstd_compressor, NULL));
}

int zstd_extension_init(WT_CONNECTION *, WT_CONFIG_ARG *);

/*
 * zstd_extension_init --
 *	WiredTiger Zstd compression extension - called directly when Zstd
 * support is built in, or via wiredtiger_extension_init when Zstd support
 * is included via extension loading.
 */
int
zstd_extension_init(WT_CONNECTION *connection, WT_CONFIG_ARG *config)
{
	int ret;

	(void)config;    				/* Unused parameters */

	if ((ret = zstd_add_compressor(connection, 1, "zstd")) != 0)
		return (ret);
	if ((ret = zstd_add_compressor(connection, 0, "zstd-noraw")) != 0)
		return (ret);
	return (0);
}

/*
 * We have to remove this symbol when building as a builtin extension otherwise
 * it will conflict with other builtin libraries.
 */
#ifndef	HAVE_BUILTIN_EXTENSION_ZSTD
/*
 * wiredtiger_extension_init --
 *	WiredTiger Zstd compression extension.
 */
int
wiredtiger_extension_init(WT_CONNECTION *connection, WT_CONFIG_ARG *config)
{
	return (zstd_extension_init(connection, config));
}
#endif
