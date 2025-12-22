/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

/*
 * WT_KEY_HEADER --
 *	Key encryption key header structure.
 */
WT_PACKED_STRUCT_BEGIN(__wt_key_header)
#define WT_KEY_HEADER_SIGNATURE 0x686b7477 /* 'wtkh' */
    uint32_t signature; /* 00-03: Key header signature; always 'wtkh' */
#define WT_KEY_HEADER_VERSION 1
    uint8_t version;    /* 04: Header version */
    uint8_t hdr_size;   /* 05: Header size, in bytes */
    uint32_t key_size;  /* 06-09: Payload size, in bytes */
    uint32_t checksum;  /* 10-13: Payload CRC32 checksum */
WT_PACKED_STRUCT_END

/*
 * __wt_key_header_byteswap --
 *     Handle big- and little-endian transformation of a key header.
 */
static WT_INLINE void
__wt_key_header_byteswap(WT_KEY_HEADER *hdr)
{
#ifdef WORDS_BIGENDIAN
    hdr->signature = __wt_bswap32(hdr->signature);
    hdr->key_size = __wt_bswap32(hdr->key_size);
    hdr->checksum = __wt_bswap32(hdr->checksum);
#else
    WT_UNUSED(hdr);
#endif
}
