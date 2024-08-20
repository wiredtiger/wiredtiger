/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_bmp_addr_string --
 *     Return a printable string representation of an address cookie.
 */
int
__wt_bmp_addr_string(
  WT_BM *bm, WT_SESSION_IMPL *session, WT_ITEM *buf, const uint8_t *addr, size_t addr_size)
{
    return (
      __wt_block_pantry_addr_string(session, (WT_BLOCK_PANTRY *)bm->block, buf, addr, addr_size));
}

/*
 * __wt_block_pantry_addr_to_buffer --
 *     Convert the filesystem components into its address cookie.
 */
int
__wt_block_pantry_addr_to_buffer(uint8_t **pp, uint64_t pantry_id, uint32_t size, uint32_t checksum)
{
    uint64_t c, p, s;

    /* See the comment above: this is the reverse operation. */
    if (size == 0) {
        p = WT_BLOCK_PANTRY_ID_INVALID;
        s = c = 0;
    } else {
        p = pantry_id;
        s = size;
        c = checksum;
    }
    WT_RET(__wt_vpack_uint(pp, 0, p));
    WT_RET(__wt_vpack_uint(pp, 0, s));
    WT_RET(__wt_vpack_uint(pp, 0, c));
    return (0);
}

/*
 * __wt_block_pantry_buffer_to_addr --
 *     Convert a filesystem address cookie into its components NOT UPDATING the caller's buffer
 *     reference.
 */
int
__wt_block_pantry_buffer_to_addr(
  const uint8_t *buf, uint64_t *pantry_idp, uint32_t *sizep, uint32_t *checksump)
{
    uint64_t c, p, s;

    WT_RET(__wt_vunpack_uint(&buf, 0, &p));
    WT_RET(__wt_vunpack_uint(&buf, 0, &s));
    WT_RET(__wt_vunpack_uint(&buf, 0, &c));

    /*
     * Any pantry ID is valid, so use a size of 0 to define an out-of-band value.
     */
    if (s == 0) {
        *pantry_idp = WT_BLOCK_PANTRY_ID_INVALID;
        *sizep = *checksump = 0;
    } else {
        *pantry_idp = p;
        *sizep = (uint32_t)s;
        *checksump = (uint32_t)c;
    }
    return (0);
}

/*
 * __wt_block_pantry_addr_invalid --
 *     Return an error code if an address cookie is invalid.
 */
int
__wt_block_pantry_addr_invalid(const uint8_t *addr)
{
    uint64_t pantry_id;
    uint32_t checksum, size;

    /* Crack the cookie - there aren't further checks for object blocks. */
    WT_RET(__wt_block_pantry_buffer_to_addr(addr, &pantry_id, &size, &checksum));

    return (0);
}

/*
 * __wt_block_pantry_addr_string --
 *     Return a printable string representation of an address cookie.
 */
int
__wt_block_pantry_addr_string(WT_SESSION_IMPL *session, WT_BLOCK_PANTRY *block_pantry, WT_ITEM *buf,
  const uint8_t *addr, size_t addr_size)
{
    uint64_t pantry_id;
    uint32_t checksum, size;

    WT_UNUSED(block_pantry);
    WT_UNUSED(addr_size);

    /* Crack the cookie. */
    WT_RET(__wt_block_pantry_buffer_to_addr(addr, &pantry_id, &size, &checksum));

    /* Printable representation. */
    WT_RET(__wt_buf_fmt(session, buf, "[%" PRIuMAX "-%" PRIuMAX ", %" PRIu32 ", %" PRIu32 "]",
      (uintmax_t)pantry_id, (uintmax_t)pantry_id + size, size, checksum));

    return (0);
}

/*
 * __wt_block_pantry_ckpt_pack --
 *     Pack the raw content of a checkpoint record for this pantry manager. It will be encoded in
 *     the metadata for the table and used to find the checkpoint again in the future.
 */
int
__wt_block_pantry_ckpt_pack(WT_BLOCK_PANTRY *block_pantry, uint8_t **buf, uint64_t root_id,
  uint32_t root_sz, uint32_t root_checksum)
{
    size_t len;
    WT_UNUSED(block_pantry);

    /* Pretend there is a root page with ID 0 and size 1024, so it can be unpacked */
    WT_RET(__wt_block_pantry_addr_to_buffer(buf, root_id, root_sz, root_checksum));
    /* Add something fun - because we are fun! */
    WT_RET(__wt_snprintf_len_set((char *)buf, WT_BLOCK_PANTRY_CHECKPOINT_BUFFER, &len, "%s",
      "supercalafragalisticexpialadoshus"));
    *buf += len;
    return (0);
}

/*
 * __wt_block_pantry_ckpt_unpack --
 *     Pack the raw content of a checkpoint record for this pantry manager. It will be encoded in
 *     the metadata for the table and used to find the checkpoint again in the future.
 */
int
__wt_block_pantry_ckpt_unpack(WT_BLOCK_PANTRY *block_pantry, uint8_t **buf)
{
    size_t len;
    uint64_t pantry_id;
    uint32_t checksum, size;
    WT_UNUSED(block_pantry);

    /* Retrieve the root page information */
    WT_RET(__wt_block_pantry_buffer_to_addr(*buf, &pantry_id, &size, &checksum));
    /* Add something fun - because we are fun! */
    WT_RET(__wt_snprintf_len_set((char *)buf, WT_BLOCK_PANTRY_CHECKPOINT_BUFFER, &len, "%s",
      "supercalafragalisticexpialadoshus"));
    *buf += len;
    return (0);
}
