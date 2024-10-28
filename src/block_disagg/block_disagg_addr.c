/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_block_disagg_addr_pack --
 *     Convert the filesystem components into its address cookie.
 */
int
__wt_block_disagg_addr_pack(uint8_t **pp, uint64_t page_id, uint64_t checkpoint_id,
  uint64_t reconciliation_id, uint32_t size, uint32_t checksum)
{
    uint64_t c, cp, p, r, s;
    /*
    uint8_t *orig;

    orig = *pp;
    */

    /* TODO: write extensible byte */
    /* See the comment above: this is the reverse operation. */
    if (size == 0) {
        p = WT_BLOCK_INVALID_PAGE_ID;
        s = c = 0;
        cp = r = 0;
    } else {
        p = page_id;
        cp = checkpoint_id;
        r = reconciliation_id;
        s = size;
        c = checksum;
    }
    WT_RET(__wt_vpack_uint(pp, 0, p));
    WT_RET(__wt_vpack_uint(pp, 0, cp));
    WT_RET(__wt_vpack_uint(pp, 0, r));
    WT_RET(__wt_vpack_uint(pp, 0, s));
    WT_RET(__wt_vpack_uint(pp, 0, c));

    /*
    fprintf(stderr, "__wt_block_disagg_addr_pack: 0x");
    while (orig != *pp) {
        fprintf(stderr, "%02x", *orig);
        orig++;
    }
    fprintf(stderr, "\n");
    */
    return (0);
}

/*
 * __wt_block_disagg_addr_unpack --
 *     Convert a disaggregated address cookie into its components UPDATING the caller's buffer
 *     reference.
 */
int
__wt_block_disagg_addr_unpack(const uint8_t **buf, size_t buf_size, uint64_t *page_idp,
  uint64_t *checkpoint_idp, uint64_t *reconciliation_idp, uint32_t *sizep, uint32_t *checksump)
{
    uint64_t c, cp, p, r, s;
    const uint8_t *begin;

    /* TODO: read extensible byte */
    begin = *buf;
    WT_RET(__wt_vunpack_uint(buf, 0, &p));
    WT_RET(__wt_vunpack_uint(buf, 0, &cp));
    WT_RET(__wt_vunpack_uint(buf, 0, &r));
    WT_RET(__wt_vunpack_uint(buf, 0, &s));
    WT_RET(__wt_vunpack_uint(buf, 0, &c));

    /*
     * Any disagg ID is valid, so use a size of 0 to define an out-of-band value.
     */
    if (s == 0) {
        *page_idp = WT_BLOCK_INVALID_PAGE_ID;
        *checkpoint_idp = *reconciliation_idp = 0;
        *sizep = *checksump = 0;
    } else {
        *page_idp = p;
        *checkpoint_idp = cp;
        *reconciliation_idp = r;
        *sizep = (uint32_t)s;
        *checksump = (uint32_t)c;
    }
    if ((size_t)(*buf - begin) != buf_size)
        return (EINVAL); /* TODO: need a message */

    return (0);
}

/*
 * __wt_block_disagg_addr_invalid --
 *     Return an error code if an address cookie is invalid.
 */
int
__wt_block_disagg_addr_invalid(const uint8_t *addr, size_t addr_size)
{
    uint64_t checkpoint_id, page_id, reconciliation_id;
    uint32_t checksum, size;

    /* Crack the cookie - there aren't further checks for object blocks. */
    WT_RET(__wt_block_disagg_addr_unpack(
      &addr, addr_size, &page_id, &checkpoint_id, &reconciliation_id, &size, &checksum));

    return (0);
}

/*
 * __wt_block_disagg_addr_string --
 *     Return a printable string representation of an address cookie.
 */
int
__wt_block_disagg_addr_string(
  WT_BM *bm, WT_SESSION_IMPL *session, WT_ITEM *buf, const uint8_t *addr, size_t addr_size)
{
    uint64_t checkpoint_id, page_id, reconciliation_id;
    uint32_t checksum, size;

    WT_UNUSED(bm);

    /* Crack the cookie. */
    WT_RET(__wt_block_disagg_addr_unpack(
      &addr, addr_size, &page_id, &checkpoint_id, &reconciliation_id, &size, &checksum));

    /* Printable representation. */
    WT_RET(__wt_buf_fmt(session, buf,
      "[%" PRIuMAX ", %" PRIuMAX ", %" PRIuMAX ", %" PRIu32 ", %" PRIu32 "]", (uintmax_t)page_id,
      (uintmax_t)checkpoint_id, (uintmax_t)reconciliation_id, size, checksum));

    return (0);
}

/*
 * __wt_block_disagg_ckpt_pack --
 *     Pack the raw content of a checkpoint record for this disagg manager. It will be encoded in
 *     the metadata for the table and used to find the checkpoint again in the future.
 */
int
__wt_block_disagg_ckpt_pack(WT_BLOCK_DISAGG *block_disagg, uint8_t **buf, uint64_t root_id,
  uint64_t checkpoint_id, uint64_t reconciliation_id, uint32_t root_sz, uint32_t root_checksum)
{
    /* size_t len; */
    WT_UNUSED(block_disagg);

    WT_RET(__wt_block_disagg_addr_pack(
      buf, root_id, checkpoint_id, reconciliation_id, root_sz, root_checksum));
    /* Add something fun - because we are fun! */
    /* WT_RET(__wt_snprintf_len_set((char *)buf, WT_BLOCK_DISAGG_CHECKPOINT_BUFFER, &len, "%s", */
    /*   "supercalafragalisticexpialadoshus")); */
    /* *buf += len; */
    return (0);
}

/*
 * __wt_block_disagg_ckpt_unpack --
 *     Pack the raw content of a checkpoint record for this disagg manager. It will be encoded in
 *     the metadata for the table and used to find the checkpoint again in the future.
 */
int
__wt_block_disagg_ckpt_unpack(WT_BLOCK_DISAGG *block_disagg, const uint8_t *buf, size_t buf_size,
  uint64_t *root_id, uint64_t *checkpoint_id, uint64_t *reconciliation_id, uint32_t *root_sz,
  uint32_t *root_checksum)
{
    WT_UNUSED(block_disagg);

    /* Retrieve the root page information */
    WT_RET(__wt_block_disagg_addr_unpack(
      &buf, buf_size, root_id, checkpoint_id, reconciliation_id, root_sz, root_checksum));
    /* Add something fun - because we are fun! */
    /* WT_RET(__wt_snprintf_len_set((char *)buf, WT_BLOCK_DISAGG_CHECKPOINT_BUFFER, &len, "%s", */
    /*   "supercalafragalisticexpialadoshus")); */
    return (0);
}
