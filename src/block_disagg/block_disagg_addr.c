/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * Address cookie version numbers.
 *
 * Before incrementing the version number, first check if the format change could be more better
 * handled by introducing a new flag. For example, a flag would be useful if adding an optional
 * field, while a new version number would be more appropriate when introducing a new mandatory
 * field, changing the meaning of an existing field, or removing a field or a flag.
 */
#define WT_BLOCK_DISAGG_ADDR_VERSION 0
#define WT_BLOCK_DISAGG_ADDR_VERSION_MIN 0 /* The oldest version that can read this format. */

/*
 * __block_disagg_addr_pack_version --
 *     Pack the address cookie version into the buffer.
 */
static inline int
__block_disagg_addr_pack_version(uint8_t **pp, size_t maxlen)
{
    uint8_t *p;
    p = *pp;

    WT_SIZE_CHECK_PACK(1, maxlen);
    *p++ = WT_BLOCK_DISAGG_ADDR_VERSION | (WT_BLOCK_DISAGG_ADDR_VERSION_MIN << 4);

    *pp = p;
    return (0);
}

/*
 * __block_disagg_addr_unpack_version --
 *     Unpack the address cookie version from the buffer.
 */
static inline int
__block_disagg_addr_unpack_version(
  const uint8_t **pp, size_t maxlen, uint8_t *version, uint8_t *version_min)
{
    uint8_t version_byte;
    const uint8_t *p;
    p = *pp;

    WT_SIZE_CHECK_UNPACK(1, maxlen);
    version_byte = *p++;
    *version = version_byte & 0x0f;
    *version_min = version_byte >> 4;

    *pp += 1;
    return (0);
}

/*
 * __block_disagg_addr_pack_uint32 --
 *     Pack a 32-bit unsigned integer into the address cookie.
 */
static inline int
__block_disagg_addr_pack_uint32(uint8_t **pp, size_t maxlen, uint32_t value)
{
    uint8_t *p;
    p = *pp;

    WT_SIZE_CHECK_PACK(4, maxlen);
    memcpy(p, &value, sizeof(value));
    p += sizeof(value);

    *pp = p;
    return (0);
}

/*
 * __block_disagg_addr_unpack_uint32 --
 *     Unpack a 32-bit unsigned integer from the address cookie.
 */
static inline int
__block_disagg_addr_unpack_uint32(const uint8_t **pp, size_t maxlen, uint32_t *valuep)
{
    const uint8_t *p;
    p = *pp;

    WT_SIZE_CHECK_UNPACK(4, maxlen);
    memcpy(valuep, p, sizeof(*valuep));
    p += sizeof(*valuep);

    *pp = p;
    return (0);
}

/*
 * __wti_block_disagg_addr_pack --
 *     Convert the filesystem components into its address cookie.
 */
int
__wti_block_disagg_addr_pack(WT_SESSION_IMPL *session, uint8_t **pp, uint64_t page_id,
  uint64_t flags, uint64_t lsn, uint64_t base_lsn, uint32_t size, uint32_t checksum)
{
    uint64_t base_lsn_delta;

    if (size == 0) {
        page_id = WT_BLOCK_INVALID_PAGE_ID;
        flags = 0;
        size = checksum = 0;
        lsn = base_lsn = 0;
    }

    /* We will store the base LSN as a delta relative to the LSN to save space. */
    WT_ASSERT_ALWAYS(session, lsn > base_lsn,
      "LSN %" PRIu64 " must be larger than base LSN %" PRIu64, lsn, base_lsn);
    base_lsn_delta = lsn - base_lsn;

    /* Write the address version. */
    WT_RET(__block_disagg_addr_pack_version(pp, 0));

    /* Pack the address cookie. */
    WT_RET(__wt_vpack_uint(pp, 0, page_id));
    WT_RET(__wt_vpack_uint(pp, 0, flags));
    WT_RET(__wt_vpack_uint(pp, 0, lsn));
    WT_RET(__wt_vpack_uint(pp, 0, base_lsn_delta));
    WT_RET(__wt_vpack_uint(pp, 0, size));

    /* Pack the checksum as a fixed-length 32-bit integer. */
    WT_RET(__block_disagg_addr_pack_uint32(pp, 0, checksum));

    return (0);
}

/*
 * __wti_block_disagg_addr_unpack --
 *     Convert a disaggregated address cookie into its components UPDATING the caller's buffer
 *     reference.
 */
int
__wti_block_disagg_addr_unpack(WT_SESSION_IMPL *session, const uint8_t **buf, size_t buf_size,
  uint64_t *page_idp, uint64_t *flagsp, uint64_t *lsnp, uint64_t *base_lsnp, uint32_t *sizep,
  uint32_t *checksump)
{
    uint64_t base_lsn, base_lsn_delta, flags, lsn, page_id, size, unsupported_flags;
    uint32_t checksum;
    uint8_t version, version_min;
    const uint8_t *begin;

    begin = *buf;
    page_id = 0; /* Avoid compiler warnings. */

    /* Unpack the address version. */
    WT_RET(__block_disagg_addr_unpack_version(buf, 0, &version, &version_min));
    if (version_min > WT_BLOCK_DISAGG_ADDR_VERSION)
        WT_RET_MSG(session, ENOTSUP,
          "Unsupported disaggregated address cookie version %" PRIu8 ", min %" PRIu8, version,
          version_min);

    /* Unpack the address cookie. */
    WT_RET(__wt_vunpack_uint(buf, 0, &page_id));
    WT_RET(__wt_vunpack_uint(buf, 0, &flags));
    WT_RET(__wt_vunpack_uint(buf, 0, &lsn));
    WT_RET(__wt_vunpack_uint(buf, 0, &base_lsn_delta));
    WT_RET(__wt_vunpack_uint(buf, 0, &size));

    /* Unpack the checksum as a fixed-length 32-bit integer. */
    WT_RET(__block_disagg_addr_unpack_uint32(buf, 0, &checksum));

    /* Get the base LSN from the delta. */
    if (lsn < base_lsn_delta)
        WT_RET_MSG(session, EINVAL,
          "Disaggregated address cookie LSN %" PRIu64 " is smaller than base LSN delta %" PRIu64,
          lsn, base_lsn_delta);
    base_lsn = lsn - base_lsn_delta;

    /*
     * Any disagg ID is valid, so use a size of 0 to define an out-of-band value.
     */
    if (size == 0) {
        *page_idp = WT_BLOCK_INVALID_PAGE_ID;
        *flagsp = 0;
        *lsnp = *base_lsnp = 0;
        *sizep = *checksump = 0;
    } else {
        *page_idp = page_id;
        *flagsp = flags;
        *lsnp = lsn;
        *base_lsnp = base_lsn;
        *sizep = (uint32_t)size;
        *checksump = checksum;
    }

    /*
     * Check the address cookie size, but only (1) if we are reading the current version of the
     * address cookie, and (2) if there are no unsupported flags. If we are reading a new version,
     * we can't check the size, as more fields could have been added.
     */
    unsupported_flags = flags;
    FLD_CLR(unsupported_flags, WT_BLOCK_DISAGG_ADDR_ALL_FLAGS);
    if ((size_t)(*buf - begin) != buf_size && version == WT_BLOCK_DISAGG_ADDR_VERSION &&
      unsupported_flags == 0)
        WT_RET_MSG(session, EINVAL,
          "Disaggregated address cookie size mismatch: expected %" PRIuMAX ", got %" PRIuMAX,
          (uintmax_t)buf_size, (uintmax_t)(*buf - begin));

    return (0);
}

/*
 * __wti_block_disagg_addr_invalid --
 *     Return an error code if an address cookie is invalid.
 */
int
__wti_block_disagg_addr_invalid(WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size)
{
    uint64_t base_lsn, flags, lsn, page_id;
    uint32_t checksum, size;

    /* Crack the cookie - there aren't further checks for object blocks. */
    WT_RET(__wti_block_disagg_addr_unpack(
      session, &addr, addr_size, &page_id, &flags, &lsn, &base_lsn, &size, &checksum));

    return (0);
}

/*
 * __wti_block_disagg_addr_string --
 *     Return a printable string representation of an address cookie.
 */
int
__wti_block_disagg_addr_string(
  WT_BM *bm, WT_SESSION_IMPL *session, WT_ITEM *buf, const uint8_t *addr, size_t addr_size)
{
    uint64_t base_lsn, flags, lsn, page_id;
    uint32_t checksum, size;

    WT_UNUSED(bm);

    /* Crack the cookie. */
    WT_RET(__wti_block_disagg_addr_unpack(
      session, &addr, addr_size, &page_id, &flags, &lsn, &base_lsn, &size, &checksum));

    /* Printable representation. */
    WT_RET(__wt_buf_fmt(session, buf,
      "[%" PRIuMAX ", %" PRIxMAX ", %" PRIuMAX ", %" PRIuMAX ", %" PRIu32 ", %" PRIu32 "]",
      (uintmax_t)page_id, (uintmax_t)flags, (uintmax_t)lsn, (uintmax_t)base_lsn, size, checksum));

    return (0);
}

/*
 * __wti_block_disagg_ckpt_pack --
 *     Pack the raw content of a checkpoint record for this disagg manager. It will be encoded in
 *     the metadata for the table and used to find the checkpoint again in the future.
 */
int
__wti_block_disagg_ckpt_pack(WT_SESSION_IMPL *session, WT_BLOCK_DISAGG *block_disagg, uint8_t **buf,
  uint64_t root_id, uint64_t flags, uint64_t lsn, uint64_t base_lsn, uint32_t root_sz,
  uint32_t root_checksum)
{
    WT_UNUSED(block_disagg);

    WT_RET(__wti_block_disagg_addr_pack(
      session, buf, root_id, flags, lsn, base_lsn, root_sz, root_checksum));

    return (0);
}

/*
 * __wti_block_disagg_ckpt_unpack --
 *     Pack the raw content of a checkpoint record for this disagg manager. It will be encoded in
 *     the metadata for the table and used to find the checkpoint again in the future.
 */
int
__wti_block_disagg_ckpt_unpack(WT_SESSION_IMPL *session, WT_BLOCK_DISAGG *block_disagg,
  const uint8_t *buf, size_t buf_size, uint64_t *root_id, uint64_t *flags, uint64_t *lsn,
  uint64_t *base_lsn, uint32_t *root_sz, uint32_t *root_checksum)
{
    WT_UNUSED(block_disagg);

    /* Retrieve the root page information */
    WT_RET(__wti_block_disagg_addr_unpack(
      session, &buf, buf_size, root_id, flags, lsn, base_lsn, root_sz, root_checksum));

    return (0);
}
