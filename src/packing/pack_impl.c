/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __struct_check --
 *     Check that the specified packing format is valid, and whether it fits into a fixed-sized
 *     bitfield.
 */
static int
__struct_check(
  WT_SESSION_IMPL *session, const char *fmt, size_t len, bool *fixedp, uint32_t *fixed_lenp)
{
    WT_DECL_PACK_VALUE(pv);
    WT_DECL_RET;
    WT_PACK pack;
    int fields;

    WT_RET(__pack_initn(session, &pack, fmt, len));
    for (fields = 0; (ret = __pack_next(&pack, &pv)) == 0; fields++)
        ;
    WT_RET_NOTFOUND_OK(ret);

    if (fixedp != NULL && fixed_lenp != NULL) {
        if (fields == 0) {
            *fixedp = true;
            *fixed_lenp = 0;
        } else if (fields == 1 && pv.type == 't') {
            *fixedp = true;
            *fixed_lenp = pv.size;
        } else
            *fixedp = false;
    }

    return (0);
}

/*
 * __wt_struct_confchk --
 *     Check that the specified packing format is valid, configuration version.
 */
int
__wt_struct_confchk(WT_SESSION_IMPL *session, WT_CONFIG_ITEM *v)
{
    return (__struct_check(session, v->str, v->len, NULL, NULL));
}

/*
 * __wt_struct_size --
 *     Calculate the size of a packed byte string.
 */
int
__wt_struct_size(WT_SESSION_IMPL *session, size_t *lenp, const char *fmt, ...)
{
    WT_DECL_RET;
    va_list ap;

    va_start(ap, fmt);
    ret = __wt_struct_sizev(session, lenp, fmt, ap);
    va_end(ap);

    return (ret);
}

/*
 * __wt_struct_pack --
 *     Pack a byte string.
 */
int
__wt_struct_pack(WT_SESSION_IMPL *session, void *buffer, size_t len, const char *fmt, ...)
{
    WT_DECL_RET;
    va_list ap;

    va_start(ap, fmt);
    ret = __wt_struct_packv(session, buffer, len, fmt, ap);
    va_end(ap);

    return (ret);
}

/*
 * __wt_struct_unpack --
 *     Unpack a byte string.
 */
int
__wt_struct_unpack(WT_SESSION_IMPL *session, const void *buffer, size_t len, const char *fmt, ...)
{
    WT_DECL_RET;
    va_list ap;

    va_start(ap, fmt);
    ret = __wt_struct_unpackv(session, buffer, len, fmt, ap);
    va_end(ap);

    return (ret);
}

/*
 * __wt_struct_repack --
 *     Return the subset of the packed buffer that represents part of the format. If the result is
 *     not contiguous in the existing buffer, a buffer is reallocated and filled.
 */
int
__wt_struct_repack(WT_SESSION_IMPL *session, const char *infmt, const char *outfmt,
  const WT_ITEM *inbuf, WT_ITEM *outbuf)
{
    WT_DECL_PACK_VALUE(pvin);
    WT_DECL_PACK_VALUE(pvout);
    WT_DECL_RET;
    WT_PACK packin, packout;
    const uint8_t *before, *end, *p;
    const void *start;

    start = NULL;
    p = inbuf->data;
    end = p + inbuf->size;

    WT_RET(__pack_init(session, &packout, outfmt));
    WT_RET(__pack_init(session, &packin, infmt));

    /* Outfmt should complete before infmt */
    while ((ret = __pack_next(&packout, &pvout)) == 0) {
        if (p >= end)
            WT_RET(EINVAL);
        if (pvout.type == 'x' && pvout.size == 0 && pvout.havesize)
            continue;
        WT_RET(__pack_next(&packin, &pvin));
        before = p;
        WT_RET(__unpack_read(session, &pvin, &p, (size_t)(end - p)));
        if (pvout.type != pvin.type)
            WT_RET(ENOTSUP);
        if (start == NULL)
            start = before;
    }
    WT_RET_NOTFOUND_OK(ret);

    /* Be paranoid - __pack_write should never overflow. */
    WT_ASSERT(session, p <= end);

    outbuf->data = start;
    outbuf->size = WT_PTRDIFF(p, start);

    return (0);
}

/*
 * __pack_int_len --
 *     Return the packed length of a variable-length integer from its first byte, 0 if the byte
 *     cannot start one.
 */
static size_t
__pack_int_len(uint8_t first)
{
    switch (first & 0xf0) {
    case NEG_MULTI_MARKER:
        return (1 + sizeof(uint64_t) - (size_t)(first & 0xf));
    case NEG_2BYTE_MARKER:
    case NEG_2BYTE_MARKER | 0x10:
    case POS_2BYTE_MARKER:
    case POS_2BYTE_MARKER | 0x10:
        return (2);
    case NEG_1BYTE_MARKER:
    case NEG_1BYTE_MARKER | 0x10:
    case NEG_1BYTE_MARKER | 0x20:
    case NEG_1BYTE_MARKER | 0x30:
    case POS_1BYTE_MARKER:
    case POS_1BYTE_MARKER | 0x10:
    case POS_1BYTE_MARKER | 0x20:
    case POS_1BYTE_MARKER | 0x30:
        return (1);
    case POS_MULTI_MARKER:
        return (1 + (size_t)(first & 0xf));
    default:
        return (0);
    }
}

/*
 * __complete_fill --
 *     Append a run of one byte value to a buffer.
 */
static int
__complete_fill(WT_SESSION_IMPL *session, WT_ITEM *buf, uint8_t byte, size_t count)
{
    if (count == 0)
        return (0);
    WT_RET(__wt_buf_grow(session, buf, buf->size + count));
    memset((uint8_t *)buf->mem + buf->size, byte, count);
    buf->size += count;
    return (0);
}

/*
 * __complete_int --
 *     Append the bytes that turn the first `have` bytes of a variable-length integer into the
 *     smallest canonical encoding starting with them. The length is fixed by the first byte; the
 *     bytes after it are big-endian magnitude, so zero is the smallest filler, except that the
 *     first byte of a multi-byte magnitude cannot be a leading zero (or 0xff for a negative value
 *     with no stripped 0xff bytes) or the number would have packed shorter. The one-byte positive
 *     magnitude is the exception: the packer writes the first multi-byte value as a zero byte so
 *     that no encoding is shorter than the two-byte form.
 */
static int
__complete_int(WT_SESSION_IMPL *session, const uint8_t *p, size_t have, size_t len, WT_ITEM *buf)
{
    uint8_t lead;

    lead = 0;
    if (have == 1) {
        if ((p[0] & 0xf0) == POS_MULTI_MARKER && (p[0] & 0xf) > 1)
            lead = 0x01;
        else if (p[0] == NEG_MULTI_MARKER)
            lead = 0x80;
    }
    WT_RET(__complete_fill(session, buf, lead, 1));
    return (__complete_fill(session, buf, 0, len - have - 1));
}

/*
 * __complete_min --
 *     Append the smallest encoding of a field type.
 */
static int
__complete_min(WT_SESSION_IMPL *session, WT_PACK_VALUE *pv, WT_ITEM *buf)
{
    size_t len;
    uint8_t *p, tmp[WT_INTPACK64_MAXSIZE];

    switch (pv->type) {
    case 'x':
    case 's':
        return (__complete_fill(session, buf, 0, pv->size));
    case 'S':
        return (__complete_fill(session, buf, 0, pv->havesize ? pv->size : 1));
    case 'u':
        /* A trailing raw item is smallest when empty, a sized one when all zeros. */
        return (__complete_fill(session, buf, 0, pv->havesize ? pv->size : 0));
    case 'U':
    case 'H':
    case 'I':
    case 'L':
    case 'Q':
    case 'r':
        return (__complete_fill(session, buf, POS_1BYTE_MARKER, 1));
    case 'b':
    case 'B':
    case 't':
        return (__complete_fill(session, buf, 0, 1));
    case 'R':
        return (__complete_fill(session, buf, 0, sizeof(uint64_t)));
    case 'h':
    case 'i':
    case 'l':
    case 'q':
        p = tmp;
        WT_RET(__wt_vpack_int(&p, sizeof(tmp),
          pv->type == 'h'   ? INT16_MIN :
            pv->type == 'q' ? INT64_MIN :
                              INT32_MIN));
        len = WT_PTRDIFF(p, tmp);
        WT_RET(__wt_buf_grow(session, buf, buf->size + len));
        memcpy((uint8_t *)buf->mem + buf->size, tmp, len);
        buf->size += len;
        return (0);
    default:
        return (__wt_illegal_value(session, pv->type));
    }
}

/*
 * __wt_struct_complete_prefix --
 *     Extend a prefix of a packed key, such as a suffix-truncated separator, into a complete key of
 *     the format. Fields entirely present are kept, the field the prefix ends inside is completed
 *     with the smallest value whose encoding starts with the bytes present, and every later field
 *     gets the smallest encoding of its type. Because the format's packing sorts in byte order, the
 *     result is no greater than any complete key that starts with the prefix and, like any
 *     extension of the prefix, greater than every key the prefix sorts above. An empty prefix is
 *     left empty.
 */
int
__wt_struct_complete_prefix(WT_SESSION_IMPL *session, const char *fmt, WT_ITEM *buf)
{
    WT_DECL_ITEM(tail);
    WT_DECL_PACK(pack);
    WT_DECL_PACK_VALUE(pv);
    WT_DECL_RET;
    size_t have, len, plen;
    uint64_t size;
    uint8_t prefix[WT_INTPACK64_MAXSIZE];
    const uint8_t *end, *nul, *p, *q;
    bool cut;

    if (buf->size == 0)
        return (0);

    WT_RET(__wt_scr_alloc(session, 0, &tail));
    WT_ERR(__pack_init(session, &pack, fmt));

    p = buf->data;
    end = p + buf->size;
    cut = false;
    while ((ret = __pack_next(&pack, &pv)) == 0) {
        have = WT_PTRDIFF(end, p);
        len = 0;
        if (cut || have == 0) {
            cut = true;
            WT_ERR(__complete_min(session, &pv, tail));
            continue;
        }

        /* Find the field's full length from the bytes present, then keep or complete it. */
        switch (pv.type) {
        case 'x':
        case 's':
            len = pv.size;
            break;
        case 'S':
            if (pv.havesize)
                len = pv.size;
            else
                len = (nul = memchr(p, 0, have)) == NULL ? have + 1 : WT_PTRDIFF(nul, p) + 1;
            break;
        case 'u':
            len = pv.havesize ? pv.size : have;
            break;
        case 'U':
            if ((plen = __pack_int_len(p[0])) == 0)
                WT_ERR_MSG(session, EINVAL, "invalid packed size in key prefix");
            if (plen > have) {
                /* Cut inside the size prefix: complete the size, then supply that many bytes. */
                WT_ERR(__complete_int(session, p, have, plen, tail));
                memcpy(prefix, p, have);
                memcpy(
                  prefix + have, (uint8_t *)tail->data + tail->size - (plen - have), plen - have);
                q = prefix;
                WT_ERR(__wt_vunpack_uint(&q, plen, &size));
                WT_ERR(__complete_fill(session, tail, 0, (size_t)size));
                len = 0;
                break;
            }
            q = p;
            WT_ERR(__wt_vunpack_uint(&q, have, &size));
            len = plen + (size_t)size;
            break;
        case 'b':
        case 'B':
        case 't':
            len = 1;
            break;
        case 'h':
        case 'i':
        case 'l':
        case 'q':
        case 'H':
        case 'I':
        case 'L':
        case 'Q':
        case 'r':
            if ((len = __pack_int_len(p[0])) == 0)
                WT_ERR_MSG(session, EINVAL, "invalid packed integer in key prefix");
            if (len > have) {
                WT_ERR(__complete_int(session, p, have, len, tail));
                len = 0;
            }
            break;
        case 'R':
            len = sizeof(uint64_t);
            break;
        default:
            WT_ERR(__wt_illegal_value(session, pv.type));
        }

        if (len != 0 && len <= have) {
            p += len;
            continue;
        }

        /* Integers and cut size prefixes are already completed, everything else pads with zeros. */
        if (len > have)
            WT_ERR(__complete_fill(session, tail, 0, len - have));
        p = end;
        cut = true;
    }
    WT_ERR_NOTFOUND_OK(ret, false);

    if (tail->size != 0) {
        WT_ERR(__wt_buf_grow(session, buf, buf->size + tail->size));
        memcpy((uint8_t *)buf->mem + buf->size, tail->data, tail->size);
        buf->size += tail->size;
    }

err:
    __wt_scr_free(session, &tail);
    return (ret);
}
