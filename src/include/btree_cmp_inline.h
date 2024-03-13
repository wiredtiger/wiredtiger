/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

#ifdef HAVE_X86INTRIN_H
#if !defined(_MSC_VER) && !defined(_lint)
#include <x86intrin.h>
#endif
#endif

#if defined(HAVE_ARM_NEON_INTRIN_H)
#include <arm_neon.h>
#endif
/* 16B alignment */
#define WT_ALIGNED_16(p) (((uintptr_t)(p)&0x0f) == 0)
#define WT_VECTOR_SIZE 16 /* chunk size */

/*
 * __wt_lex_compare --
 *     Lexicographic comparison routine. Returns: < 0 if user_item is lexicographically < tree_item,
 *     = 0 if user_item is lexicographically = tree_item, > 0 if user_item is lexicographically >
 *     tree_item. We use the names "user" and "tree" so it's clear in the btree code which the
 *     application is looking at when we call its comparison function.
 */
static WT_INLINE int
__wt_lex_compare(const WT_ITEM *user_item, const WT_ITEM *tree_item)
{
    size_t len, usz, tsz;
    const uint8_t *userp, *treep;

    usz = user_item->size;
    tsz = tree_item->size;
    len = WT_MIN(usz, tsz);

    userp = (const uint8_t *)user_item->data;
    treep = (const uint8_t *)tree_item->data;

#ifdef HAVE_X86INTRIN_H
    /* Use vector instructions if we'll execute at least 2 of them. */
    if (len >= WT_VECTOR_SIZE * 2) {
        size_t remain;
        __m128i res_eq, u, t;

        remain = len % WT_VECTOR_SIZE;
        len -= remain;
        if (WT_ALIGNED_16(userp) && WT_ALIGNED_16(treep))
            for (; len > 0;
                 len -= WT_VECTOR_SIZE, userp += WT_VECTOR_SIZE, treep += WT_VECTOR_SIZE) {
                u = _mm_load_si128((const __m128i *)userp);
                t = _mm_load_si128((const __m128i *)treep);
                res_eq = _mm_cmpeq_epi8(u, t);
                if (_mm_movemask_epi8(res_eq) != 65535)
                    break;
            }
        else
            for (; len > 0;
                 len -= WT_VECTOR_SIZE, userp += WT_VECTOR_SIZE, treep += WT_VECTOR_SIZE) {
                u = _mm_loadu_si128((const __m128i *)userp);
                t = _mm_loadu_si128((const __m128i *)treep);
                res_eq = _mm_cmpeq_epi8(u, t);
                if (_mm_movemask_epi8(res_eq) != 65535)
                    break;
            }
        len += remain;
    }
#elif defined(HAVE_ARM_NEON_INTRIN_H)
    /* Use vector instructions if we'll execute at least 1 of them. */
    if (len >= WT_VECTOR_SIZE) {
        size_t remain;
        uint8x16_t res_eq, u, t;
        remain = len % WT_VECTOR_SIZE;
        len -= remain;
        for (; len > 0; len -= WT_VECTOR_SIZE, userp += WT_VECTOR_SIZE, treep += WT_VECTOR_SIZE) {
            u = vld1q_u8(userp);
            t = vld1q_u8(treep);
            res_eq = vceqq_u8(u, t);
            if (vminvq_u8(res_eq) != 255)
                break;
        }
        len += remain;
    }
#endif
    /*
     * Use the non-vectorized version for the remaining bytes and for the small key sizes.
     */
    for (; len > 0; --len, ++userp, ++treep)
        if (*userp != *treep)
            return (*userp < *treep ? -1 : 1);

    /* Contents are equal up to the smallest length. */
    return ((usz == tsz) ? 0 : (usz < tsz) ? -1 : 1);
}

/*
 * __wt_compare --
 *     The same as __wt_lex_compare, but using the application's collator function when configured.
 */
static WT_INLINE int
__wt_compare(WT_SESSION_IMPL *session, WT_COLLATOR *collator, const WT_ITEM *user_item,
  const WT_ITEM *tree_item, int *cmpp)
{
    if (collator == NULL) {
        *cmpp = __wt_lex_compare(user_item, tree_item);
        return (0);
    }
    return (collator->compare(collator, &session->iface, user_item, tree_item, cmpp));
}

/*
 * __wt_compare_bounds --
 *     Return if the cursor key is within the bounded range. If upper is True, this indicates a next
 *     call and the key is checked against the upper bound. If upper is False, this indicates a prev
 *     call and the key is then checked against the lower bound.
 */
static WT_INLINE int
__wt_compare_bounds(WT_SESSION_IMPL *session, WT_CURSOR *cursor, WT_ITEM *key, uint64_t recno,
  bool upper, bool *key_out_of_bounds)
{
    uint64_t recno_bound;
    int cmpp;

    cmpp = 0;
    recno_bound = 0;

    WT_STAT_CONN_DATA_INCR(session, cursor_bounds_comparisons);

    if (upper) {
        WT_ASSERT(session, WT_DATA_IN_ITEM(&cursor->upper_bound));
        if (CUR2BT(cursor)->type == BTREE_ROW)
            WT_RET(
              __wt_compare(session, CUR2BT(cursor)->collator, key, &cursor->upper_bound, &cmpp));
        else
            /* Unpack the raw recno buffer into integer variable. */
            WT_RET(__wt_struct_unpack(
              session, cursor->upper_bound.data, cursor->upper_bound.size, "q", &recno_bound));

        if (F_ISSET(cursor, WT_CURSTD_BOUND_UPPER_INCLUSIVE))
            *key_out_of_bounds =
              CUR2BT(cursor)->type == BTREE_ROW ? (cmpp > 0) : (recno > recno_bound);
        else
            *key_out_of_bounds =
              CUR2BT(cursor)->type == BTREE_ROW ? (cmpp >= 0) : (recno >= recno_bound);
    } else {
        WT_ASSERT(session, WT_DATA_IN_ITEM(&cursor->lower_bound));
        if (CUR2BT(cursor)->type == BTREE_ROW)
            WT_RET(
              __wt_compare(session, CUR2BT(cursor)->collator, key, &cursor->lower_bound, &cmpp));
        else
            /* Unpack the raw recno buffer into integer variable. */
            WT_RET(__wt_struct_unpack(
              session, cursor->lower_bound.data, cursor->lower_bound.size, "q", &recno_bound));

        if (F_ISSET(cursor, WT_CURSTD_BOUND_LOWER_INCLUSIVE))
            *key_out_of_bounds =
              CUR2BT(cursor)->type == BTREE_ROW ? (cmpp < 0) : (recno < recno_bound);
        else
            *key_out_of_bounds =
              CUR2BT(cursor)->type == BTREE_ROW ? (cmpp <= 0) : (recno <= recno_bound);
    }
    return (0);
}

/*
 * __lex_compare_skip_ge_16 --
 *     Lexicographic comparison routine for data greater than or equal to 16 bytes, skipping leading
 *     bytes. Returns: < 0 if user_item is lexicographically < tree_item = 0 if user_item is
 *     lexicographically = tree_item > 0 if user_item is lexicographically > tree_item We use the
 *     names "user" and "tree" so it's clear in the btree code which the application is looking at
 *     when we call its comparison function.
 */
static WT_INLINE int
__lex_compare_skip_ge_16(
  const uint8_t *ustartp, const uint8_t *tstartp, size_t len, int lencmp, size_t *matchp)
{
    struct {
        uint64_t a, b;
    } udata, tdata;
    size_t match;
    uint64_t u64, t64;
    const uint8_t *uendp, *userp, *tendp, *treep;
    bool firsteq;

    match = *matchp;
    len = len - match;

    userp = ustartp + match;
    treep = tstartp + match;
    uendp = ustartp + len;
    tendp = tstartp + len;

    while (uendp - userp > WT_VECTOR_SIZE) {
        memcpy(&udata, userp, WT_VECTOR_SIZE);
        memcpy(&tdata, treep, WT_VECTOR_SIZE);
        if (udata.a != tdata.a || udata.b != tdata.b) {
            match = (size_t)(userp - ustartp);
            goto final128;
        }
        userp += WT_VECTOR_SIZE;
        treep += WT_VECTOR_SIZE;
    }

    match = (size_t)(uendp - ustartp) - WT_VECTOR_SIZE;
    memcpy(&udata, uendp - WT_VECTOR_SIZE, WT_VECTOR_SIZE);
    memcpy(&tdata, tendp - WT_VECTOR_SIZE, WT_VECTOR_SIZE);

final128:
    firsteq = udata.a == tdata.a;
    u64 = firsteq ? udata.b : udata.a;
    t64 = firsteq ? tdata.b : tdata.a;
    match += firsteq ? sizeof(uint64_t) : 0;

#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    u64 = __builtin_bswap64(u64);
    t64 = __builtin_bswap64(t64);
#endif

    match += (size_t)__builtin_clzll(u64 ^ t64) / 8;
    *matchp = match;

    return (u64 < t64 ? -1 : u64 > t64 ? 1 : lencmp);
}

/*
 * __lex_compare_lt_16 --
 *     Lexicographic comparison routine for data less than 16 bytes. Returns: < 0 if user_item is
 *     lexicographically < tree_item, = 0 if user_item is lexicographically = tree_item, > 0 if
 *     user_item is lexicographically > tree_item. We use the names "user" and "tree" so it's clear
 *     in the btree code which the application is looking at when we call its comparison function.
 */
static WT_INLINE int
__lex_compare_lt_16(const uint8_t *ustartp, const uint8_t *tstartp, size_t len, int lencmp)
{
    uint64_t ta, tb, ua, ub, u64, t64;
    const uint8_t *tendp, *uendp;

    uendp = ustartp + len;
    tendp = tstartp + len;
    if (len & sizeof(uint64_t)) {
        memcpy(&ua, ustartp, sizeof(uint64_t));
        memcpy(&ta, tstartp, sizeof(uint64_t));
        memcpy(&ub, uendp - sizeof(uint64_t), sizeof(uint64_t));
        memcpy(&tb, tendp - sizeof(uint64_t), sizeof(uint64_t));
    } else if (len & sizeof(uint32_t)) {
        uint32_t UA, TA, UB, TB;
        memcpy(&UA, ustartp, sizeof(uint32_t));
        memcpy(&TA, tstartp, sizeof(uint32_t));
        memcpy(&UB, uendp - sizeof(uint32_t), sizeof(uint32_t));
        memcpy(&TB, tendp - sizeof(uint32_t), sizeof(uint32_t));
        ua = UA;
        ta = TA;
        ub = UB;
        tb = TB;
    } else if (len & sizeof(uint16_t)) {
        uint16_t UA, TA, UB, TB;
        memcpy(&UA, ustartp, sizeof(uint16_t));
        memcpy(&TA, tstartp, sizeof(uint16_t));
        memcpy(&UB, uendp - sizeof(uint16_t), sizeof(uint16_t));
        memcpy(&TB, tendp - sizeof(uint16_t), sizeof(uint16_t));
        ua = UA;
        ta = TA;
        ub = UB;
        tb = TB;
    } else if (len & sizeof(uint8_t)) {
        uint8_t UA, TA;
        memcpy(&UA, ustartp, sizeof(uint8_t));
        memcpy(&TA, tstartp, sizeof(uint8_t));
        return (UA < TA ? -1 : UA > TA ? 1 : lencmp);
    } else
        return (lencmp);

    u64 = ua == ta ? ub : ua;
    t64 = ua == ta ? tb : ta;

#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    u64 = __builtin_bswap64(u64);
    t64 = __builtin_bswap64(t64);
#endif

    return (u64 < t64 ? -1 : u64 > t64 ? 1 : lencmp);
}

/*
 * __wt_lex_compare_skip --
 *     Lexicographic comparison routine, skipping leading bytes. Returns: < 0 if user_item is
 *     lexicographically < tree_item = 0 if user_item is lexicographically = tree_item > 0 if
 *     user_item is lexicographically > tree_item We use the names "user" and "tree" so it's clear
 *     in the btree code which the application is looking at when we call its comparison function.
 */
static WT_INLINE int
__wt_lex_compare_skip(
  WT_SESSION_IMPL *session, const WT_ITEM *user_item, const WT_ITEM *tree_item, size_t *matchp)
{
    size_t len, usz, tsz;
    int lencmp, ret_val;

    usz = user_item->size;
    tsz = tree_item->size;
    if (usz < tsz) {
        len = usz;
        lencmp = -1;
    } else if (usz > tsz) {
        len = tsz;
        lencmp = 1;
    } else {
        len = usz;
        lencmp = 0;
    }

    if (len >= WT_VECTOR_SIZE) {
        ret_val = __lex_compare_skip_ge_16(
          (const uint8_t *)user_item->data, (const uint8_t *)tree_item->data, len, lencmp, matchp);

#ifdef HAVE_DIAGNOSTIC
        /*
         * There are various optimizations in the code to skip comparing prefixes that are known to
         * be the same. If configured, check that the prefixes actually match.
         */
        if (FLD_ISSET(S2C(session)->timing_stress_flags, WT_TIMING_STRESS_PREFIX_COMPARE)) {
            int full_cmp_ret;
            full_cmp_ret = __wt_lex_compare(user_item, tree_item);
            WT_ASSERT_ALWAYS(session, full_cmp_ret == ret_val,
              "Comparison that skipped prefix returned different result than a full comparison");
        }
#else
        WT_UNUSED(session);
#endif
    } else
        ret_val = __lex_compare_lt_16(
          (const uint8_t *)user_item->data, (const uint8_t *)tree_item->data, len, lencmp);

    return (ret_val);
}

/*
 * __wt_compare_skip --
 *     The same as __wt_lex_compare_skip, but using the application's collator function when
 *     configured.
 */
static WT_INLINE int
__wt_compare_skip(WT_SESSION_IMPL *session, WT_COLLATOR *collator, const WT_ITEM *user_item,
  const WT_ITEM *tree_item, int *cmpp, size_t *matchp)
{
    if (collator == NULL) {
        *cmpp = __wt_lex_compare_skip(session, user_item, tree_item, matchp);
        return (0);
    }
    return (collator->compare(collator, &session->iface, user_item, tree_item, cmpp));
}

/*
 * __wt_lex_compare_short --
 *     Lexicographic comparison routine for short keys. Returns: < 0 if user_item is
 *     lexicographically < tree_item = 0 if user_item is lexicographically = tree_item > 0 if
 *     user_item is lexicographically > tree_item We use the names "user" and "tree" so it's clear
 *     in the btree code which the application is looking at when we call its comparison function.
 */
static WT_INLINE int
__wt_lex_compare_short(const WT_ITEM *user_item, const WT_ITEM *tree_item)
{
    size_t len, usz, tsz;
    const uint8_t *userp, *treep;

    usz = user_item->size;
    tsz = tree_item->size;
    len = WT_MIN(usz, tsz);

    userp = (const uint8_t *)user_item->data;
    treep = (const uint8_t *)tree_item->data;

/*
 * The maximum packed uint64_t is 9B, catch row-store objects using packed record numbers as keys.
 *
 * Don't use a #define to compress this case statement: gcc7 complains about implicit fallthrough
 * and doesn't support explicit fallthrough comments in macros.
 */
#define WT_COMPARE_SHORT_MAXLEN 9
    switch (len) {
    case 9:
        if (*userp != *treep)
            break;
        ++userp;
        ++treep;
    /* FALLTHROUGH */
    case 8:
        if (*userp != *treep)
            break;
        ++userp;
        ++treep;
    /* FALLTHROUGH */
    case 7:
        if (*userp != *treep)
            break;
        ++userp;
        ++treep;
    /* FALLTHROUGH */
    case 6:
        if (*userp != *treep)
            break;
        ++userp;
        ++treep;
    /* FALLTHROUGH */
    case 5:
        if (*userp != *treep)
            break;
        ++userp;
        ++treep;
    /* FALLTHROUGH */
    case 4:
        if (*userp != *treep)
            break;
        ++userp;
        ++treep;
    /* FALLTHROUGH */
    case 3:
        if (*userp != *treep)
            break;
        ++userp;
        ++treep;
    /* FALLTHROUGH */
    case 2:
        if (*userp != *treep)
            break;
        ++userp;
        ++treep;
    /* FALLTHROUGH */
    case 1:
        if (*userp != *treep)
            break;

        /* Contents are equal up to the smallest length. */
        return ((usz == tsz) ? 0 : (usz < tsz) ? -1 : 1);
    }
    return (*userp < *treep ? -1 : 1);
}
