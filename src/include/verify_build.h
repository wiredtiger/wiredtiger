/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <assert.h>

/*
 * NOTE: If you see a compile failure in this file, your compiler is laying out structs in memory in
 * a way WiredTiger does not expect. Please refer to the build instructions in the documentation
 * (docs/html/install.html) for more information.
 */

/*
 * Compile time assertions.
 *
 * If the argument to WT_STATIC_ASSERT is zero, the macro evaluates to:
 *
 *	(void)sizeof(char[-1])
 *
 * which fails to compile (which is what we want, the assertion failed).
 * If the value of the argument to WT_STATIC_ASSERT is non-zero, then the
 * macro evaluates to:
 *
 *	(void)sizeof(char[1]);
 *
 * which compiles with no warnings, and produces no code.
 *
 * For more details about why this works, see
 * http://scaryreasoner.wordpress.com/2009/02/28/
 */
#define WT_STATIC_ASSERT(cond) (void)sizeof(char[1 - 2 * !(cond)])

/*
 * __wt_verify_build --
 *     This function is never called: it exists so there is a place for code that checks build-time
 *     conditions.
 */
static inline void
__wt_verify_build(void)
{
    /* Check specific structures weren't padded. */
    static_assert(sizeof(WT_BLOCK_DESC) == WT_BLOCK_DESC_SIZE,
      "size of WT_BLOCK_DESC did not match expected size WT_BLOCK_DESC_SIZE");
    static_assert(
      sizeof(WT_REF) == WT_REF_SIZE, "size of WT_REF did not match expected size WT_REF_SIZE");

    /*
     * WT_UPDATE is special: we arrange fields to avoid padding within the structure but it could be
     * padded at the end depending on the timestamp size. Further check that the data field in the
     * update structure is where we expect it.
     */
    static_assert(
      sizeof(WT_UPDATE) == WT_ALIGN(WT_UPDATE_SIZE, 8), "size of WT_UPDATE is not a multiple of 8");
    static_assert(offsetof(WT_UPDATE, data) == WT_UPDATE_SIZE,
      "variable length array 'data' is not the last field in WT_UPDATE");

    /*
     * WT_UPDATE: Validate expected sum of field sizes compared to compiler determined structure
     * size. If the fields WT_UPDATE these assertions should be revised to match the trailing
     * padding of the updated structure.
     */
    static_assert(WT_UPDATE_SIZE_NOVALUE == sizeof(WT_UPDATE));
    static_assert((WT_UPDATE_SIZE_NOVALUE - WT_UPDATE_SIZE) == 1);

/* Check specific structures were padded. */
#define WT_PADDING_CHECK(s) \
    static_assert(sizeof(s) > WT_CACHE_LINE_ALIGNMENT || sizeof(s) % WT_CACHE_LINE_ALIGNMENT == 0)
    WT_PADDING_CHECK(WT_LOGSLOT);
    WT_PADDING_CHECK(WT_TXN_SHARED);

    /*
     * The btree code encodes key/value pairs in size_t's, and requires at least 8B size_t's.
     */
    static_assert(sizeof(size_t) >= 8, "size_t is smaller than 8 bytes");

    /*
     * We require a wt_off_t fit into an 8B chunk because 8B is the largest integral value we can
     * encode into an address cookie.
     *
     * WiredTiger has never been tested on a system with 4B file offsets, disallow them for now.
     */
    static_assert(
      sizeof(wt_off_t) == 8, "WiredTiger is only supported on systems with an 8 byte file offset");

    /*
     * We require a time_t be an integral type and fit into a uint64_t for simplicity.
     */
    static_assert(sizeof(time_t) <= sizeof(uint64_t), "time_t must fit within a uint64_t");
}
