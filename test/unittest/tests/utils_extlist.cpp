/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/* Choose one */
#define DEBUG /* Print debugging output */
//#undef DEBUG

#include <cstdio>
#include <string>
#include <stdexcept>

#include <catch2/catch.hpp>

#include "utils.h"
#include "utils_extlist.h"

bool
operator<(const utils::off_size &left, const utils::off_size &right)
{
    return ((left._off < right._off) || ((left._off == right._off) && (left._size < right._size)));
}

namespace utils {
/*
 * ext_print_list --
 *     Print a skip list of WT_EXT *.
 */
void
ext_print_list(WT_EXT **head)
{
    WT_EXT *extp;
    int i;

    if (head == nullptr)
        return;

    for (i = 0; i < WT_SKIP_MAXDEPTH; i++) {
        printf("L%d: ", i);

        extp = head[i];
        while (extp != nullptr) {
            printf("%p {off %" PRId64 ", size %" PRId64 ", end %" PRId64 "} -> ", extp, extp->off,
              extp->size, (extp->off + extp->size - 1));
            extp = extp->next[i];
        }

        printf("X\n");
    }
}

/*
 * extlist_print_off --
 *     Print an WT_EXTLIST and it's off skip list.
 */
void
extlist_print_off(WT_EXTLIST &extlist)
{
    printf("{name %s, bytes %" PRIu64 ", entries %" PRIu32 ", objectid %" PRIu32 ", offset %" PRId64
           ", checksum 0x%" PRIu32 ", size %" PRIu32 ", track_size %s, last %p",
      extlist.name, extlist.bytes, extlist.entries, extlist.objectid, extlist.offset,
      extlist.checksum, extlist.size, extlist.track_size ? "true" : "false", extlist.last);
    if (extlist.last != nullptr)
        printf(" {off %" PRId64 ", size %" PRId64 ", depth %" PRIu8 ", next %p}", extlist.last->off,
          extlist.last->size, extlist.last->depth, extlist.last->next);
    putchar('\n');
    printf("off:\n");
    ext_print_list(extlist.off);
}

/*!
 * alloc_new_ext --
 *     Allocate and initialize a WT_EXT structure for tests. Require that the allocation succeeds. A
 *     convenience wrapper for __wti_block_ext_alloc().
 *
 * @param off The offset.
 * @param size The size.
 */
WT_EXT *
alloc_new_ext(WT_SESSION_IMPL *session, wt_off_t off, wt_off_t size)
{
    WT_EXT *ext;
    REQUIRE(__wti_block_ext_alloc(session, &ext) == 0);
    ext->off = off;
    ext->size = size;

#ifdef DEBUG
    printf("Allocated WT_EXT %p {off %" PRId64 ", size %" PRId64 ", end %" PRId64 ", depth %" PRIu8
           ", next[0] %p}\n",
      ext, ext->off, ext->size, (ext->off + ext->size - 1), ext->depth, ext->next[0]);
    fflush(stdout);
#endif

    return ext;
}

/*!
 * alloc_new_ext --
 *     Allocate and initialize a WT_EXT structure for tests. Require that the allocation succeeds. A
 *     convenience wrapper for __wti_block_ext_alloc().
 *
 * @param off_size The offset and the size.
 */
WT_EXT *
alloc_new_ext(WT_SESSION_IMPL *session, const off_size &one)
{
    return alloc_new_ext(session, one._off, one._size);
}

/*
 * get_off_n --
 *     Get the nth level [0] member of a WT_EXTLIST's offset skiplist. Use case: Scan the offset
 *     skiplist to verify the contents.
 */
WT_EXT *
get_off_n(const WT_EXTLIST &extlist, uint32_t idx)
{
    REQUIRE(idx < extlist.entries);
    if ((extlist.last != nullptr) && (idx == (extlist.entries - 1))) {
        return extlist.last;
    }
    return extlist.off[idx];
}

/*!
 * ext_free_list --
 *    Free a skip list of WT_EXT * for tests.
 *    Return whether WT_EXTLIST.last was found and freed.
 *
 * @param session the session
 * @param head the skip list
 * @param last WT_EXTLIST.last
 */
bool
ext_free_list(WT_SESSION_IMPL *session, WT_EXT **head, WT_EXT *last)
{
    WT_EXT *extp;
    bool last_found = false;
    WT_EXT *next_extp;

    if (head == nullptr)
        return false;

    /* Free just the top level. Lower levels are duplicates. */
    extp = head[0];
    while (extp != nullptr) {
        if (extp == last)
            last_found = true;
        next_extp = extp->next[0];
        extp->next[0] = nullptr;
        __wti_block_ext_free(session, extp);
        extp = next_extp;
    }
    return last_found;
}

/*!
 * size_free_list --
 *    Free a skip list of WT_SIZE * for tests.
 *    Return whether WT_EXTLIST.last was found and freed.
 *
 * @param session the session
 * @param head the skip list
 */
void
size_free_list(WT_SESSION_IMPL *session, WT_SIZE **head)
{
    WT_SIZE *sizep;
    WT_SIZE *next_sizep;

    if (head == nullptr)
        return;

    /* Free just the top level. Lower levels are duplicates. */
    sizep = head[0];
    while (sizep != nullptr) {
        next_sizep = sizep->next[0];
        sizep->next[0] = nullptr;
        __wti_block_size_free(session, sizep);
        sizep = next_sizep;
    }
}

/*!
 * extlist_free --
 *    Free the a skip lists of WT_EXTLIST * for tests.
 *
 * @param session the session
 * @param extlist the extent list
 */
void
extlist_free(WT_SESSION_IMPL *session, WT_EXTLIST &extlist)
{
    if (!ext_free_list(session, extlist.off, extlist.last) && extlist.last != nullptr) {
        __wti_block_ext_free(session, extlist.last);
        extlist.last = nullptr;
    }
    size_free_list(session, extlist.sz);
}

/*!
 * verify_empty_extent_list --
 *     Verify an extent list is empty. This was derived from the tests for __block_off_srch_last.
 *
 * @param head the extlist
 * @param stack the stack for appending returned by __block_off_srch_last
 */
void
verify_empty_extent_list(WT_EXT **head, WT_EXT ***stack)
{
    REQUIRE(__ut_block_off_srch_last(&head[0], &stack[0]) == nullptr);
    for (int i = 0; i < WT_SKIP_MAXDEPTH; i++) {
        REQUIRE(stack[i] == &head[i]);
    }
}

/*!
 * verify_off_extent_list --
 *     Verify the offset skiplist of a WT_EXTLIST is as expected. Also optionally verify the entries
 *     and bytes of a WT_EXTLIST are as expected.
 *
 * @param extlist the extent list to verify
 * @param expected_order the expected offset skip list
 * @param verify_entries_bytes whether to verify entries and bytes
 */
void
verify_off_extent_list(
  const WT_EXTLIST &extlist, const std::vector<off_size> &expected_order, bool verify_entries_bytes)
{
    REQUIRE(extlist.entries == expected_order.size());
    uint32_t idx = 0;
    uint64_t expected_bytes = 0;
    for (const off_size &expected : expected_order) {
        WT_EXT *ext = get_off_n(extlist, idx);
#ifdef DEBUG
        printf("Verify: %" PRIu32 ". Expected: {off %" PRId64 ", size %" PRId64 ", end %" PRId64
               "}; Actual: %p {off %" PRId64 ", size %" PRId64 ", end %" PRId64 "}\n",
          idx, expected._off, expected._size, (expected._off + expected._size - 1), ext, ext->off,
          ext->size, (ext->off + ext->size - 1));
        fflush(stdout);
#endif
        REQUIRE(ext->off == expected._off);
        REQUIRE(ext->size == expected._size);
        ++idx;
        expected_bytes += ext->size;
    }
    if (!verify_entries_bytes)
        return;
    REQUIRE(extlist.entries == idx);
    REQUIRE(extlist.bytes == expected_bytes);
}
} // namespace utils
