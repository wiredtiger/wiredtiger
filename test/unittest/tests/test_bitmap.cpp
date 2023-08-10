/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>
#include "wt_internal.h"
#include "wiredtiger.h"
#include "wrappers/mock_session.h"

// Mark the free chunk in the bitmap as in use.
void
alloc_bitmap(WT_CHUNKCACHE* chunkcache, size_t bit_index)
{
    uint8_t *map_byte = &chunkcache->free_bitmap[bit_index / 8];
    *map_byte = *map_byte | (uint8_t)(0x01 << (bit_index % 8));
}

// Mark the used chunk in the bitmap as free.
void
free_bitmap(WT_CHUNKCACHE* chunkcache, size_t bit_index)
{
    uint8_t *map_byte = &chunkcache->free_bitmap[bit_index / 8];
    *map_byte = *map_byte & ~(0x01 << (bit_index % 8));
}

TEST_CASE("Chunkcache bitmap: __chunkcache_bitmap_find_free", "[bitmap]")
{
    // Build Mock session, this will automatically create a mock connection.
    std::shared_ptr<MockSession> session = MockSession::buildTestMockSession();

    WT_SESSION_IMPL *session_impl = session->getWtSessionImpl();
    uint64_t capacity = 100;
    size_t chunk_size = 10;
    WT_CHUNKCACHE *chunkcache;

    // Setup chunk cache.
    REQUIRE(
      (session->getMockConnection()->setupChunkCache(session_impl, capacity, chunk_size, chunkcache)) == 0);

    SECTION("Free index found in bitmap")
    {
        size_t bit_index;

        // We have 10 chunks so there are 10 bits.
        for (int i = 0; i < 10; i++)
            alloc_bitmap(chunkcache, i);
        REQUIRE(__ut_chunkcache_bitmap_find_free(session_impl, &bit_index) == ENOSPC);

        free_bitmap(chunkcache, 3);
        REQUIRE(__ut_chunkcache_bitmap_find_free(session_impl, &bit_index) == 0);
        REQUIRE(bit_index == 3);

    }

    // Edge case of full bitmap - getting correct error
    // Test remainder bitmap is working
    // Test removing / freeing of the bitmap is working
    // Test removing then finding is finding the one I just free'd
    // Random generator for free, then add them back to the bitmap
}
