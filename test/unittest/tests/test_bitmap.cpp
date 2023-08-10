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

TEST_CASE("Chunkcache bitmap: __chunkcache_bitmap_find_free", "[bitmap]")
{
    // Build Mock session, this will automatically create a mock connection.
    std::shared_ptr<MockSession> session = MockSession::buildTestMockSession();

    WT_SESSION_IMPL *session_impl = session->getWtSessionImpl();
    uint64_t capacity = 1000;
    size_t chunk_size = 10;

    // Setup chunk cache.
    REQUIRE(
      (session->getMockConnection()->setupChunkCache(session_impl, capacity, chunk_size)) == 0);

    SECTION("Free index found in bitmap")
    {
        size_t bit_index;
        REQUIRE(__ut_chunkcache_bitmap_find_free(session_impl, &bit_index) == 0);
        // REQUIRE(bit_index == 0);

        // __chunkcache_bitmap_find_free(session, &bit_index);
        // REQUIRE(bit_index == 0);

        // map_byte = &chunkcache->free_bitmap[bit_index / 8];
        // __wt_atomic_cas8(map_byte, *map_byte, *map_byte | (uint8_t)(0x01 << (bit_index % 8)))

        // __chunkcache_bitmap_find_free(session, &bit_index);
        // REQUIRE(bit_index == 1);
    }

    // Edge case of full bitmap - getting correct error
    // Test remainder bitmap is working
    // Test removing / freeing of the bitmap is working
    // Test removing then finding is finding the one I just free'd
    // Random generator for free, then add them back to the bitmap
}
