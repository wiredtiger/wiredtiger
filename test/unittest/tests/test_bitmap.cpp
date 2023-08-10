/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>
#include <cstdlib>
#include "wt_internal.h"
#include "wiredtiger.h"
#include "wrappers/mock_session.h"
#include <time.h>

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

    SECTION("Sequential allocation and free")
    {
        size_t bit_index;

        // We have 10 chunks so there are 10 bits.
        for (int i = 0; i < 10; i++) {
            REQUIRE(__ut_chunkcache_bitmap_find_free(session_impl, &bit_index) == 0);
            alloc_bitmap(chunkcache, bit_index);
            REQUIRE(i == bit_index);
        }

        REQUIRE(__ut_chunkcache_bitmap_find_free(session_impl, &bit_index) == ENOSPC);

        for (int i = 0; i < 10; i++) {
            free_bitmap(chunkcache, i);
        }

        for (int i = 0; i < 10; i++) {
            REQUIRE(__ut_chunkcache_bitmap_find_free(session_impl, &bit_index) == 0);
            alloc_bitmap(chunkcache, bit_index);
            REQUIRE(i == bit_index);
        }
    }

    SECTION("Random allocation and free")
    {
        size_t bit_index;

        /* initialize random seed: */
        srand (time(NULL));

        // We have 10 chunks so there are 10 bits.
        for (int i = 0; i < 10; i++) {
            REQUIRE(__ut_chunkcache_bitmap_find_free(session_impl, &bit_index) == 0);
            REQUIRE(i == bit_index);
            alloc_bitmap(chunkcache, bit_index);
        }

        REQUIRE(__ut_chunkcache_bitmap_find_free(session_impl, &bit_index) == ENOSPC);

        for (int i = 0; i < 5; i++) {
            int random_number = rand() % 10;
            free_bitmap(chunkcache, random_number);
            REQUIRE(__ut_chunkcache_bitmap_find_free(session_impl, &bit_index) == 0);
            REQUIRE(random_number == bit_index);
            alloc_bitmap(chunkcache, bit_index);
        }
    }
}
