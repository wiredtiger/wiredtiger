/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>
#include <cstdlib>
#include <thread>
#include <time.h>
#include <vector>

#include "wiredtiger.h"
#include "wrappers/mock_session.h"
#include "wt_internal.h"

// Mark the free chunk in the bitmap as in use.
int
alloc_bitmap(WT_SESSION_IMPL *session, size_t &bit_index)
{
    WT_CHUNKCACHE *chunkcache = &S2C(session)->chunkcache;
    uint8_t *map_byte;

retry:
    /* Use the bitmap to find a free slot for a chunk in the cache. */
    WT_RET(__ut_chunkcache_bitmap_find_free(session, &bit_index));

    /* Mark the free chunk in the bitmap as in use. */
    map_byte = &chunkcache->free_bitmap[bit_index / 8];
    if (!__wt_atomic_cas8(map_byte, *map_byte, *map_byte | (uint8_t)(0x01 << (bit_index % 8))))
        goto retry;

    return 0;
}

// Mark the used chunk in the bitmap as free.
void
free_bitmap(WT_CHUNKCACHE *chunkcache, size_t bit_index)
{
    uint8_t *map_byte;
    do {
        map_byte = &chunkcache->free_bitmap[bit_index / 8];
    } while (
      !__wt_atomic_cas8(map_byte, *map_byte, *map_byte & (uint8_t) ~(0x01 << (bit_index % 8))));
}

TEST_CASE("Chunkcache bitmap: __chunkcache_bitmap_find_free", "[bitmap]")
{
    // Build Mock session, this will automatically create a mock connection.
    std::shared_ptr<MockSession> session = MockSession::buildTestMockSession();

    WT_SESSION_IMPL *session_impl = session->getWtSessionImpl();
    WT_CHUNKCACHE *chunkcache;
    uint64_t capacity = 100;
    size_t chunk_size = 10;
    // size_t max_num_chunks = capacity/chunk_size;

    // Setup chunk cache.
    REQUIRE((session->getMockConnection()->setupChunkCache(
              session_impl, capacity, chunk_size, chunkcache)) == 0);

    SECTION("Sequential allocation and free")
    {
        size_t bit_index;

        // We have 10 chunks so there are 10 bits.
        for (int i = 0; i < 10; i++) {
            REQUIRE(alloc_bitmap(session_impl, bit_index) == 0);
            REQUIRE(i == bit_index);
        }

        REQUIRE(alloc_bitmap(session_impl, bit_index) == ENOSPC);

        for (int i = 0; i < 10; i++) {
            free_bitmap(chunkcache, i);
        }

        for (int i = 0; i < 10; i++) {
            REQUIRE(alloc_bitmap(session_impl, bit_index) == 0);
            REQUIRE(i == bit_index);
        }
    }

    SECTION("Random allocation and free")
    {
        size_t bit_index;

        /* initialize random seed: */
        srand(time(NULL));

        // We have 10 chunks so there are 10 bits.
        for (int i = 0; i < 10; i++) {
            REQUIRE(alloc_bitmap(session_impl, bit_index) == 0);
            REQUIRE(i == bit_index);
        }

        REQUIRE(alloc_bitmap(session_impl, bit_index) == ENOSPC);

        for (int i = 0; i < 5; i++) {
            int random_number = rand() % 10;
            free_bitmap(chunkcache, random_number);
            REQUIRE(alloc_bitmap(session_impl, bit_index) == 0);
            REQUIRE(random_number == bit_index);
        }
    }

    SECTION("Concurrent allocation and free")
    {
        const int iterations = 10000;

        std::vector<std::thread> threads;
        size_t bit_index;

        for (int i = 0; i < 50; i++) {
            // Concurrent allocation
            threads.emplace_back([session_impl, iterations]() {
                size_t bit_index;
                for (int j = 0; j < iterations; j++) {
                    alloc_bitmap(session_impl, bit_index);
                }
            });
            // Concurrent freeing
            threads.emplace_back([chunkcache, iterations]() {
                for (int j = 0; j < iterations; j++) {
                    int random_number = rand() % 10;
                    free_bitmap(chunkcache, random_number);
                }
            });
        }

        // Wait for all threads to finish
        for (auto &thread : threads) {
            thread.join();
        }

        // Verify that the cache is still consistent
        for (int i = 0; i < 10; i++) {
            REQUIRE(alloc_bitmap(session_impl, bit_index) == 0);
        }

        REQUIRE(alloc_bitmap(session_impl, bit_index) == ENOSPC);
    }
}
