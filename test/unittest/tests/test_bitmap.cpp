/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>
#include <cstdlib>
#include <mutex>
#include <thread>
#include <time.h>
#include <vector>

#include "wiredtiger.h"
#include "wrappers/mock_session.h"
#include "wt_internal.h"

/* Mark the free chunk in the bitmap as in use. */
int
alloc_bitmap(WT_SESSION_IMPL *session, size_t &bit_index)
{
    WT_CHUNKCACHE *chunkcache = &S2C(session)->chunkcache;
    uint8_t *map_byte_p, map_byte_expected, map_byte_mask;

retry:
    /* Use the bitmap to find a free slot for a chunk in the cache. */
    WT_RET(__ut_chunkcache_bitmap_find_free(session, &bit_index));

    /* Mark the free chunk in the bitmap as in use. */
    map_byte_p = &chunkcache->free_bitmap[bit_index / 8];
    map_byte_expected = *map_byte_p;
    map_byte_mask = (uint8_t)(0x01 << (bit_index % 8));
    if (((*map_byte_p & map_byte_mask) == 0) &&
      !__wt_atomic_cas8(map_byte_p, map_byte_expected, map_byte_expected | map_byte_mask))
        goto retry;

    return 0;
}

/* Mark the used chunk in the bitmap as free.  */
void
free_bitmap(WT_CHUNKCACHE *chunkcache, size_t bit_index)
{
    uint8_t *map_byte_p, map_byte_expected;
    do {
        map_byte_p = &chunkcache->free_bitmap[bit_index / 8];
        map_byte_expected = *map_byte_p;
    } while (!__wt_atomic_cas8(
      map_byte_p, map_byte_expected, map_byte_expected & (uint8_t) ~(0x01 << (bit_index % 8))));
}

TEST_CASE("Chunkcache bitmap: __chunkcache_bitmap_find_free", "[bitmap]")
{
    /* Build Mock session, this will automatically create a mock connection. */
    std::shared_ptr<MockSession> session = MockSession::buildTestMockSession();

    WT_SESSION_IMPL *session_impl = session->getWtSessionImpl();
    WT_CHUNKCACHE *chunkcache;
    uint64_t capacity = 101 + rand() % 10000;
    size_t chunk_size = 1 + rand() % 100;
    size_t num_chunks = capacity / chunk_size;

    /* Setup chunk cache. */
    REQUIRE((session->getMockConnection()->setupChunkCache(
              session_impl, capacity, chunk_size, chunkcache)) == 0);

    SECTION("Sequential allocation and free")
    {
        size_t bit_index;

        /* Allocate all the bits in the bitmap sequentially. */
        for (int i = 0; i < num_chunks; i++) {
            REQUIRE(alloc_bitmap(session_impl, bit_index) == 0);
            REQUIRE(i == bit_index);
        }

        REQUIRE(alloc_bitmap(session_impl, bit_index) == ENOSPC);

        /* Free all the bits in the bitmap sequentially. */
        for (int i = 0; i < num_chunks; i++) {
            free_bitmap(chunkcache, i);
        }

        /* Reallocate all the bits to ensure all the frees were successful. */
        for (int i = 0; i < num_chunks; i++) {
            REQUIRE(alloc_bitmap(session_impl, bit_index) == 0);
            REQUIRE(i == bit_index);
        }
    }

    SECTION("Random allocation and free")
    {
        size_t bit_index, random_num_chunks;

        /* Initialize random seed: */
        srand(time(NULL));

        /* Allocate bits to the bitmap */
        for (int i = 0; i < num_chunks; i++) {
            REQUIRE(alloc_bitmap(session_impl, bit_index) == 0);
            REQUIRE(i == bit_index);
        }

        REQUIRE(alloc_bitmap(session_impl, bit_index) == ENOSPC);

        /* Generate random number of chunks within range. */
        random_num_chunks = rand() % num_chunks;

        /* Free the bits in the bitmap randomly within range of allocated bits. */
        for (int cycle = 0; cycle < 20; cycle++) {
            for (int i = 0; i < random_num_chunks; i++) {
                int random_number = rand() % random_num_chunks;
                free_bitmap(chunkcache, random_number);
                REQUIRE(alloc_bitmap(session_impl, bit_index) == 0);
                REQUIRE(random_number == bit_index);
            }
        }
    }

    SECTION("Concurrent allocations")
    {
        const int iterations = num_chunks;
        const int threads_num = 5;

        std::vector<std::thread> threads;
        uint64_t allocations_made = 0;

        for (int i = 0; i < threads_num; i++) {
            /* Concurrent allocation */
            threads.emplace_back([session_impl, iterations, &allocations_made]() {
                size_t bit_index;
                for (int j = 0; j < iterations; j++) {
                    if (alloc_bitmap(session_impl, bit_index) == 0)
                        __wt_atomic_add64(&allocations_made, 1);
                }
            });
        }

        /* Wait for all threads to finish */
        for (auto &thread : threads) {
            thread.join();
        }

        size_t bit_index;
        REQUIRE(allocations_made == num_chunks);
        REQUIRE(alloc_bitmap(session_impl, bit_index) == ENOSPC);
    }

    SECTION("Concurrent allocations and free")
    {
        const int iterations = num_chunks;
        const int threads_num = 500;

        std::vector<std::thread> threads;
        uint64_t allocations_made = 0;
        std::mutex mtx;

        for (int i = 0; i < threads_num; i++) {
            /* Concurrent allocation */
            threads.emplace_back([session_impl, iterations, &allocations_made]() {
                size_t bit_index;
                for (int j = 0; j < iterations; j++) {
                    if (alloc_bitmap(session_impl, bit_index) == 0)
                        __wt_atomic_add64(&allocations_made, 1);
                }
            });

            /* Concurrent free */
            threads.emplace_back([chunkcache, iterations, &allocations_made, &mtx]() {
                for (int j = 0; j < iterations; j++) {
                    int k = rand() %
                      (WT_CHUNKCACHE_BITMAP_SIZE(chunkcache->capacity, chunkcache->chunk_size) - 1);
                    int l = rand() % 8;
                    if (!mtx.try_lock())
                        continue;
                    {
                        std::lock_guard<std::mutex> lock(mtx, std::adopt_lock);
                        if (((uint8_t)chunkcache->free_bitmap[k] & (uint8_t)(0x01 << l)) != 0) {
                            free_bitmap(chunkcache, (k * 8) + l);
                            __wt_atomic_sub64(&allocations_made, 1);
                        }
                    }
                }
            });
        }

        /* Wait for all threads to finish */
        for (auto &thread : threads) {
            thread.join();
        }

        size_t bit_index;
        for (int i = 0; i < num_chunks - allocations_made; i++) {
            REQUIRE(alloc_bitmap(session_impl, bit_index) == 0);
        }

        REQUIRE(alloc_bitmap(session_impl, bit_index) == ENOSPC);
    }
}
