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
#include "wrappers/chunkcache_wrapper.h"

TEST_CASE("Chunkcache bitmap: __chunkcache_bitmap_find_free", "[bitmap]")
{
    chunkcache_wrapper chunkcache(1000, 5); // Wrapper name 
    size_t bit_index;
    
    // Use mock session / connection - create a chunkcache object through a wrapper here and set it up and connect 
    // it to the mock session and pass that in
    std::shared_ptr<MockSession> session = MockSession::buildTestMockSession();
    S2C(session->getWtSessionImpl())->chunkcache = *chunkcache.chunkcache_get();
    bit_index = 0;

    // Do I need to calloc the space for bitmap? I can perhaps then hardcode the chunkcache->capcity etc..
    // __wt_calloc(session, 1, ((chunkcache->capacity / chunkcache->chunk_size) / 8),
    //       &chunkcache->free_bitmap);
    
    SECTION("Free index found in bitmap")
    {
        // __chunkcache_bitmap_find_free(session, &bit_index);
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
