/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>

#include "wt_internal.h"
#include "btree_cmp_inline.h"

#include <vector>
#include <cstdlib>
#include <cstring>

// Helper function to generate WT_ITEM pairs with identical prefix and randomized suffix
static void
generate_wt_item_pair(WT_ITEM *item1, WT_ITEM *item2, size_t identical_len,
    std::vector<uint8_t> &buf1, std::vector<uint8_t> &buf2)
{
    // Calculate total length: identical part + 20% randomized
    size_t random_len = (identical_len * 20) / 100;
    if (random_len == 0 && identical_len > 0)
        random_len = 1;  // Ensure at least 1 byte of random data
    size_t total_len = identical_len + random_len;

    buf1.resize(total_len);
    buf2.resize(total_len);

    // Fill identical prefix
    for (size_t i = 0; i < identical_len; ++i) {
        uint8_t val = static_cast<uint8_t>(rand() % 256);
        buf1[i] = val;
        buf2[i] = val;
    }

    // Fill randomized suffix (different for each item)
    for (size_t i = identical_len; i < total_len; ++i) {
        buf1[i] = static_cast<uint8_t>(rand() % 256);
        buf2[i] = static_cast<uint8_t>(rand() % 256);
    }

    item1->data = buf1.data();
    item1->size = total_len;
    item2->data = buf2.data();
    item2->size = total_len;
}

TEST_CASE("Btree compare: __wt_lex_compare", "[btree][!hide]")
{
    WT_ITEM item1, item2;
    std::vector<uint8_t> buf1, buf2;

    SECTION("5 bytes")
    {
        generate_wt_item_pair(&item1, &item2, 5, buf1, buf2);

        BENCHMARK("__wt_lex_compare")
        {
            return __wt_lex_compare(&item1, &item2);
        };

        BENCHMARK("__wt_lex_compare_short")
        {
            return __wt_lex_compare_short(&item1, &item2);
        };

        BENCHMARK("__wt_mem_compare")
        {
            return __wt_mem_compare(&item1, &item2);
        };
    }

    SECTION("10 bytes")
    {
        generate_wt_item_pair(&item1, &item2, 10, buf1, buf2);

        BENCHMARK("__wt_lex_compare")
        {
            return __wt_lex_compare(&item1, &item2);
        };

        BENCHMARK("__wt_mem_compare")
        {
            return __wt_mem_compare(&item1, &item2);
        };
    }

    SECTION("100 bytes")
    {
        generate_wt_item_pair(&item1, &item2, 100, buf1, buf2);

        BENCHMARK("__wt_lex_compare")
        {
            return __wt_lex_compare(&item1, &item2);
        };

        BENCHMARK("__wt_mem_compare")
        {
            return __wt_mem_compare(&item1, &item2);
        };
    }

    SECTION("500 bytes")
    {
        generate_wt_item_pair(&item1, &item2, 500, buf1, buf2);

        BENCHMARK("__wt_lex_compare")
        {
            return __wt_lex_compare(&item1, &item2);
        };

        BENCHMARK("__wt_mem_compare")
        {
            return __wt_mem_compare(&item1, &item2);
        };
    }

    SECTION("1024 bytes")
    {
        generate_wt_item_pair(&item1, &item2, 1024, buf1, buf2);

        BENCHMARK("__wt_lex_compare")
        {
            return __wt_lex_compare(&item1, &item2);
        };

        BENCHMARK("__wt_mem_compare")
        {
            return __wt_mem_compare(&item1, &item2);
        };
    }

    SECTION("10240 bytes")
    {
        generate_wt_item_pair(&item1, &item2, 1024 * 10, buf1, buf2);

        BENCHMARK("__wt_lex_compare")
        {
            return __wt_lex_compare(&item1, &item2);
        };

        BENCHMARK("__wt_mem_compare")
        {
            return __wt_mem_compare(&item1, &item2);
        };
    }
}
