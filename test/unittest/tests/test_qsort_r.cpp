/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <cstdlib>
#include <vector>
#include <mutex>
#include <thread>

#include <catch2/catch.hpp>
#include "wt_internal.h"

TEST_CASE("Test QSORT_R: sorts", "[QSORT_R]")
{
    using namespace std;

    vector<int> input{{ 5, 6, 7, 4, 3, 8, 9, 2, 0, 1 }};
    int cmp_count{ 0 };

    auto cmp = [](const void *a, const void *b, void *ctx) -> int {
        *static_cast<int*>(ctx) += 1;
        return *static_cast<const int*>(a) - *static_cast<const int*>(b);
    };
    qsort_r(&input[0], input.size(), sizeof(input[0]), cmp, &cmp_count);
    CHECK(is_sorted(input.begin(), input.end()));
    CHECK(cmp_count > 0);
}
