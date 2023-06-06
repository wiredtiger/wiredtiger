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

    vector<int> input{{5, 6, 7, 4, 3, 8, 9, 2, 0, 1}};
    bool reverse;

    auto cmp = [](QSORT_R_ARGS(a, b, ctx)) -> int {
        if (*(bool*)ctx)
            return *static_cast<const int *>(b) - *static_cast<const int *>(a);
        return *static_cast<const int *>(a) - *static_cast<const int *>(b);
    };

    reverse = false;
    __wt_qsort_r(&input[0], input.size(), sizeof(input[0]), cmp, &reverse);
    CHECK(is_sorted(input.begin(), input.end()));

    reverse = true;
    __wt_qsort_r(&input[0], input.size(), sizeof(input[0]), cmp, &reverse);
    CHECK(is_sorted(input.rbegin(), input.rend()));    
}
