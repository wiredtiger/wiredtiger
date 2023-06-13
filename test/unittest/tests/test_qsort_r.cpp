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

using namespace std;
namespace {

class random_generator {
    random_device rnd_device;
    mt19937 mersenne_engine;
    uniform_int_distribution<int> dist;

public:
    random_generator() = default;
    random_generator(const random_generator &) = delete;
    random_generator &operator=(const random_generator &) = delete;
    ~random_generator() = default;

    std::vector<int>
    make_vector(size_t size)
    {
        std::vector<int> rv(size);
        auto rnd = [this]() { return this->dist(this->mersenne_engine); };
        generate(begin(rv), end(rv), rnd);
        return rv;
    }
};

template <typename T>
const T
vp_as(const void *vp)
{
    return *static_cast<const T *>(vp);
}

int transposable_cmp(QSORT_R_ARGS(a, b, ctx))
{
    if (vp_as<bool>(ctx))
        return vp_as<int>(b) - vp_as<int>(a);
    return vp_as<int>(a) - vp_as<int>(b);
};

int counting_cmp(QSORT_R_ARGS(a, b, ctx))
{
    auto count = static_cast<int *>(ctx);
    (*count)++;
    return vp_as<int>(a) - vp_as<int>(b);
};

} // namespace

TEST_CASE("Safe to invoke on an empty array.", "[qsort_r]")
{
    vector<int> input;
    __wt_qsort_r(&input[0], input.size(), sizeof(input[0]), transposable_cmp, nullptr);
}

TEST_CASE("Test context argument for comparator", "[qsort_r]")
{
    random_generator rand_gen;
    vector<int> input{rand_gen.make_vector(100)};
    bool reverse;

    reverse = false;
    __wt_qsort_r(&input[0], input.size(), sizeof(input[0]), transposable_cmp, &reverse);
    CHECK(is_sorted(input.begin(), input.end()));

    reverse = true;
    __wt_qsort_r(&input[0], input.size(), sizeof(input[0]), transposable_cmp, &reverse);
    CHECK(is_sorted(input.rbegin(), input.rend()));
}

TEST_CASE("Test context is mutable", "[qsort_r]")
{
    vector<int> input{{1, 2, 3, 4, 5}};
    int count;

    count = 0;
    __wt_qsort_r(&input[0], input.size(), sizeof(input[0]), counting_cmp, &count);
    CHECK(count > 0);
}
