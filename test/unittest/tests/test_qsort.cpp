/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <algorithm>
#include <vector>

#include <catch2/catch.hpp>

#include "wt_internal.h"

namespace {

const uint8_t COMPOUND_MAGIC_B = 123;
const uint64_t COMPOUND_MAGIC_C = 0xdeadbeefBAADF00D;
struct compound_test_type {
    int a;
    uint8_t b;
    uint64_t c;
};

class random_generator {
    std::random_device rnd_device;
    std::mt19937 mersenne_engine;
    std::uniform_int_distribution<int> dist;

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
        std::generate(begin(rv), end(rv), rnd);
        return rv;
    }

    std::vector<struct compound_test_type>
    make_compound_vector(size_t size)
    {
        std::vector<struct compound_test_type> rv(size);
        auto rnd = [this]() {
            struct compound_test_type t;
            t.a = this->dist(this->mersenne_engine);
            t.b = COMPOUND_MAGIC_B;
            t.c = COMPOUND_MAGIC_C;

            return t;
        };

        std::generate(begin(rv), end(rv), rnd);

        return rv;
    }
};

int
simple_cmp(const void *a, const void *b)
{
    auto lhs = static_cast<const int *>(a);
    auto rhs = static_cast<const int *>(b);

    return *lhs - *rhs;
}

int
compound_cmp(const void *a, const void *b)
{
    auto lhs = *(static_cast<const struct compound_test_type *>(a));
    auto rhs = *(static_cast<const struct compound_test_type *>(b));

    return lhs.a - rhs.a;
}

int
transposable_cmp(const void *a, const void *b, void *ctx)
{
    bool reverse = ctx ? *(static_cast<bool *>(ctx)) : false;
    auto lhs = static_cast<const int *>(a);
    auto rhs = static_cast<const int *>(b);

    if (reverse)
        return *rhs - *lhs;
    return *lhs - *rhs;
};

int
counting_cmp(const void *a, const void *b, void *ctx)
{
    auto count = static_cast<int *>(ctx);
    (*count)++;

    auto lhs = static_cast<const int *>(a);
    auto rhs = static_cast<const int *>(b);
    return *lhs - *rhs;
};
} // namespace

TEST_CASE("Safe to invoke on an empty array", "[qsort]")
{
    std::vector<int> input;
    __wt_qsort_r(&input[0], input.size(), sizeof(input[0]), transposable_cmp, nullptr);
    __wt_qsort(&input[0], input.size(), sizeof(input[0]), simple_cmp);
}

TEST_CASE("Single element", "[qsort]")
{
    std::vector<int> input = {123};
    __wt_qsort_r(&input[0], input.size(), sizeof(input[0]), transposable_cmp, nullptr);
    CHECK(input[0] == 123);
}

TEST_CASE("Compound type", "[qsort]")
{
    random_generator rand_gen;
    std::vector<struct compound_test_type> input{rand_gen.make_compound_vector(1000)};

    __wt_qsort(&input[0], input.size(), sizeof(input[0]), compound_cmp);

    for (int i = 0; i < 1000; i++) {
        if (i != 0)
            CHECK(input[i - 1].a <= input[i].a);
        CHECK(input[i].b == COMPOUND_MAGIC_B);
        CHECK(input[i].c == COMPOUND_MAGIC_C);
    }
}

TEST_CASE("Check contents", "[qsort]")
{
    random_generator rand_gen;
    std::vector<int> input{rand_gen.make_vector(10000)};
    std::vector<int> input_copy{input};

    __wt_qsort(&input[0], input.size(), sizeof(input[0]), simple_cmp);

    CHECK(std::is_sorted(input.begin(), input.end()));
    for (int i = 0; i < 10000; i++)
        CHECK(std::find(input_copy.begin(), input_copy.end(), input[i]) != input_copy.end());
}

TEST_CASE("Test context argument for comparator", "[qsort]")
{
    random_generator rand_gen;
    std::vector<int> input{rand_gen.make_vector(100)};
    bool reverse;

    reverse = false;
    __wt_qsort_r(&input[0], input.size(), sizeof(input[0]), transposable_cmp, &reverse);
    CHECK(std::is_sorted(input.begin(), input.end()));

    reverse = true;
    __wt_qsort_r(&input[0], input.size(), sizeof(input[0]), transposable_cmp, &reverse);
    CHECK(std::is_sorted(input.rbegin(), input.rend()));
}

TEST_CASE("Test context is mutable", "[qsort]")
{
    std::vector<int> input{1, 2, 3, 4, 5};

    int count = 0;
    __wt_qsort_r(&input[0], input.size(), sizeof(input[0]), counting_cmp, &count);
    CHECK(count > 0);
}
