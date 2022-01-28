#include <catch2/catch.hpp>

#include "wt_internal.h"

uint32_t factorial(uint32_t num) {
    return num > 1 ? factorial(num - 1) * num : 1;
}

TEST_CASE("Playing with extent list testing", "[extent_list]") {
    int res = __wt_block_extlist_read(nullptr, nullptr, nullptr, 0);

    REQUIRE(res == 1);
}

TEST_CASE("Factorials are computed", "[factorial]") {
    REQUIRE(factorial(0) == 1);
    REQUIRE(factorial(1) == 1);
    REQUIRE(factorial(2) == 2);
    REQUIRE(factorial(3) == 6);
    REQUIRE(factorial(10) == 3628800);
}
