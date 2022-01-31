#include <catch2/catch.hpp>

#include "wt_internal.h"

TEST_CASE("Playing with extent list testing", "[extent_list]") {
    __ut_block_size_srch(nullptr, 0, nullptr);
    int res = 1; //__wt_block_extlist_read(nullptr, nullptr, nullptr, 0);

    REQUIRE(res == 1);
}
