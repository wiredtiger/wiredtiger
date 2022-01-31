#include <memory>

#include <catch2/catch.hpp>

#include "wt_internal.h"

TEST_CASE("block_off_srch_last", "[extent_list]") {
    std::vector<WT_EXT*> head(WT_SKIP_MAXDEPTH, nullptr);
    WT_EXT **stack[WT_SKIP_MAXDEPTH];

    SECTION("empty list has empty final element") {
        REQUIRE(__ut_block_off_srch_last(&head[0], stack) == nullptr);
    }
}
