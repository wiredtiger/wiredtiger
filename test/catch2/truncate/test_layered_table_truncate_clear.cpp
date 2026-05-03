/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

// External include:
#include <catch2/catch.hpp>

// WiredTiger include:
#include "wt_internal.h"
#include "truncate_list_helpers.hpp"

using namespace truncate_list_helpers;

SCENARIO("clearing empties the truncate list", "[truncate_list][clear]")
{
    GIVEN("a fixture with two truncate entries")
    {
        truncate_list_fixture fixture;
        fixture.add_entry(make_item("a"), make_item("z"));
        fixture.add_entry(make_item("b"), make_item("y"));
        const auto size = truncate_list_size(fixture.layered_table());

        WHEN("the truncate list is cleared")
        {
            __wt_layered_table_truncate_clear(&fixture.session(), &fixture.layered_table());

            THEN("the truncate list entries are removed")
            {
                const auto expected_size = size - 2;
                REQUIRE(truncate_list_size(fixture.layered_table()) == expected_size);
            }
        }
    }
}

SCENARIO("clearing the truncate list releases the dhandle references", "[truncate_list][clear]")
{
    GIVEN("a fixture with two truncate entries")
    {
        truncate_list_fixture fixture;
        fixture.add_entry(make_item("a"), make_item("z"));
        fixture.add_entry(make_item("b"), make_item("y"));
        const auto reference_count = fixture.reference_count();

        WHEN("the truncate list is cleared")
        {
            __wt_layered_table_truncate_clear(&fixture.session(), &fixture.layered_table());

            THEN("the dhandle references are released")
            {
                const auto expected_reference_count = reference_count - 2;
                REQUIRE(fixture.reference_count() == expected_reference_count);
            }
        }
    }
}

SCENARIO("clearing an empty truncate list is a no-op", "[truncate_list][clear]")
{
    GIVEN("a fixture with an empty truncate list")
    {
        truncate_list_fixture fixture;
        const auto reference_count = fixture.reference_count();

        WHEN("the truncate list is cleared")
        {
            __wt_layered_table_truncate_clear(&fixture.session(), &fixture.layered_table());

            THEN("the truncate list is empty and the reference count is unchanged")
            {
                REQUIRE(truncate_list_size(fixture.layered_table()) == 0);
                REQUIRE(fixture.reference_count() == reference_count);
            }
        }
    }
}

SCENARIO("clearing the truncate list releases the truncate lock", "[truncate_list][clear]")
{
    GIVEN("a fixture with two truncate entries")
    {
        truncate_list_fixture fixture;
        fixture.add_entry(make_item("a"), make_item("z"));
        fixture.add_entry(make_item("b"), make_item("y"));

        WHEN("the truncate list is cleared")
        {
            __wt_layered_table_truncate_clear(&fixture.session(), &fixture.layered_table());

            THEN("the truncate lock is not held")
            {
                auto &lock = fixture.layered_table().truncate_lock;
                REQUIRE(__wt_try_writelock(&fixture.session(), &lock) == 0);
                __wt_writeunlock(&fixture.session(), &lock);
            }
        }
    }
}
