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

namespace {

WT_TXN_OP
make_op(WT_TRUNCATE *entry)
{
    WT_TXN_OP op{};
    op.type = WT_TXN_OP_FOLLOWER_TRUNCATE;
    op.u.follower_truncate.t = entry;
    return op;
}

} // namespace

SCENARIO(
  "rolling back a truncate entry removes it from the truncate list", "[truncate_list][rollback]")
{
    GIVEN("a fixture with one truncate entry")
    {
        truncate_list_fixture fixture;
        fixture.add_entry(make_item("a"), make_item("z"));
        auto op = make_op(TAILQ_FIRST(&fixture.layered_table().truncateqh));
        const auto size = truncate_list_size(fixture.layered_table());

        WHEN("the truncate is rolled back")
        {
            __wti_layered_table_truncate_rollback(&fixture.session(), &op);

            THEN("the truncate list entry is removed")
            {
                const auto expected_size = size - 1;
                REQUIRE(truncate_list_size(fixture.layered_table()) == expected_size);
            }
        }
    }
}

SCENARIO("rolling back a truncate entry clears the op pointer", "[truncate_list][rollback]")
{
    GIVEN("a fixture with one truncate entry")
    {
        truncate_list_fixture fixture;
        fixture.add_entry(make_item("a"), make_item("z"));
        auto op = make_op(TAILQ_FIRST(&fixture.layered_table().truncateqh));

        WHEN("the truncate is rolled back")
        {
            __wti_layered_table_truncate_rollback(&fixture.session(), &op);

            THEN("the op pointer is null")
            {
                REQUIRE(op.u.follower_truncate.t == nullptr);
            }
        }
    }
}

SCENARIO(
  "rolling back a truncate entry releases the dhandle reference", "[truncate_list][rollback]")
{
    GIVEN("a fixture with one truncate entry")
    {
        truncate_list_fixture fixture;
        fixture.add_entry(make_item("a"), make_item("z"));
        const auto reference_count = fixture.reference_count();
        auto op = make_op(TAILQ_FIRST(&fixture.layered_table().truncateqh));

        WHEN("the truncate is rolled back")
        {
            __wti_layered_table_truncate_rollback(&fixture.session(), &op);

            THEN("the dhandle reference is released")
            {
                REQUIRE(fixture.reference_count() == reference_count - 1);
            }
        }
    }
}

SCENARIO("rolling back a truncate entry releases the truncate lock", "[truncate_list][rollback]")
{
    GIVEN("a fixture with one truncate entry")
    {
        truncate_list_fixture fixture;
        fixture.add_entry(make_item("a"), make_item("z"));
        auto op = make_op(TAILQ_FIRST(&fixture.layered_table().truncateqh));

        WHEN("the truncate is rolled back")
        {
            __wti_layered_table_truncate_rollback(&fixture.session(), &op);

            THEN("the truncate lock is not held")
            {
                auto &lock = fixture.layered_table().truncate_lock;
                REQUIRE(__wt_try_writelock(&fixture.session(), &lock) == 0);
                __wt_writeunlock(&fixture.session(), &lock);
            }
        }
    }
}

SCENARIO("rolling back affects only the targeted entry in a multi-entry truncate list",
  "[truncate_list][rollback]")
{
    GIVEN("a fixture with three truncate entries")
    {
        truncate_list_fixture fixture;
        const auto *first_entry = fixture.add_entry(make_item("a"), make_item("b"));
        const auto *middle_entry = fixture.add_entry(make_item("c"), make_item("d"));
        const auto *last_entry = fixture.add_entry(make_item("e"), make_item("f"));
        const auto reference_count = fixture.reference_count();

        auto first_op = make_op(const_cast<WT_TRUNCATE *>(first_entry));
        auto middle_op = make_op(const_cast<WT_TRUNCATE *>(middle_entry));
        auto last_op = make_op(const_cast<WT_TRUNCATE *>(last_entry));

        WHEN("the middle truncate is rolled back")
        {
            __wti_layered_table_truncate_rollback(&fixture.session(), &middle_op);

            THEN("only the middle entry is removed and the others remain in order")
            {
                const auto *const head = TAILQ_FIRST(&fixture.layered_table().truncateqh);
                REQUIRE(head == first_entry);
                REQUIRE(TAILQ_NEXT(head, q) == last_entry);
                REQUIRE(TAILQ_NEXT(last_entry, q) == nullptr);
            }

            THEN("only the rolled-back op pointer is cleared")
            {
                REQUIRE(middle_op.u.follower_truncate.t == nullptr);
                REQUIRE(first_op.u.follower_truncate.t == first_entry);
                REQUIRE(last_op.u.follower_truncate.t == last_entry);
            }

            THEN("the dhandle reference count drops by exactly one")
            {
                REQUIRE(fixture.reference_count() == reference_count - 1);
            }
        }
    }
}
