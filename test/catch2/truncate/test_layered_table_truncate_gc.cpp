/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>

#include "wt_internal.h"
#include "truncate_list_helpers.hpp"

using namespace truncate_list_helpers;

namespace {

void
insert_durable_entry(truncate_list_fixture &fixture, const wt_timestamp_t durable_ts)
{
    auto *entry = fixture.add_entry(make_item("a"), make_item("z"));
    fixture.commit_entry(entry, durable_ts);
}

} // namespace

TEST_CASE("truncate garbage collection counts runs and entries", "[truncate_list][gc]")
{
    truncate_list_fixture fixture;
    auto *session = &fixture.session();
    auto *table = &fixture.layered_table();
    fixture.add_entry(make_item("a"), make_item("b"));
    insert_durable_entry(fixture, WT_TS_NONE);
    insert_durable_entry(fixture, 20);
    insert_durable_entry(fixture, 10);

    for (const auto prune_ts :
      {wt_timestamp_t(WT_TS_NONE), wt_timestamp_t(10), wt_timestamp_t(20)}) {
        CAPTURE(prune_ts);
        WT_WITH_DHANDLE(
          session, &table->iface, __ut_layered_table_truncate_gc(session, table, prune_ts));
        const auto runs = prune_ts / 10;
        const auto examined = prune_ts == 0 ? 0 : (prune_ts == 10 ? 4 : 7);
        CHECK(WT_STAT_CONN_READ(S2C(session)->stats, layered_truncate_list_gc_runs) == runs);
        CHECK(WT_STAT_DSRC_READ(table->iface.stats, layered_truncate_list_gc_runs) == runs);
        CHECK(WT_STAT_CONN_READ(S2C(session)->stats, layered_truncate_list_gc_entries_examined) ==
          examined);
        CHECK(WT_STAT_DSRC_READ(table->iface.stats, layered_truncate_list_gc_entries_examined) ==
          examined);
        CHECK(
          WT_STAT_CONN_READ(S2C(session)->stats, layered_truncate_list_gc_entries_removed) == runs);
        CHECK(
          WT_STAT_DSRC_READ(table->iface.stats, layered_truncate_list_gc_entries_removed) == runs);
    }
    CHECK(truncate_list_size(*table) == 2);
}

SCENARIO("garbage collection with a zeroed prune timestamp is a no-op", "[truncate_list][gc]")
{
    GIVEN("a truncate list with one durable entry")
    {
        truncate_list_fixture fixture;

        const wt_timestamp_t durable_ts = 10u;
        insert_durable_entry(fixture, durable_ts);

        const auto initial_size = truncate_list_size(fixture.layered_table());
        const auto initial_reference_count = fixture.reference_count();

        WHEN("garbage collection runs with WT_TS_NONE as the prune timestamp")
        {
            __ut_layered_table_truncate_gc(
              &fixture.session(), &fixture.layered_table(), WT_TS_NONE);

            THEN("the truncate list remains unchanged")
            {
                REQUIRE(truncate_list_size(fixture.layered_table()) == initial_size);
                REQUIRE(fixture.reference_count() == initial_reference_count);
            }
        }
    }
}

SCENARIO("garbage collection does not remove an uncommitted entry", "[truncate_list][gc]")
{
    GIVEN("a truncate list with one uncommitted entry")
    {
        truncate_list_fixture fixture;
        fixture.add_entry(make_item("a"), make_item("z"));

        const auto initial_size = truncate_list_size(fixture.layered_table());
        const auto initial_reference_count = fixture.reference_count();

        WHEN("garbage collection runs with a valid prune timestamp")
        {
            const wt_timestamp_t prune_ts = 10u;
            __ut_layered_table_truncate_gc(&fixture.session(), &fixture.layered_table(), prune_ts);

            THEN("the truncate list remains unchanged")
            {
                REQUIRE(truncate_list_size(fixture.layered_table()) == initial_size);
                REQUIRE(fixture.reference_count() == initial_reference_count);
            }
        }
    }
}

SCENARIO("garbage collection does not remove an entry with a zeroed durable timestamp",
  "[truncate_list][gc]")
{
    GIVEN("a truncate list with one entry whose durable timestamp is WT_TS_NONE")
    {
        truncate_list_fixture fixture;
        insert_durable_entry(fixture, WT_TS_NONE);

        const auto initial_size = truncate_list_size(fixture.layered_table());
        const auto initial_reference_count = fixture.reference_count();

        WHEN("garbage collection runs with a valid prune timestamp")
        {
            const wt_timestamp_t prune_ts = 10u;
            __ut_layered_table_truncate_gc(&fixture.session(), &fixture.layered_table(), prune_ts);

            THEN("the truncate list remains unchanged")
            {
                REQUIRE(truncate_list_size(fixture.layered_table()) == initial_size);
                REQUIRE(fixture.reference_count() == initial_reference_count);
            }
        }
    }
}

SCENARIO(
  "garbage collection does not remove an entry above the prune timestamp", "[truncate_list][gc]")
{
    GIVEN("a truncate list with one entry whose durable timestamp is above the prune timestamp")
    {
        truncate_list_fixture fixture;

        const wt_timestamp_t durable_ts = 10u;
        insert_durable_entry(fixture, durable_ts);

        const auto initial_size = truncate_list_size(fixture.layered_table());
        const auto initial_reference_count = fixture.reference_count();

        WHEN("garbage collection runs with a lower prune timestamp")
        {
            const auto prune_ts = durable_ts - 1u;
            __ut_layered_table_truncate_gc(&fixture.session(), &fixture.layered_table(), prune_ts);

            THEN("the truncate list remains unchanged")
            {
                REQUIRE(truncate_list_size(fixture.layered_table()) == initial_size);
                REQUIRE(fixture.reference_count() == initial_reference_count);
            }
        }
    }
}

SCENARIO("garbage collection removes an entry at the prune timestamp", "[truncate_list][gc]")
{
    GIVEN("a truncate list with one entry whose durable timestamp equals the prune timestamp")
    {
        truncate_list_fixture fixture;

        const wt_timestamp_t durable_ts = 10u;
        insert_durable_entry(fixture, durable_ts);

        WHEN("garbage collection runs with a matching prune timestamp")
        {
            __ut_layered_table_truncate_gc(
              &fixture.session(), &fixture.layered_table(), durable_ts);

            THEN("the entry is removed")
            {
                REQUIRE(truncate_list_size(fixture.layered_table()) == 0u);
            }
        }
    }
}

SCENARIO("garbage collection removes an entry below the prune timestamp", "[truncate_list][gc]")
{
    GIVEN("a truncate list with one entry whose durable timestamp is below the prune timestamp")
    {
        truncate_list_fixture fixture;

        const wt_timestamp_t durable_ts = 10u;
        insert_durable_entry(fixture, durable_ts);

        WHEN("garbage collection runs with a higher prune timestamp")
        {
            const auto prune_ts = durable_ts + 1u;
            __ut_layered_table_truncate_gc(&fixture.session(), &fixture.layered_table(), prune_ts);

            THEN("the entry is removed")
            {
                REQUIRE(truncate_list_size(fixture.layered_table()) == 0u);
            }
        }
    }
}

SCENARIO(
  "garbage collection on a multi-entry list only removes eligible entries", "[truncate_list][gc]")
{
    GIVEN("a truncate list with one eligible entry and one ineligible entry")
    {
        truncate_list_fixture fixture;

        const wt_timestamp_t prune_ts = 10u;

        const auto eligible_durable_ts = prune_ts - 1u;
        insert_durable_entry(fixture, eligible_durable_ts);

        const auto surviving_durable_ts = prune_ts + 1u;
        insert_durable_entry(fixture, surviving_durable_ts);

        const auto initial_size = truncate_list_size(fixture.layered_table());

        WHEN("garbage collection runs with a valid prune timestamp")
        {
            __ut_layered_table_truncate_gc(&fixture.session(), &fixture.layered_table(), prune_ts);

            THEN("the eligible entry is removed")
            {
                const auto expected_size = initial_size - 1u;
                REQUIRE(truncate_list_size(fixture.layered_table()) == expected_size);

                const auto *head = truncate_list_head(fixture.layered_table());
                REQUIRE(head->durable_ts == surviving_durable_ts);
            }
        }
    }

    GIVEN("a truncate list with two eligible entries")
    {
        truncate_list_fixture fixture;

        const wt_timestamp_t prune_ts = 10u;
        insert_durable_entry(fixture, prune_ts - 1u);
        insert_durable_entry(fixture, prune_ts);

        WHEN("garbage collection runs with a valid prune timestamp")
        {
            __ut_layered_table_truncate_gc(&fixture.session(), &fixture.layered_table(), prune_ts);

            THEN("all entries are removed")
            {
                REQUIRE(truncate_list_size(fixture.layered_table()) == 0u);
            }
        }
    }

    GIVEN("a truncate list with two ineligible entries")
    {
        truncate_list_fixture fixture;

        const wt_timestamp_t prune_ts = 10u;
        insert_durable_entry(fixture, prune_ts + 1u);
        insert_durable_entry(fixture, prune_ts + 2u);

        const auto initial_size = truncate_list_size(fixture.layered_table());
        const auto initial_reference_count = fixture.reference_count();

        WHEN("garbage collection runs with a valid prune timestamp")
        {
            __ut_layered_table_truncate_gc(&fixture.session(), &fixture.layered_table(), prune_ts);

            THEN("the truncate list remains unchanged")
            {
                REQUIRE(truncate_list_size(fixture.layered_table()) == initial_size);
                REQUIRE(fixture.reference_count() == initial_reference_count);
            }
        }
    }

    GIVEN("a truncate list with one uncommitted entry and one eligible entry")
    {
        truncate_list_fixture fixture;

        const wt_timestamp_t prune_ts = 10u;
        fixture.add_entry(make_item("a"), make_item("z"));

        insert_durable_entry(fixture, prune_ts - 1u);

        const auto initial_size = truncate_list_size(fixture.layered_table());

        WHEN("garbage collection runs with a valid prune timestamp")
        {
            __ut_layered_table_truncate_gc(&fixture.session(), &fixture.layered_table(), prune_ts);

            THEN("the eligible entry is removed")
            {
                const auto expected_size = initial_size - 1u;
                REQUIRE(truncate_list_size(fixture.layered_table()) == expected_size);

                const auto *head = truncate_list_head(fixture.layered_table());
                REQUIRE(head->durable_ts == WT_TS_NONE);
            }
        }
    }

    GIVEN("a truncate list with one uncommitted entry and one ineligible entry")
    {
        truncate_list_fixture fixture;

        fixture.add_entry(make_item("a"), make_item("z"));

        const wt_timestamp_t prune_ts = 10u;
        insert_durable_entry(fixture, prune_ts + 1u);

        const auto initial_size = truncate_list_size(fixture.layered_table());
        const auto initial_reference_count = fixture.reference_count();

        WHEN("garbage collection runs with a valid prune timestamp")
        {
            __ut_layered_table_truncate_gc(&fixture.session(), &fixture.layered_table(), prune_ts);

            THEN("the truncate list remains unchanged")
            {
                REQUIRE(truncate_list_size(fixture.layered_table()) == initial_size);
                REQUIRE(fixture.reference_count() == initial_reference_count);
            }
        }
    }
}

SCENARIO("garbage collection with an empty truncate list is a no-op", "[truncate_list][gc]")
{
    GIVEN("an empty truncate list")
    {
        truncate_list_fixture fixture;

        const auto initial_size = truncate_list_size(fixture.layered_table());
        const auto initial_reference_count = fixture.reference_count();

        WHEN("garbage collection runs with a valid prune timestamp")
        {
            const wt_timestamp_t prune_ts = 10u;
            __ut_layered_table_truncate_gc(&fixture.session(), &fixture.layered_table(), prune_ts);

            THEN("the truncate list remains unchanged")
            {
                REQUIRE(truncate_list_size(fixture.layered_table()) == initial_size);
                REQUIRE(fixture.reference_count() == initial_reference_count);
            }
        }
    }
}

SCENARIO("garbage collection does not release the dhandle reference when the list is not cleared",
  "[truncate_list][gc]")
{
    GIVEN("a truncate list with one eligible entry and one ineligible entry")
    {
        truncate_list_fixture fixture;
        const wt_timestamp_t prune_ts = 10u;
        insert_durable_entry(fixture, prune_ts - 1u);
        insert_durable_entry(fixture, prune_ts + 1u);

        const auto initial_reference_count = fixture.reference_count();

        WHEN("garbage collection runs with a valid prune timestamp")
        {
            __ut_layered_table_truncate_gc(&fixture.session(), &fixture.layered_table(), prune_ts);

            THEN("the dhandle reference count is unchanged")
            {
                REQUIRE(fixture.reference_count() == initial_reference_count);
            }
        }
    }
}

SCENARIO(
  "garbage collection that empties the list releases the dhandle reference", "[truncate_list][gc]")
{
    GIVEN("a truncate list with one eligible entry")
    {
        truncate_list_fixture fixture;
        const wt_timestamp_t prune_ts = 10u;
        insert_durable_entry(fixture, prune_ts - 1u);
        const auto initial_reference_count = fixture.reference_count();

        WHEN("garbage collection runs with a valid prune timestamp")
        {
            __ut_layered_table_truncate_gc(&fixture.session(), &fixture.layered_table(), prune_ts);

            THEN("the dhandle reference count is decremented by exactly one")
            {
                REQUIRE(fixture.reference_count() == initial_reference_count - 1u);
            }
        }
    }
}
