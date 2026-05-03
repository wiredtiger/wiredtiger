/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

// Standard include:
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string_view>

// External include:
#include <catch2/catch.hpp>

// WiredTiger include:
#include "wt_internal.h"
#include "wrappers/connection_wrapper.h"
#include "truncate_list_helpers.hpp"

using namespace truncate_list_helpers;

namespace {

const char *
prepare_home_directory(const char *home)
{
    std::filesystem::remove_all(home);
    return home;
}

class follower_connection {
public:
    follower_connection()
    {
        auto &wt_session = _session->iface;

        constexpr auto table_uri = "layered:test_truncate_list";
        constexpr auto table_config =
          "key_format=S,value_format=S,block_manager=disagg,type=layered";

        REQUIRE(wt_session.create(&wt_session, table_uri, table_config) == 0);

        WT_CURSOR *cursor = nullptr;
        REQUIRE(wt_session.open_cursor(&wt_session, table_uri, nullptr, nullptr, &cursor) == 0);

        auto *clayered = reinterpret_cast<WT_CURSOR_LAYERED *>(cursor);
        _layered = reinterpret_cast<WT_LAYERED_TABLE *>(clayered->dhandle);

        REQUIRE(wt_session.begin_transaction(&wt_session, nullptr) == 0);
    }

    ~follower_connection()
    {
        auto *txn = _session->txn;

        for (size_t i = 0; i < txn->mod_count; ++i) {
            __wt_txn_op_free(_session, &txn->mod[i]);
        }

        txn->mod_count = 0;
    }

    follower_connection(const follower_connection &) = delete;
    follower_connection &operator=(const follower_connection &) = delete;

    follower_connection(follower_connection &&) = delete;
    follower_connection &operator=(follower_connection &&) = delete;

    [[nodiscard]] WT_SESSION_IMPL &
    session() const
    {
        return *_session;
    }

    [[nodiscard]] WT_LAYERED_TABLE &
    layered_table() const
    {
        return *_layered;
    }

    [[nodiscard]] uint32_t
    reference_count() const
    {
        return __wt_atomic_load_uint32_relaxed(&_layered->iface.references);
    }

private:
    scoped_fast_truncate_enable _enable;

    static constexpr auto home = "WT_TEST.truncate_list";

    static constexpr auto connection_config =
      "create,"
      "extensions=[./ext/page_log/palite/libwiredtiger_palite.so],"
      "disaggregated=(role=follower,page_log=palite)";

    connection_wrapper _wrapper{prepare_home_directory(home), connection_config};
    WT_SESSION_IMPL *_session = _wrapper.create_session();
    WT_LAYERED_TABLE *_layered = nullptr;
};

int
insert_one_entry(follower_connection &connection)
{
    auto start_key = make_item("a");
    auto stop_key = make_item("z");

    return __wt_insert_truncate_entry(
      &connection.session(), &connection.layered_table(), &start_key, &stop_key);
}

int
insert_n_entries(follower_connection &connection, const size_t count)
{
    for (size_t i = 0; i < count; ++i) {
        WT_RET(insert_one_entry(connection));
    }

    return 0;
}

} // namespace

SCENARIO("adding an entry successfully returns 0", "[truncate_list][insert]")
{
    GIVEN("a follower with an empty truncate list")
    {
        follower_connection connection;

        WHEN("an entry is inserted")
        {
            const auto result = insert_one_entry(connection);

            THEN("it returns 0")
            {
                REQUIRE(result == 0);
            }
        }
    }
}

SCENARIO("adding an entry inserts one entry into the truncate list", "[truncate_list][insert]")
{
    GIVEN("a follower with an empty truncate list")
    {
        follower_connection connection;

        WHEN("an entry is inserted")
        {
            insert_one_entry(connection);

            THEN("exactly one entry appears on the truncate list")
            {
                REQUIRE(truncate_list_size(connection.layered_table()) == 1);
            }
        }
    }
}

SCENARIO("adding an entry increments the dhandle reference count", "[truncate_list][insert]")
{
    GIVEN("a follower with an empty truncate list")
    {
        follower_connection connection;

        WHEN("an entry is inserted")
        {
            const auto reference_count = connection.reference_count();
            insert_one_entry(connection);

            THEN("the dhandle reference count is incremented by one")
            {
                const auto expected_count = reference_count + 1;
                REQUIRE(connection.reference_count() == expected_count);
            }
        }
    }
}

SCENARIO("adding an entry preserves the session dhandle", "[truncate_list][insert]")
{
    GIVEN("a follower with an empty truncate list")
    {
        follower_connection connection;

        WHEN("an entry is inserted")
        {
            const auto *const expected_dhandle = connection.session().dhandle;
            insert_one_entry(connection);

            THEN("the session dhandle does not change")
            {
                REQUIRE(connection.session().dhandle == expected_dhandle);
            }
        }
    }
}

SCENARIO(
  "adding an entry stores a bounded range when both keys are provided", "[truncate_list][insert]")
{
    GIVEN("a follower with an empty truncate list")
    {
        follower_connection connection;

        WHEN("an entry is inserted with a start and a stop key")
        {
            auto start_key = make_item("a");
            auto stop_key = make_item("z");

            std::ignore = __wt_insert_truncate_entry(
              &connection.session(), &connection.layered_table(), &start_key, &stop_key);

            THEN("the truncate list entry contains both keys")
            {
                const auto *const first = TAILQ_FIRST(&connection.layered_table().truncateqh);
                REQUIRE(first != nullptr);
                REQUIRE(as_view(first->start_key) == as_view(start_key));
                REQUIRE(as_view(first->stop_key) == as_view(stop_key));
            }
        }
    }
}

SCENARIO("adding multiple entries stores them in insertion order", "[truncate_list][insert]")
{
    GIVEN("a follower with an empty truncate list")
    {
        follower_connection connection;

        WHEN("multiple entries are inserted in order")
        {
            using key_pair = std::pair<WT_ITEM, WT_ITEM>;
            std::array<key_pair, 3> keys = {
              key_pair{make_item("a"), make_item("b")},
              key_pair{make_item("c"), make_item("d")},
              key_pair{make_item("e"), make_item("f")},
            };

            for (auto &[start_key, stop_key] : keys) {
                std::ignore = __wt_insert_truncate_entry(
                  &connection.session(), &connection.layered_table(), &start_key, &stop_key);
            }

            THEN("the entries appear on the truncate list in insertion order")
            {
                REQUIRE(truncate_list_size(connection.layered_table()) == keys.size());

                const auto *entry = TAILQ_FIRST(&connection.layered_table().truncateqh);
                for (const auto &[start_key, stop_key] : keys) {
                    REQUIRE(entry != nullptr);
                    REQUIRE(as_view(entry->start_key) == as_view(start_key));
                    REQUIRE(as_view(entry->stop_key) == as_view(stop_key));
                    entry = TAILQ_NEXT(entry, q);
                }
            }
        }
    }
}

SCENARIO("adding N entries increments the dhandle reference count by N", "[truncate_list][insert]")
{
    GIVEN("a follower with an empty truncate list")
    {
        follower_connection connection;
        const auto reference_count = connection.reference_count();

        WHEN("multiple entries are inserted")
        {
            const auto num_entries = 4;
            insert_n_entries(connection, num_entries);

            THEN("the dhandle reference count is incremented by N")
            {
                const auto expected_count = reference_count + num_entries;
                REQUIRE(connection.reference_count() == expected_count);
            }
        }
    }
}

SCENARIO("adding an entry registers a follower-truncate op", "[truncate_list][insert]")
{
    GIVEN("a follower with an empty truncate list")
    {
        follower_connection connection;

        WHEN("an entry is inserted")
        {
            const auto mod_count = connection.session().txn->mod_count;
            insert_one_entry(connection);

            THEN("the registered op points to the entry")
            {
                const auto *const txn = connection.session().txn;
                const auto expected_count = mod_count + 1;
                REQUIRE(txn->mod_count == expected_count);

                const auto &op = txn->mod[txn->mod_count - 1];
                REQUIRE(op.type == WT_TXN_OP_FOLLOWER_TRUNCATE);
                REQUIRE(
                  op.u.follower_truncate.t == TAILQ_FIRST(&connection.layered_table().truncateqh));
            }
        }
    }
}

SCENARIO("adding two entries with identical keys adds both to the truncate list",
  "[truncate_list][insert]")
{
    GIVEN("a follower with an empty truncate list")
    {
        follower_connection connection;

        WHEN("inserting two identical entries")
        {
            auto start_key = make_item("a");
            auto stop_key = make_item("z");

            std::ignore = __wt_insert_truncate_entry(
              &connection.session(), &connection.layered_table(), &start_key, &stop_key);

            std::ignore = __wt_insert_truncate_entry(
              &connection.session(), &connection.layered_table(), &start_key, &stop_key);

            THEN("both entries appear on the truncate list")
            {
                const auto *entry = TAILQ_FIRST(&connection.layered_table().truncateqh);
                REQUIRE(entry != nullptr);
                REQUIRE(as_view(entry->start_key) == as_view(start_key));
                REQUIRE(as_view(entry->stop_key) == as_view(stop_key));

                entry = TAILQ_NEXT(entry, q);
                REQUIRE(entry != nullptr);
                REQUIRE(as_view(entry->start_key) == as_view(start_key));
                REQUIRE(as_view(entry->stop_key) == as_view(stop_key));

                entry = TAILQ_NEXT(entry, q);
                REQUIRE(entry == nullptr);
            }
        }
    }
}

SCENARIO("adding an entry stamps it with the inserting transaction id", "[truncate_list][insert]")
{
    GIVEN("a follower with an empty truncate list")
    {
        follower_connection connection;

        WHEN("an entry is inserted")
        {
            insert_one_entry(connection);

            THEN("the entry txn_id matches the transaction id")
            {
                const auto *const first = TAILQ_FIRST(&connection.layered_table().truncateqh);
                REQUIRE(first != nullptr);
                REQUIRE(first->txn_id == connection.session().txn->time_point.id);
                REQUIRE(first->txn_id != WT_TXN_NONE);
            }
        }
    }
}

SCENARIO("adding an entry leaves the start and durable timestamps unset", "[truncate_list][insert]")
{
    GIVEN("a follower with an empty truncate list")
    {
        follower_connection connection;

        WHEN("an entry is inserted")
        {
            insert_one_entry(connection);

            THEN("the entry has unset start and durable timestamps")
            {
                const auto *const first = TAILQ_FIRST(&connection.layered_table().truncateqh);
                REQUIRE(first != nullptr);
                REQUIRE(first->start_ts == WT_TS_NONE);
                REQUIRE(first->durable_ts == WT_TS_NONE);
            }
        }
    }
}

SCENARIO("adding an entry records the originating layered table", "[truncate_list][insert]")
{
    GIVEN("a follower with an empty truncate list")
    {
        follower_connection connection;

        WHEN("an entry is inserted")
        {
            insert_one_entry(connection);

            THEN("the entry references the layered table it was inserted on")
            {
                const auto *const first = TAILQ_FIRST(&connection.layered_table().truncateqh);
                REQUIRE(first != nullptr);
                REQUIRE(first->layered_table == &connection.layered_table());
            }
        }
    }
}

SCENARIO("adding an entry releases the truncate lock", "[truncate_list][insert]")
{
    GIVEN("a follower with an empty truncate list")
    {
        follower_connection connection;

        WHEN("an entry is inserted")
        {
            insert_one_entry(connection);

            THEN("the truncate lock is not held")
            {
                auto &lock = connection.layered_table().truncate_lock;
                REQUIRE(__wt_try_writelock(&connection.session(), &lock) == 0);
                __wt_writeunlock(&connection.session(), &lock);
            }
        }
    }
}
