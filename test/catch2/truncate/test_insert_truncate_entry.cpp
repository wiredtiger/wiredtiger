/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

// Standard include:
#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <mutex>
#include <string_view>
#include <thread>

// External include:
#include <catch2/catch.hpp>

// WiredTiger include:
#include "wt_internal.h"

#include "src/cursor/cur_layered_private.h"
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
        constexpr auto uri = "layered:test_truncate_list";
        constexpr auto config = "key_format=S,value_format=S,block_manager=disagg,type=layered";
        auto &session = _session_impl->iface;

        REQUIRE(session.create(&session, uri, config) == 0);
        REQUIRE(session.open_cursor(&session, uri, nullptr, nullptr, &_cursor) == 0);
        REQUIRE(session.begin_transaction(&session, nullptr) == 0);
    }

    ~follower_connection()
    {
        auto *txn = _session_impl->txn;

        for (size_t i = 0; i < txn->mod_count; ++i)
            __wt_txn_op_free(_session_impl, &txn->mod[i]);

        txn->mod_count = 0;
    }

    [[nodiscard]] WT_SESSION_IMPL &
    session() const
    {
        return *_session_impl;
    }

    [[nodiscard]] WT_LAYERED_TABLE &
    layered_table() const
    {
        auto *clayered = reinterpret_cast<WTI_CURSOR_LAYERED *>(_cursor);
        return *reinterpret_cast<WT_LAYERED_TABLE *>(clayered->dhandle);
    }

    [[nodiscard]] uint32_t
    reference_count() const
    {
        return __wt_atomic_load_uint32_relaxed(&layered_table().iface.references);
    }

private:
    static constexpr auto home = "WT_TEST.truncate_list";

    static constexpr auto connection_config =
      "create,"
      "extensions=[./ext/page_log/palite/libwiredtiger_palite.so],"
      "disaggregated=(role=follower,page_log=palite)";

    connection_wrapper _wrapper{prepare_home_directory(home), connection_config};
    WT_SESSION_IMPL *_session_impl{_wrapper.create_session()};
    WT_CURSOR *_cursor{};
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
    for (size_t i = 0; i < count; ++i)
        WT_RET(insert_one_entry(connection));

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
            CHECK(insert_one_entry(connection) == 0);

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
            CHECK(insert_one_entry(connection) == 0);

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
            CHECK(insert_one_entry(connection) == 0);

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

            CHECK(__wt_insert_truncate_entry(&connection.session(), &connection.layered_table(),
                    &start_key, &stop_key) == 0);

            THEN("the truncate list entry contains both keys")
            {
                const auto *const first = truncate_list_head(connection.layered_table());
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
            std::array<truncate_range, 3> keys{
              truncate_range{make_item("a"), make_item("b")},
              truncate_range{make_item("c"), make_item("d")},
              truncate_range{make_item("e"), make_item("f")},
            };

            for (auto &[start_key, stop_key] : keys) {
                CHECK(__wt_insert_truncate_entry(&connection.session(), &connection.layered_table(),
                        &start_key, &stop_key) == 0);
            }

            THEN("the entries appear on the truncate list in insertion order")
            {
                REQUIRE(truncate_list_size(connection.layered_table()) == keys.size());

                const auto *entry = truncate_list_head(connection.layered_table());
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

SCENARIO("adding multiple entries to an empty list increments the dhandle reference count by one",
  "[truncate_list][insert]")
{
    GIVEN("a follower with an empty truncate list")
    {
        follower_connection connection;
        const auto reference_count = connection.reference_count();

        WHEN("multiple entries are inserted")
        {
            CHECK(insert_n_entries(connection, 4u) == 0);

            THEN("the dhandle reference count is incremented by exactly one")
            {
                REQUIRE(connection.reference_count() == reference_count + 1u);
            }
        }
    }
}

SCENARIO("adding an entry to a non-empty truncate list does not change the dhandle reference count",
  "[truncate_list][insert]")
{
    GIVEN("a follower with one entry in the truncate list")
    {
        follower_connection connection;
        CHECK(insert_one_entry(connection) == 0);
        const auto reference_count = connection.reference_count();

        WHEN("a second entry is inserted")
        {
            CHECK(insert_one_entry(connection) == 0);

            THEN("the dhandle reference count is unchanged")
            {
                REQUIRE(connection.reference_count() == reference_count);
            }
        }
    }
}

SCENARIO("adding an entry registers a follower-truncate op", "[truncate_list][insert]")
{
    GIVEN("a follower with an empty truncate list")
    {
        follower_connection connection;
        const auto mod_count = connection.session().txn->mod_count;

        WHEN("an entry is inserted")
        {
            CHECK(insert_one_entry(connection) == 0);

            THEN("exactly one new txn op is registered")
            {
                const auto expected_mod_count = mod_count + 1;
                REQUIRE(connection.session().txn->mod_count == expected_mod_count);
            }

            THEN("the registered op is a follower-truncate pointing to the list head")
            {
                const auto *op = last_txn_op(connection.session());
                REQUIRE(op->type == WT_TXN_OP_FOLLOWER_TRUNCATE);
                REQUIRE(
                  op->u.follower_truncate.t == truncate_list_head(connection.layered_table()));
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

            CHECK(__wt_insert_truncate_entry(&connection.session(), &connection.layered_table(),
                    &start_key, &stop_key) == 0);

            CHECK(__wt_insert_truncate_entry(&connection.session(), &connection.layered_table(),
                    &start_key, &stop_key) == 0);

            THEN("both entries appear on the truncate list")
            {
                const auto *entry = truncate_list_head(connection.layered_table());
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
            CHECK(insert_one_entry(connection) == 0);

            THEN("the entry txn_id matches the transaction id")
            {
                const auto *const first = truncate_list_head(connection.layered_table());
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
            CHECK(insert_one_entry(connection) == 0);

            THEN("the entry has unset start and durable timestamps")
            {
                const auto *const first = truncate_list_head(connection.layered_table());
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
            CHECK(insert_one_entry(connection) == 0);

            THEN("the entry references the layered table it was inserted on")
            {
                const auto *const first = truncate_list_head(connection.layered_table());
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
            CHECK(insert_one_entry(connection) == 0);

            THEN("the truncate lock is not held")
            {
                REQUIRE(lock_is_released(connection.session(), connection.layered_table()));
            }
        }
    }
}

TEST_CASE("uncommitted truncate ranges remain invisible during concurrent insertion",
  "[truncate_list][insert][concurrent]")
{
    follower_connection connection;
    auto *wt_conn = connection.session().iface.connection;
    auto &table = connection.layered_table();
    constexpr size_t worker_count = 4;
    constexpr size_t iterations = 1000;
    std::array<WT_SESSION *, worker_count> readers{}, writers{};
    std::array<int, worker_count> insert_results{}, search_results{};
    std::array<std::thread, worker_count> inserters, searchers;
    const std::array<std::string_view, worker_count> keys{"a", "b", "c", "d"};

    for (size_t i = 0; i < worker_count; ++i) {
        REQUIRE(wt_conn->open_session(wt_conn, nullptr, nullptr, &readers[i]) == 0);
        REQUIRE(readers[i]->begin_transaction(readers[i], "isolation=snapshot") == 0);
        REQUIRE(wt_conn->open_session(wt_conn, nullptr, nullptr, &writers[i]) == 0);
        REQUIRE(writers[i]->begin_transaction(writers[i], nullptr) == 0);
        auto key = make_item(keys[i]);
        REQUIRE(__wt_insert_truncate_entry(
                  reinterpret_cast<WT_SESSION_IMPL *>(writers[i]), &table, &key, &key) == 0);
    }
    search_results.fill(WT_NOTFOUND);

    std::mutex start_mutex;
    std::unique_lock<std::mutex> start_lock(start_mutex);
    for (size_t i = 0; i < worker_count; ++i) {
        inserters[i] = std::thread([&, i] {
            {
                std::lock_guard<std::mutex> guard(start_mutex);
            }
            auto key = make_item(keys[i]);
            for (size_t j = 0; j < iterations && insert_results[i] == 0; ++j)
                insert_results[i] = __wt_insert_truncate_entry(
                  reinterpret_cast<WT_SESSION_IMPL *>(writers[i]), &table, &key, &key);
        });
        searchers[i] = std::thread([&, i] {
            {
                std::lock_guard<std::mutex> guard(start_mutex);
            }
            auto key = make_item(keys[i]);
            for (size_t j = 0; j < iterations && search_results[i] == WT_NOTFOUND; ++j)
                search_results[i] = __wt_truncate_delete_visible_check(
                  reinterpret_cast<WT_SESSION_IMPL *>(readers[i]), &table.truncate_list, nullptr,
                  &key, nullptr, nullptr);
        });
    }

    start_lock.unlock();
    for (size_t i = 0; i < worker_count; ++i) {
        inserters[i].join();
        searchers[i].join();
    }

    CHECK(truncate_list_size(table) == worker_count * (iterations + 1));
    for (size_t i = 0; i < worker_count; ++i) {
        CHECK(insert_results[i] == 0);
        CHECK(search_results[i] == WT_NOTFOUND);
        REQUIRE(readers[i]->rollback_transaction(readers[i], nullptr) == 0);
        REQUIRE(readers[i]->close(readers[i], nullptr) == 0);
        REQUIRE(writers[i]->rollback_transaction(writers[i], nullptr) == 0);
        REQUIRE(writers[i]->close(writers[i], nullptr) == 0);
    }
}
