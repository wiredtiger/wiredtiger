/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <string_view>
#include <tuple>
#include <utility>

#include <catch2/catch.hpp>

#include "wt_internal.h"
#include "../wrappers/connection_wrapper.h"

namespace {

/* Start key, stop key. */
using truncate_range = std::pair<WT_ITEM, WT_ITEM>;

[[nodiscard]] WT_ITEM
make_item(std::string_view view)
{
    WT_ITEM item{};
    item.data = view.data();
    item.size = view.size();
    return item;
}

[[nodiscard]] std::string_view
as_view(const WT_ITEM &item)
{
    return {static_cast<const char *>(item.data), item.size};
}

/* Enables the flag disagg_fast_truncate_2026 for the duration of its scope. */
class scoped_fast_truncate_flag {
public:
    scoped_fast_truncate_flag() : _old(__wt_process.disagg_fast_truncate_2026)
    {
        __wt_process.disagg_fast_truncate_2026 = true;
    }

    ~scoped_fast_truncate_flag()
    {
        __wt_process.disagg_fast_truncate_2026 = _old;
    }

private:
    bool _old;
};

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
        auto *layered_cursor = reinterpret_cast<WT_CURSOR_LAYERED *>(_cursor);
        return *reinterpret_cast<WT_LAYERED_TABLE *>(layered_cursor->dhandle);
    }

private:
    static constexpr auto home = "WT_TEST.truncate_list";

    static constexpr auto connection_config =
      "create,"
      "extensions=[./ext/page_log/palite/libwiredtiger_palite.so],"
      "disaggregated=(role=follower,page_log=palite)";

    scoped_fast_truncate_flag _fast_truncate_flag{};
    connection_wrapper _wrapper{home, connection_config};
    WT_SESSION_IMPL *_session_impl = _wrapper.create_session();
    WT_CURSOR *_cursor{};
};

[[nodiscard]] WT_TRUNCATE *
truncate_list_head(follower_connection &connection)
{
    return TAILQ_FIRST(&connection.layered_table().truncateqh);
}

[[nodiscard]] bool
lock_is_released(follower_connection &connection)
{
    auto &lock = connection.layered_table().truncate_lock;

    if (__wt_try_writelock(&connection.session(), &lock) != 0)
        return false;

    __wt_writeunlock(&connection.session(), &lock);
    return true;
}

[[nodiscard]] WT_TXN_OP *
last_txn_op(follower_connection &connection)
{
    const auto *txn = connection.session().txn;
    return &txn->mod[txn->mod_count - 1];
}

int
insert_one_entry(follower_connection &connection)
{
    auto start_key = make_item("a");
    auto stop_key = make_item("z");

    return __wt_insert_truncate_entry(
      &connection.session(), connection.layered_table().iface.name, &start_key, &stop_key);
}

int
insert_n_entries(follower_connection &connection, size_t count)
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
            insert_one_entry(connection);

            THEN("exactly one entry appears on the truncate list")
            {
                const auto *first = truncate_list_head(connection);
                REQUIRE(first != nullptr);
                REQUIRE(TAILQ_NEXT(first, q) == nullptr);
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
            const auto *expected_dhandle = connection.session().dhandle;
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
              &connection.session(), connection.layered_table().iface.name, &start_key, &stop_key);

            THEN("the truncate list entry contains both keys")
            {
                const auto *first = truncate_list_head(connection);
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
            const truncate_range keys[] = {
              {make_item("a"), make_item("b")},
              {make_item("c"), make_item("d")},
              {make_item("e"), make_item("f")},
            };

            for (auto [start_key, stop_key] : keys) {
                std::ignore = __wt_insert_truncate_entry(&connection.session(),
                  connection.layered_table().iface.name, &start_key, &stop_key);
            }

            THEN("the entries appear on the truncate list in insertion order")
            {
                const auto *entry = truncate_list_head(connection);

                for (const auto &[start_key, stop_key] : keys) {
                    REQUIRE(entry != nullptr);
                    REQUIRE(as_view(entry->start_key) == as_view(start_key));
                    REQUIRE(as_view(entry->stop_key) == as_view(stop_key));
                    entry = TAILQ_NEXT(entry, q);
                }

                REQUIRE(entry == nullptr);
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
            const auto *txn = connection.session().txn;
            const auto mod_count = txn->mod_count;
            insert_one_entry(connection);

            THEN("the registered op points to the entry")
            {
                const auto expected_count = mod_count + 1;
                REQUIRE(txn->mod_count == expected_count);

                const auto *op = last_txn_op(connection);
                const auto *head = truncate_list_head(connection);
                REQUIRE(op->type == WT_TXN_OP_FOLLOWER_TRUNCATE);
                REQUIRE(op->u.follower_truncate.t == head);
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
              &connection.session(), connection.layered_table().iface.name, &start_key, &stop_key);

            std::ignore = __wt_insert_truncate_entry(
              &connection.session(), connection.layered_table().iface.name, &start_key, &stop_key);

            THEN("both entries appear on the truncate list")
            {
                const auto *entry = truncate_list_head(connection);
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
                const auto *first = truncate_list_head(connection);
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
                const auto *first = truncate_list_head(connection);
                REQUIRE(first != nullptr);
                REQUIRE(first->start_ts == WT_TS_NONE);
                REQUIRE(first->durable_ts == WT_TS_NONE);
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
                REQUIRE(lock_is_released(connection));
            }
        }
    }
}

SCENARIO("clearing empties the truncate list", "[truncate_list][clear]")
{
    GIVEN("a follower with two truncate entries")
    {
        follower_connection connection;
        insert_n_entries(connection, 2);

        WHEN("the truncate list is cleared")
        {
            __wt_layered_table_truncate_clear(&connection.session(), &connection.layered_table());

            THEN("the truncate list is empty")
            {
                REQUIRE(truncate_list_head(connection) == nullptr);
            }
        }
    }
}

SCENARIO("clearing an empty truncate list is a no-op", "[truncate_list][clear]")
{
    GIVEN("a follower with an empty truncate list")
    {
        follower_connection connection;

        WHEN("the truncate list is cleared")
        {
            __wt_layered_table_truncate_clear(&connection.session(), &connection.layered_table());

            THEN("the truncate list is empty")
            {
                REQUIRE(truncate_list_head(connection) == nullptr);
            }
        }
    }
}

SCENARIO("clearing the truncate list releases the truncate lock", "[truncate_list][clear]")
{
    GIVEN("a follower with two truncate entries")
    {
        follower_connection connection;
        insert_n_entries(connection, 2);

        WHEN("the truncate list is cleared")
        {
            __wt_layered_table_truncate_clear(&connection.session(), &connection.layered_table());

            THEN("the truncate lock is not held")
            {
                REQUIRE(lock_is_released(connection));
            }
        }
    }
}

SCENARIO(
  "rolling back a truncate entry removes it from the truncate list", "[truncate_list][rollback]")
{
    GIVEN("a follower with one truncate entry")
    {
        follower_connection connection;
        insert_one_entry(connection);

        WHEN("the truncate is rolled back")
        {
            auto *op = last_txn_op(connection);
            REQUIRE(__wti_layered_table_truncate_rollback(&connection.session(), op) == 0);

            THEN("the entry is removed from the truncate list")
            {
                REQUIRE(truncate_list_head(connection) == nullptr);
            }
        }
    }
}

SCENARIO("rolling back a truncate entry clears the op pointer", "[truncate_list][rollback]")
{
    GIVEN("a follower with one truncate entry")
    {
        follower_connection connection;
        insert_one_entry(connection);

        WHEN("the truncate is rolled back")
        {
            auto *op = last_txn_op(connection);
            REQUIRE(__wti_layered_table_truncate_rollback(&connection.session(), op) == 0);

            THEN("the op pointer is null")
            {
                REQUIRE(op->u.follower_truncate.t == nullptr);
            }
        }
    }
}

SCENARIO("rolling back a truncate entry releases the truncate lock", "[truncate_list][rollback]")
{
    GIVEN("a follower with one truncate entry")
    {
        follower_connection connection;
        insert_one_entry(connection);

        WHEN("the truncate is rolled back")
        {
            auto *op = last_txn_op(connection);
            REQUIRE(__wti_layered_table_truncate_rollback(&connection.session(), op) == 0);

            THEN("the truncate lock is not held")
            {
                REQUIRE(lock_is_released(connection));
            }
        }
    }
}

SCENARIO("rolling back affects only the targeted entry in a multi-entry truncate list",
  "[truncate_list][rollback]")
{
    GIVEN("a follower with three truncate entries")
    {
        follower_connection connection;
        insert_n_entries(connection, 3);

        WHEN("the middle truncate is rolled back")
        {
            auto *last_op = last_txn_op(connection);
            auto *middle_op = last_op - 1;
            const auto *first_op = last_op - 2;

            const auto *first_entry = first_op->u.follower_truncate.t;
            const auto *last_entry = last_op->u.follower_truncate.t;

            REQUIRE(__wti_layered_table_truncate_rollback(&connection.session(), middle_op) == 0);

            THEN("only the middle entry is removed and the others remain in order")
            {
                const auto *head = truncate_list_head(connection);
                REQUIRE(head == first_entry);
                REQUIRE(TAILQ_NEXT(head, q) == last_entry);
                REQUIRE(TAILQ_NEXT(last_entry, q) == nullptr);
            }

            THEN("only the rolled-back op pointer is cleared")
            {
                REQUIRE(middle_op->u.follower_truncate.t == nullptr);
                REQUIRE(first_op->u.follower_truncate.t == first_entry);
                REQUIRE(last_op->u.follower_truncate.t == last_entry);
            }
        }
    }
}
