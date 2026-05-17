#include <memory>
#include <string_view>

#define CATCH_CONFIG_MAIN
#include <catch2/catch.hpp>

#include "fake_cur_layered.h"
#include "fake_os_alloc.h"
#include "fake_txn_truncate.h"
#include "mock_session.h"

namespace {

WT_ITEM
make_key()
{
    constexpr std::string_view sv = "key123";

    WT_ITEM item{};
    item.data = sv.data();
    item.size = sv.size() + 1;

    return item;
}

WT_ITEM
make_value()
{
    constexpr std::string_view sv = "value123";

    WT_ITEM item{};
    item.data = sv.data();
    item.size = sv.size() + 1;

    return item;
}

} // namespace

bool
operator==(const WT_ITEM &lhs, const WT_ITEM &rhs)
{
    return (lhs.size == rhs.size) && (lhs.data == rhs.data) && (lhs.memsize == rhs.memsize) &&
      (lhs.mem == rhs.mem);
}

class layered_cursor_fixture {
protected:
    layered_cursor_fixture()
    {
        reset_fakes();

        mock_session = mock_session::build_test_mock_session();
        session = mock_session->get_wt_session_impl();

        wire_cursors();
        set_follower();
    }

    void
    reset_fakes()
    {
        // os_alloc must be first: build_test_mock_session calls __wt_calloc.
        // FIXME: remove once mock_session no longer depends on WT allocation functions.
        reset_os_alloc_fakes();
        reset_txn_truncate_fakes();
        reset_cur_layered_fakes();
    }

    void
    wire_cursors()
    {
        ingest.set_key = ingest_set_key;
        ingest.set_value = ingest_set_value;
        ingest.get_value = ingest_get_value;
        ingest.search = ingest_search;
        ingest.insert = ingest_insert;
        ingest.update = ingest_update;
        ingest.remove = ingest_remove;
        ingest.reset = ingest_reset;

        stable_cursor.set_key = stable_set_key;
        stable_cursor.set_value = stable_set_value;
        stable_cursor.get_value = stable_get_value;
        stable_cursor.search = stable_search;
        stable_cursor.insert = stable_insert;
        stable_cursor.update = stable_update;
        stable_cursor.remove = stable_remove;
        stable_cursor.reset = stable_reset;

        layered.ingest_cursor = &ingest;
        layered.stable_cursor = &stable_cursor;
        layered.current_cursor = nullptr;

        layered.iface.session = reinterpret_cast<WT_SESSION *>(session);
        layered.iface.key_format = "S";
        layered.iface.value_format = "u";
    }

    void
    set_follower()
    {
        S2C(session)->layered_table_manager.leader = false;
    }

    void
    set_leader()
    {
        S2C(session)->layered_table_manager.leader = true;
    }

    std::shared_ptr<mock_session> mock_session;
    WT_SESSION_IMPL *session = nullptr;
    WT_CURSOR ingest = {};
    WT_CURSOR stable_cursor = {};
    WT_CURSOR_LAYERED layered = {};
};

SCENARIO("clayered_deleted correctly identifies tombstone values", "[layered_cursor][tombstone]")
{
    GIVEN("a WT_ITEM")
    {
        WT_ITEM item{};

        WHEN("the item is zero-length")
        {
            item.data = nullptr;
            item.size = 0;

            THEN("it is not considered deleted")
            {
                REQUIRE(__wt_clayered_deleted(&item) == false);
            }
        }

        WHEN("the item contains the exact two-byte tombstone value")
        {
            item.data = "\x14\x14";
            item.size = 2;

            THEN("it is considered deleted")
            {
                REQUIRE(__wt_clayered_deleted(&item) == true);
            }
        }

        WHEN("the item has tombstone bytes followed by a trailing byte")
        {
            item.data = "\x14\x14\x01";
            item.size = 3;

            THEN("it is not considered deleted")
            {
                REQUIRE(__wt_clayered_deleted(&item) == false);
            }
        }

        WHEN("the item has the right size but wrong bytes")
        {
            item.data = "\x14\x15";
            item.size = 2;

            THEN("it is not considered deleted")
            {
                REQUIRE(__wt_clayered_deleted(&item) == false);
            }
        }
    }
}

SCENARIO_METHOD(layered_cursor_fixture,
  "lookup_constituent correctly searches a constituent cursor for a key",
  "[layered_cursor][lookup]")
{
    GIVEN("a layered cursor with a key set")
    {
        auto *iface = &layered.iface;
        iface->key = make_key();

        WHEN("any search outcome occurs")
        {
            const auto outcome = GENERATE(0, WT_NOTFOUND, WT_PANIC);
            ingest_search_fake.return_val = outcome;

            const auto ret = __clayered_lookup_constituent(&ingest, &layered, nullptr);

            THEN("the key is forwarded to the constituent cursor")
            {
                REQUIRE(ingest_set_key_item_fake.arg1_val == iface->key);
            }
        }

        WHEN("any unsuccessful search outcome occurs")
        {
            const auto outcome = GENERATE(WT_NOTFOUND, WT_PANIC);
            ingest_search_fake.return_val = outcome;

            const auto ret = __clayered_lookup_constituent(&ingest, &layered, nullptr);

            THEN("the current cursor is not updated")
            {
                REQUIRE(layered.current_cursor == nullptr);
            }
        }

        WHEN("the constituent cursor finds the key")
        {
            ingest_search_fake.return_val = 0;
            ingest_get_value_item_fake.return_val = make_value();

            WT_ITEM value{};
            const auto ret = __clayered_lookup_constituent(&ingest, &layered, &value);

            THEN("0 is returned")
            {
                REQUIRE(ret == 0);
            }

            AND_THEN("the current cursor is updated to the constituent cursor")
            {
                REQUIRE(layered.current_cursor == &ingest);
            }

            AND_THEN("the value is retrieved from the constituent cursor")
            {
                REQUIRE(value == ingest_get_value_item_fake.return_val);
            }
        }

        WHEN("the constituent cursor does not find the key")
        {
            ingest_search_fake.return_val = WT_NOTFOUND;
            const auto ret = __clayered_lookup_constituent(&ingest, &layered, nullptr);

            THEN("WT_NOTFOUND is returned")
            {
                REQUIRE(ret == WT_NOTFOUND);
            }
        }

        WHEN("a hard error occurs during the search")
        {
            ingest_search_fake.return_val = WT_PANIC;
            const auto ret = __clayered_lookup_constituent(&ingest, &layered, nullptr);

            THEN("the error is returned")
            {
                REQUIRE(ret == WT_PANIC);
            }
        }

        WHEN("search succeeds but there is an error getting the value")
        {
            ingest_search_fake.return_val = 0;
            ingest_get_value_fake.return_val = WT_ROLLBACK;

            const auto ret = __clayered_lookup_constituent(&ingest, &layered, nullptr);

            THEN("the error is returned")
            {
                REQUIRE(ret == WT_ROLLBACK);
            }
        }
    }
}

SCENARIO_METHOD(layered_cursor_fixture,
  "clayered_put forwards the caller's key to the correct constituent cursor",
  "[layered_cursor][put]")
{
    const auto op = GENERATE(WT_CLAYERED_PUT_INSERT, WT_CLAYERED_PUT_UPDATE);

    GIVEN("a follower cursor")
    {
        set_follower();

        WHEN("a write operation is performed")
        {
            WT_ITEM key = make_key();
            WT_ITEM value = make_value();

            CHECK(__clayered_put(session, &layered, &key, &value, op) == 0);

            THEN("the caller's key is forwarded to the ingest cursor")
            {
                REQUIRE(ingest_set_key_item_fake.arg1_val == key);
            }
        }
    }

    GIVEN("a leader cursor")
    {
        set_leader();

        WHEN("a write operation is performed")
        {
            WT_ITEM key = make_key();
            WT_ITEM value = make_value();

            CHECK(__clayered_put(session, &layered, &key, &value, op) == 0);

            THEN("the caller's key is forwarded to the stable cursor")
            {
                REQUIRE(stable_set_key_item_fake.arg1_val == key);
            }
        }
    }
}

SCENARIO_METHOD(layered_cursor_fixture,
  "clayered_put dispatches the correct operation to the constituent cursor",
  "[layered_cursor][put]")
{
    GIVEN("a follower cursor")
    {
        WHEN("the op is INSERT")
        {
            WT_ITEM key = make_key();
            WT_ITEM value = make_value();

            CHECK(__clayered_put(session, &layered, &key, &value, WT_CLAYERED_PUT_INSERT) == 0);

            THEN("an insert is performed on the constituent cursor")
            {
                REQUIRE(ingest_insert_fake.call_count == 1);
            }
        }

        WHEN("the op is UPDATE")
        {
            WT_ITEM key = make_key();
            WT_ITEM value = make_value();

            CHECK(__clayered_put(session, &layered, &key, &value, WT_CLAYERED_PUT_UPDATE) == 0);

            THEN("an update is performed on the constituent cursor")
            {
                REQUIRE(ingest_update_fake.call_count == 1);
            }
        }
    }
}

SCENARIO_METHOD(layered_cursor_fixture,
  "clayered_put forwards the caller's value to the constituent cursor for write operations",
  "[layered_cursor][put]")
{
    const auto op = GENERATE(WT_CLAYERED_PUT_INSERT, WT_CLAYERED_PUT_UPDATE);

    GIVEN("a follower cursor")
    {
        WHEN("a write operation is performed")
        {
            WT_ITEM key = make_key();
            WT_ITEM value = make_value();

            CHECK(__clayered_put(session, &layered, &key, &value, op) == 0);

            THEN("the caller's value is forwarded to the constituent cursor")
            {
                REQUIRE(ingest_set_value_item_fake.arg1_val == value);
            }
        }
    }
}

SCENARIO_METHOD(layered_cursor_fixture,
  "clayered_put establishes cursor position for non-insert operations", "[layered_cursor][put]")
{
    GIVEN("a follower cursor")
    {
        set_follower();

        WHEN("an UPDATE succeeds")
        {
            WT_ITEM key = make_key();
            WT_ITEM value = make_value();

            CHECK(__clayered_put(session, &layered, &key, &value, WT_CLAYERED_PUT_UPDATE) == 0);

            THEN("current_cursor is the ingest cursor")
            {
                REQUIRE(layered.current_cursor == layered.ingest_cursor);
            }
        }
    }

    GIVEN("a leader cursor")
    {
        set_leader();

        WHEN("an UPDATE succeeds")
        {
            WT_ITEM key = make_key();
            WT_ITEM value = make_value();

            CHECK(__clayered_put(session, &layered, &key, &value, WT_CLAYERED_PUT_UPDATE) == 0);

            THEN("current_cursor is the stable cursor")
            {
                REQUIRE(layered.current_cursor == layered.stable_cursor);
            }
        }
    }

    GIVEN("a cursor in any role")
    {
        const auto leader = GENERATE(false, true);

        if (leader)
            set_leader();
        else
            set_follower();

        WHEN("an INSERT succeeds")
        {
            WT_ITEM key = make_key();
            WT_ITEM value = make_value();

            CHECK(__clayered_put(session, &layered, &key, &value, WT_CLAYERED_PUT_INSERT) == 0);

            THEN("current_cursor is not updated")
            {
                REQUIRE(layered.current_cursor == nullptr);
            }
        }
    }
}

SCENARIO_METHOD(layered_cursor_fixture,
  "clayered_put performs follower-specific setup before writing", "[layered_cursor][put]")
{
    GIVEN("a follower cursor with the stable cursor positioned")
    {
        F_SET(&stable_cursor, WT_CURSTD_KEY_SET);

        mock_session->get_mock_connection();

        WHEN("a write operation is issued")
        {
            WT_ITEM key = make_key();
            WT_ITEM value = make_value();

            CHECK(__clayered_put(session, &layered, &key, &value, WT_CLAYERED_PUT_INSERT) == 0);

            THEN("the stable cursor is reset before the write")
            {
                REQUIRE(stable_reset_fake.call_count == 1);
            }
        }
    }

    GIVEN("a follower cursor")
    {
        WHEN("conflict detection rejects the write")
        {
            __wt_layered_table_truncate_detect_write_conflict_fake.return_val = WT_ROLLBACK;

            WT_ITEM key = make_key();
            WT_ITEM value = make_value();

            const auto ret =
              __clayered_put(session, &layered, &key, &value, WT_CLAYERED_PUT_INSERT);

            THEN("the conflict error is returned to the caller")
            {
                REQUIRE(ret == WT_ROLLBACK);
            }
        }
    }
}

SCENARIO_METHOD(layered_cursor_fixture,
  "clayered_put propagates errors from the constituent write operation", "[layered_cursor][put]")
{
    GIVEN("a follower cursor")
    {
        WHEN("the constituent write fails")
        {
            ingest_insert_fake.return_val = WT_PANIC;

            WT_ITEM key = make_key();
            WT_ITEM value = make_value();

            const auto ret =
              __clayered_put(session, &layered, &key, &value, WT_CLAYERED_PUT_INSERT);

            THEN("the error is returned to the caller")
            {
                REQUIRE(ret == WT_PANIC);
            }

            AND_THEN("current_cursor is not updated")
            {
                REQUIRE(layered.current_cursor == nullptr);
            }
        }
    }
}

SCENARIO_METHOD(layered_cursor_fixture,
  "clayered_remove_leader forwards the key when the cursor is not positioned",
  "[layered_cursor][remove_leader]")
{
    GIVEN("a leader cursor that is not positioned")
    {
        set_leader();

        WHEN("remove_leader is called")
        {
            WT_ITEM key = make_key();
            CHECK(__clayered_remove_leader(session, &layered, &key, false) == 0);

            THEN("the key is forwarded to the stable cursor")
            {
                REQUIRE(stable_set_key_item_fake.arg1_val == key);
            }

            AND_THEN("remove is called on the stable cursor")
            {
                REQUIRE(stable_remove_fake.call_count == 1);
            }
        }
    }
}

SCENARIO_METHOD(layered_cursor_fixture,
  "clayered_remove_leader skips set_key when the cursor is already positioned",
  "[layered_cursor][remove_leader]")
{
    GIVEN("a leader cursor that is positioned on the stable cursor")
    {
        set_leader();
        F_SET(&stable_cursor, WT_CURSTD_KEY_INT);

        WHEN("remove_leader is called")
        {
            WT_ITEM key = make_key();
            CHECK(__clayered_remove_leader(session, &layered, &key, true) == 0);

            THEN("set_key is not called")
            {
                REQUIRE(stable_set_key_fake.call_count == 0);
            }

            AND_THEN("remove is still called on the stable cursor")
            {
                REQUIRE(stable_remove_fake.call_count == 1);
            }
        }
    }
}

SCENARIO_METHOD(layered_cursor_fixture,
  "clayered_remove_leader sets current_cursor and propagates errors",
  "[layered_cursor][remove_leader]")
{
    GIVEN("a leader cursor")
    {
        set_leader();

        WHEN("remove succeeds")
        {
            WT_ITEM key = make_key();
            const auto ret = __clayered_remove_leader(session, &layered, &key, false);

            THEN("current_cursor is set to the stable cursor")
            {
                REQUIRE(ret == 0);
                REQUIRE(layered.current_cursor == &stable_cursor);
            }
        }

        WHEN("remove fails")
        {
            stable_remove_fake.return_val = WT_PANIC;
            WT_ITEM key = make_key();
            const auto ret = __clayered_remove_leader(session, &layered, &key, false);

            THEN("the error is returned to the caller")
            {
                REQUIRE(ret == WT_PANIC);
            }
        }
    }
}
