#include <array>
#include <cerrno>
#include <cstdlib>
#include <memory>
#include <utility>

#define CATCH_CONFIG_MAIN
#include <catch2/catch.hpp>

#include "fake_mtx_rw.h"
#include "fake_os_alloc.h"
#include "fake_scratch.h"
#include "fake_session_dhandle.h"
#include "fake_txn.h"
#include "mock_session.h"
#include "utils.h"

namespace {

using truncate_range = std::pair<WT_ITEM, WT_ITEM>;

[[nodiscard]] auto
make_key(const std::string_view value) -> WT_ITEM
{
    WT_ITEM item{};
    item.data = value.data();
    item.size = value.size() + 1;
    return item;
}

[[nodiscard]] auto
as_view(const WT_ITEM &item) -> std::string_view
{
    return {static_cast<const char *>(item.data), item.size};
}

[[nodiscard]] auto
truncate_list_head(WT_LAYERED_TABLE &table) -> WT_TRUNCATE *
{
    return TAILQ_FIRST(&table.truncateqh);
}

[[nodiscard]] auto
truncate_list_size(const WT_LAYERED_TABLE &table) -> size_t
{
    size_t count = 0;
    WT_TRUNCATE *entry = nullptr;
    TAILQ_FOREACH (entry, &table.truncateqh, q)
        ++count;
    return count;
}

WT_TXN_OP captured_txn_op{};

void
make_calloc_fail()
{
    __wt_calloc_fake.custom_fake = nullptr;
    __wt_calloc_fake.return_val = ENOMEM;
}

void
make_buf_set_fail()
{
    __wt_buf_set_fake.custom_fake = nullptr;
    __wt_buf_set_fake.return_val = ENOMEM;
}

void
make_buf_set_fail_on_stop_key()
{
    static std::array<int, 2> results = {0, ENOMEM};
    __wt_buf_set_fake.custom_fake = nullptr;
    SET_RETURN_SEQ(__wt_buf_set, results.data(), results.size());
}

void
make_txn_next_op_fail()
{
    __wt_txn_next_op_fake.custom_fake = nullptr;
    __wt_txn_next_op_fake.return_val = ENOMEM;
}

auto
txn_next_op_passthrough(WT_SESSION_IMPL *, WT_TXN_OP **opp) -> int
{
    captured_txn_op = {};
    *opp = &captured_txn_op;
    return 0;
}

class scoped_fast_truncate_enable {
public:
    scoped_fast_truncate_enable() : _previous(__wt_process.disagg_fast_truncate_2026)
    {
        __wt_process.disagg_fast_truncate_2026 = true;
    }

    ~scoped_fast_truncate_enable()
    {
        __wt_process.disagg_fast_truncate_2026 = _previous;
    }

private:
    bool _previous;
};

class txn_deleter {
public:
    void
    operator()(WT_TXN *p) const
    {
        std::free(p);
    }
};

class truncate_list_fixture {
protected:
    truncate_list_fixture()
    {
        reset_fakes();
        setup_session();
        setup_table();
    }

    ~truncate_list_fixture()
    {
        // __wt_buf_free is faked: restore stubs so the cleanup loop below works.
        reset_fakes();

        const bool had_data = !TAILQ_EMPTY(&_layered_table.truncateqh);

        WT_TRUNCATE *entry = nullptr;
        while ((entry = TAILQ_FIRST(&_layered_table.truncateqh)) != nullptr) {
            TAILQ_REMOVE(&_layered_table.truncateqh, entry, q);
            __wt_buf_free(_session, &entry->start_key);
            __wt_buf_free(_session, &entry->stop_key);
            __wt_free(_session, entry);
        }

        if (had_data)
            WT_DHANDLE_RELEASE(&_layered_table.iface);
    }

    [[nodiscard]] auto
    insert_one_entry(std::string_view start, std::string_view stop) -> int
    {
        auto start_key = make_key(start);
        auto stop_key = make_key(stop);
        return __wt_insert_truncate_entry(_session, &_layered_table, &start_key, &stop_key);
    }

    [[nodiscard]] auto
    insert_n_entries(size_t count) -> int
    {
        for (size_t i = 0; i < count; ++i)
            WT_RET(insert_one_entry("a", "z"));

        return 0;
    }

    [[nodiscard]] auto
    layered_table() -> WT_LAYERED_TABLE &
    {
        return _layered_table;
    }

    [[nodiscard]] auto
    reference_count() const -> uint32_t
    {
        return __wt_atomic_load_uint32_relaxed(&_layered_table.iface.references);
    }

    [[nodiscard]] auto
    session() -> WT_SESSION_IMPL *
    {
        return _session;
    }

    [[nodiscard]] auto
    txn() -> WT_TXN *
    {
        return _txn.get();
    }

private:
    void
    reset_fakes()
    {
        // os_alloc must be first: build_test_mock_session calls __wt_calloc.
        // FIXME: remove mock dependency on WT allocation functions.
        reset_os_alloc_fakes();
        reset_scratch_fakes();
        reset_session_dhandle_fakes();
        reset_mtx_rw_fakes();
        reset_txn_fakes();
        __wt_txn_next_op_fake.custom_fake = txn_next_op_passthrough;
    }

    void
    setup_session()
    {
        _mock_session = mock_session::build_test_mock_session();
        _session = _mock_session->get_wt_session_impl();

        auto *heap_txn = static_cast<WT_TXN *>(std::calloc(1, sizeof(WT_TXN)));
        _txn.reset(heap_txn);
        _txn->time_point.id = 1;
        _session->txn = _txn.get();
    }

    void
    setup_table()
    {
        TAILQ_INIT(&_layered_table.truncateqh);
        F_SET(&_layered_table.iface, WT_DHANDLE_OPEN);
    }

    scoped_fast_truncate_enable _enable;
    std::shared_ptr<mock_session> _mock_session;
    WT_SESSION_IMPL *_session = nullptr;
    mutable WT_LAYERED_TABLE _layered_table = {};

    // WT_TXN ends with a flexible array member, so it must be heap-allocated.
    std::unique_ptr<WT_TXN, txn_deleter> _txn;
};

} // namespace

SCENARIO_METHOD(
  truncate_list_fixture, "inserting an entry successfully returns 0", "[truncate_list][insert]")
{
    GIVEN("an empty truncate list")
    {
        WHEN("an entry is inserted")
        {
            const auto result = insert_one_entry("aaa", "zzz");

            THEN("it returns 0")
            {
                REQUIRE(result == 0);
            }
        }
    }
}

SCENARIO_METHOD(truncate_list_fixture,
  "inserting an entry inserts one entry into the truncate list", "[truncate_list][insert]")
{
    GIVEN("an empty truncate list")
    {
        WHEN("an entry is inserted")
        {
            CHECK(insert_one_entry("aaa", "zzz") == 0);

            THEN("exactly one entry appears on the truncate list")
            {
                REQUIRE(truncate_list_size(layered_table()) == 1);
            }
        }
    }
}

SCENARIO_METHOD(truncate_list_fixture, "inserting an entry increments the dhandle reference count",
  "[truncate_list][insert]")
{
    GIVEN("an empty truncate list")
    {
        const auto initial_reference_count = reference_count();

        WHEN("an entry is inserted")
        {
            CHECK(insert_one_entry("aaa", "zzz") == 0);

            THEN("the dhandle reference count is incremented by one")
            {
                REQUIRE(reference_count() == initial_reference_count + 1);
            }
        }
    }
}

SCENARIO_METHOD(truncate_list_fixture, "inserting an entry preserves the session dhandle",
  "[truncate_list][insert]")
{
    GIVEN("an empty truncate list")
    {
        const auto *const initial_dhandle = session()->dhandle;

        WHEN("an entry is inserted")
        {
            CHECK(insert_one_entry("aaa", "zzz") == 0);

            THEN("the session dhandle is unchanged after the call")
            {
                REQUIRE(session()->dhandle == initial_dhandle);
            }
        }
    }
}

SCENARIO_METHOD(truncate_list_fixture,
  "inserting an entry stores a bounded range when both keys are provided",
  "[truncate_list][insert]")
{
    GIVEN("an empty truncate list")
    {
        WHEN("an entry is inserted with a start and a stop key")
        {
            auto start_key = make_key("a");
            auto stop_key = make_key("z");

            CHECK(
              __wt_insert_truncate_entry(session(), &layered_table(), &start_key, &stop_key) == 0);

            THEN("the truncate list entry contains both keys")
            {
                const auto *const first = truncate_list_head(layered_table());
                REQUIRE(first != nullptr);

                REQUIRE(as_view(first->start_key) == as_view(start_key));
                REQUIRE(as_view(first->stop_key) == as_view(stop_key));
            }
        }
    }
}

SCENARIO_METHOD(truncate_list_fixture, "inserting multiple entries stores them in insertion order",
  "[truncate_list][insert]")
{
    GIVEN("an empty truncate list")
    {
        WHEN("multiple entries are inserted in order")
        {
            std::array<truncate_range, 3> keys{
              truncate_range{make_key("a"), make_key("b")},
              truncate_range{make_key("c"), make_key("d")},
              truncate_range{make_key("e"), make_key("f")},
            };

            for (auto &[start, stop] : keys)
                CHECK(__wt_insert_truncate_entry(session(), &layered_table(), &start, &stop) == 0);

            THEN("the entries appear on the truncate list in insertion order")
            {
                REQUIRE(truncate_list_size(layered_table()) == keys.size());

                const auto *entry = truncate_list_head(layered_table());

                for (const auto &[start, stop] : keys) {
                    REQUIRE(entry != nullptr);
                    REQUIRE(as_view(entry->start_key) == as_view(start));
                    REQUIRE(as_view(entry->stop_key) == as_view(stop));
                    entry = TAILQ_NEXT(entry, q);
                }

                REQUIRE(entry == nullptr);
            }
        }
    }
}

SCENARIO_METHOD(truncate_list_fixture,
  "inserting multiple entries into an empty list increments the dhandle reference count by one",
  "[truncate_list][insert]")
{
    GIVEN("an empty truncate list")
    {
        const auto initial_reference_count = reference_count();

        WHEN("multiple entries are inserted")
        {
            const auto num_entries = 4;
            CHECK(insert_n_entries(num_entries) == 0);

            THEN("the dhandle reference count is incremented by exactly one")
            {
                REQUIRE(reference_count() == initial_reference_count + 1);
            }
        }
    }
}

SCENARIO_METHOD(truncate_list_fixture,
  "inserting an entry into a non-empty truncate list does not change the dhandle reference count",
  "[truncate_list][insert]")
{
    GIVEN("a truncate list with one entry")
    {
        CHECK(insert_one_entry("a", "z") == 0);
        const auto initial_reference_count = reference_count();

        WHEN("an entry is inserted")
        {
            CHECK(insert_one_entry("a", "z") == 0);

            THEN("the dhandle reference count is unchanged")
            {
                REQUIRE(reference_count() == initial_reference_count);
            }
        }
    }
}

SCENARIO_METHOD(truncate_list_fixture, "inserting an entry registers a follower-truncate op",
  "[truncate_list][insert]")
{
    GIVEN("an empty truncate list")
    {
        WHEN("an entry is inserted")
        {
            CHECK(insert_one_entry("a", "z") == 0);

            THEN("exactly one txn op is registered")
            {
                REQUIRE(__wt_txn_next_op_fake.call_count == 1);
            }

            THEN("the registered op is a follower-truncate pointing to the list head")
            {
                REQUIRE(captured_txn_op.type == WT_TXN_OP_FOLLOWER_TRUNCATE);

                const auto *const head = truncate_list_head(layered_table());
                REQUIRE(captured_txn_op.u.follower_truncate.t == head);
            }
        }
    }
}

SCENARIO_METHOD(truncate_list_fixture,
  "inserting two entries with identical keys adds both to the truncate list",
  "[truncate_list][insert]")
{
    GIVEN("an empty truncate list")
    {
        WHEN("two entries with the same keys are inserted")
        {
            const auto num_entries = 2;

            for (size_t i = 0; i < num_entries; ++i) {
                CHECK(insert_one_entry("a", "z") == 0);
            }

            THEN("both entries appear on the truncate list")
            {
                REQUIRE(truncate_list_size(layered_table()) == num_entries);
            }
        }
    }
}

SCENARIO_METHOD(truncate_list_fixture,
  "inserting an entry stamps it with the inserting transaction id", "[truncate_list][insert]")
{
    GIVEN("an empty truncate list and a transaction with a known id")
    {
        txn()->time_point.id = 42;

        WHEN("an entry is inserted")
        {
            CHECK(insert_one_entry("a", "z") == 0);

            THEN("the entry txn_id matches the transaction id")
            {
                const auto *const first = truncate_list_head(layered_table());
                REQUIRE(first != nullptr);
                REQUIRE(first->txn_id == txn()->time_point.id);
                REQUIRE(first->txn_id != WT_TXN_NONE);
            }
        }
    }
}

SCENARIO_METHOD(truncate_list_fixture, "inserting an entry records the originating layered table",
  "[truncate_list][insert]")
{
    GIVEN("an empty truncate list")
    {
        WHEN("an entry is inserted")
        {
            CHECK(insert_one_entry("a", "z") == 0);

            THEN("the entry references the layered table it was inserted on")
            {
                const auto *const first = truncate_list_head(layered_table());
                REQUIRE(first != nullptr);
                REQUIRE(first->layered_table == &layered_table());
            }
        }
    }
}

SCENARIO_METHOD(truncate_list_fixture, "inserting an entry acquires and releases the truncate lock",
  "[truncate_list][insert]")
{
    GIVEN("an empty truncate list")
    {
        WHEN("an entry is inserted")
        {
            CHECK(insert_one_entry("a", "z") == 0);

            THEN("the truncate lock is acquired and released exactly once")
            {
                REQUIRE(__wt_writelock_fake.call_count == 1);
                REQUIRE(__wt_writeunlock_fake.call_count == 1);
            }
        }
    }
}

SCENARIO_METHOD(truncate_list_fixture, "inserting an entry propagates allocation failure",
  "[truncate_list][insert][error]")
{
    GIVEN("an empty truncate list")
    {
        WHEN("memory allocation fails during entry insertion")
        {
            make_calloc_fail();
            const auto result = insert_one_entry("a", "z");

            THEN("it returns ENOMEM")
            {
                REQUIRE(result == ENOMEM);
            }

            AND_THEN("no entry is added to the truncate list")
            {
                REQUIRE(truncate_list_size(layered_table()) == 0);
            }
        }
    }
}

SCENARIO_METHOD(truncate_list_fixture, "inserting an entry propagates start key copy failure",
  "[truncate_list][insert][error]")
{
    GIVEN("an empty truncate list")
    {
        WHEN("copying the start key fails")
        {
            make_buf_set_fail();
            const auto result = insert_one_entry("a", "z");

            THEN("it returns ENOMEM")
            {
                REQUIRE(result == ENOMEM);
            }

            AND_THEN("no entry is added to the truncate list")
            {
                REQUIRE(truncate_list_size(layered_table()) == 0);
            }
        }
    }
}

SCENARIO_METHOD(truncate_list_fixture, "inserting an entry propagates stop key copy failure",
  "[truncate_list][insert][error]")
{
    GIVEN("an empty truncate list")
    {
        WHEN("copying the stop key fails")
        {
            make_buf_set_fail_on_stop_key();
            const auto result = insert_one_entry("a", "z");

            THEN("it returns ENOMEM")
            {
                REQUIRE(result == ENOMEM);
            }

            AND_THEN("no entry is added to the truncate list")
            {
                REQUIRE(truncate_list_size(layered_table()) == 0);
            }
        }
    }
}

SCENARIO_METHOD(
  truncate_list_fixture, "inserting an entry copies the keys correctly", "[truncate_list][insert]")
{
    GIVEN("an empty truncate list")
    {
        WHEN("an entry is inserted with a start and stop key")
        {
            auto start_key = make_key("abc");
            auto stop_key = make_key("yz");

            CHECK(
              __wt_insert_truncate_entry(session(), &layered_table(), &start_key, &stop_key) == 0);

            THEN("the start key is copied correctly")
            {
                const auto *const first = truncate_list_head(layered_table());
                REQUIRE(first != nullptr);

                REQUIRE(__wt_buf_set_fake.arg1_history[0] == &first->start_key);
                REQUIRE(__wt_buf_set_fake.arg2_history[0] == start_key.data);
                REQUIRE(__wt_buf_set_fake.arg3_history[0] == start_key.size);
            }

            THEN("the stop key is copied correctly")
            {
                const auto *const first = truncate_list_head(layered_table());
                REQUIRE(first != nullptr);

                REQUIRE(__wt_buf_set_fake.arg1_history[1] == &first->stop_key);
                REQUIRE(__wt_buf_set_fake.arg2_history[1] == stop_key.data);
                REQUIRE(__wt_buf_set_fake.arg3_history[1] == stop_key.size);
            }
        }
    }
}

SCENARIO_METHOD(truncate_list_fixture, "inserting an entry releases the session dhandle on success",
  "[truncate_list][insert]")
{
    GIVEN("an empty truncate list")
    {
        WHEN("an entry is inserted")
        {
            CHECK(insert_one_entry("a", "z") == 0);

            THEN("the session dhandle is released exactly once")
            {
                REQUIRE(__wt_session_release_dhandle_fake.call_count == 1);
            }
        }
    }
}

SCENARIO_METHOD(truncate_list_fixture,
  "inserting an entry releases the session dhandle on post-acquisition error",
  "[truncate_list][insert][error]")
{
    GIVEN("an empty truncate list")
    {
        WHEN("txn op registration fails")
        {
            make_txn_next_op_fail();
            CHECK(insert_one_entry("a", "z") != 0);

            THEN("the session dhandle is released exactly once")
            {
                REQUIRE(__wt_session_release_dhandle_fake.call_count == 1);
            }

            AND_THEN("no entry is added to the truncate list")
            {
                REQUIRE(truncate_list_size(layered_table()) == 0);
            }
        }
    }
}

SCENARIO_METHOD(truncate_list_fixture, "inserting an entry acquires the ingest dhandle",
  "[truncate_list][insert]")
{
    GIVEN("an empty truncate list with an ingest URI set")
    {
        layered_table().ingest_uri = "layered:test";

        WHEN("an entry is inserted")
        {
            CHECK(insert_one_entry("a", "z") == 0);

            THEN("the ingest dhandle is acquired")
            {
                REQUIRE(__wt_session_get_dhandle_fake.call_count == 1);
                REQUIRE(__wt_session_get_dhandle_fake.arg1_val == layered_table().ingest_uri);
            }
        }
    }
}

SCENARIO_METHOD(truncate_list_fixture, "inserting an entry propagates dhandle acquisition failure",
  "[truncate_list][insert][error]")
{
    GIVEN("an empty truncate list")
    {
        WHEN("the ingest dhandle cannot be obtained during insertion")
        {
            __wt_session_get_dhandle_fake.return_val = WT_NOTFOUND;
            const auto result = insert_one_entry("a", "z");

            THEN("it returns WT_NOTFOUND")
            {
                REQUIRE(result == WT_NOTFOUND);
            }

            AND_THEN("no entry is added to the truncate list")
            {
                REQUIRE(truncate_list_size(layered_table()) == 0);
            }

            AND_THEN("the session dhandle is not released")
            {
                REQUIRE(__wt_session_release_dhandle_fake.call_count == 0);
            }
        }
    }
}
