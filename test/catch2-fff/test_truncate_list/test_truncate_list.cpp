#include <cstdlib>
#include <memory>

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

[[nodiscard]] auto
make_key(const std::string_view value) -> WT_ITEM
{
    WT_ITEM item{};
    item.data = value.data();
    item.size = value.size() + 1;
    return item;
}

WT_TXN_OP captured_txn_op{};

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

private:
    void
    reset_fakes()
    {
        // os_alloc must be first: build_test_mock_session calls __wt_calloc.
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
    WT_LAYERED_TABLE _layered_table = {};

    // WT_TXN ends with a flexible array member, so it must be heap-allocated.
    std::unique_ptr<WT_TXN, txn_deleter> _txn;
};

} // namespace

SCENARIO_METHOD(
  truncate_list_fixture, "adding an entry successfully returns 0", "[truncate_list][insert]")
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
