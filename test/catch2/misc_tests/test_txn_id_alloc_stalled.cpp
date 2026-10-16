/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>

#include "wiredtiger.h"
#include "../wrappers/connection_wrapper.h"
#include "wt_internal.h"

/*
 * A transaction allocating its ID publishes a provisional ID, increments the global current ID and
 * publishes the final ID. A snapshot built while the allocating thread is stopped between those
 * steps must not wait for it, must not include the ID, and must make the allocation retry with an
 * ID the snapshot can't see.
 */

static constexpr const char *k_db = "test_db_txn_id_alloc_stalled";

/* Leave the slot in the state of an allocation stopped after the increment. */
static uint64_t
stall_allocation(WT_SESSION_IMPL *session)
{
    WT_TXN_GLOBAL *txn_global = &S2C(session)->txn_global;
    WT_TXN_SHARED *txn_shared = WT_SESSION_TXN_SHARED(session);
    uint64_t id;

    __wt_atomic_store_uint8_v(&txn_shared->is_allocating, WT_TXN_ID_ALLOCATING);
    __wt_atomic_store_uint64_v(&txn_shared->id, __wt_atomic_load_uint64_v(&txn_global->current));
    id = __wt_atomic_fetch_add_uint64_v(&txn_global->current, 1);
    __wt_atomic_store_uint64_v(&txn_shared->id, id);
    return (id);
}

TEST_CASE("txn: a stalled transaction ID allocation", "[txn]")
{
    connection_wrapper conn(k_db, "create,statistics=(all)");
    WT_SESSION_IMPL *writer = conn.create_session();
    WT_SESSION_IMPL *reader = conn.create_session();
    WT_TXN_GLOBAL *txn_global = &conn.get_wt_connection_impl()->txn_global;
    WT_TXN_SHARED *writer_shared = WT_SESSION_TXN_SHARED(writer);
    uint64_t stalled_id, retried_id;

    REQUIRE(writer->iface.begin_transaction(&writer->iface, nullptr) == 0);
    stalled_id = stall_allocation(writer);

    SECTION("a snapshot doesn't wait and makes the allocation retry")
    {
        REQUIRE(reader->iface.begin_transaction(&reader->iface, nullptr) == 0);
        CHECK(__wt_atomic_load_uint8_v(&writer_shared->is_allocating) == WT_TXN_ID_ALLOC_RETRY);
        CHECK(reader->txn->snapshot_data.snap_max > stalled_id);
        for (uint32_t i = 0; i < reader->txn->snapshot_data.snapshot_count; i++)
            CHECK(reader->txn->snapshot_data.snapshot[i] != stalled_id);

        /* The stalled allocation can't complete, and the retried ID is not visible. */
        CHECK_FALSE(__wt_atomic_cas_uint8_v(
          &writer_shared->is_allocating, WT_TXN_ID_ALLOCATING, WT_TXN_ID_ALLOC_DONE));
        retried_id = __wt_txn_id_alloc(writer, true);
        CHECK(retried_id > stalled_id);
        CHECK(__wt_atomic_load_uint8_v(&writer_shared->is_allocating) == WT_TXN_ID_ALLOC_DONE);
        CHECK(__wt_atomic_load_uint64_v(&writer_shared->id) == retried_id);
        CHECK_FALSE(__wt_txn_visible_id(reader, retried_id));

        REQUIRE(reader->iface.rollback_transaction(&reader->iface, nullptr) == 0);
    }

    SECTION("a snapshot includes the ID once the allocation completes")
    {
        __wt_atomic_store_uint8_v(&writer_shared->is_allocating, WT_TXN_ID_ALLOC_DONE);
        REQUIRE(reader->iface.begin_transaction(&reader->iface, nullptr) == 0);
        CHECK(reader->txn->snapshot_data.snap_max > stalled_id);
        CHECK_FALSE(__wt_txn_visible_id(reader, stalled_id));
        REQUIRE(reader->iface.rollback_transaction(&reader->iface, nullptr) == 0);
    }

    SECTION("the oldest ID scan doesn't wait and stays at or below the published ID")
    {
        REQUIRE(__wt_txn_update_oldest(reader, WT_TXN_OLDEST_STRICT | WT_TXN_OLDEST_WAIT) == 0);
        CHECK(__wt_atomic_load_uint64_v(&txn_global->last_running) <= stalled_id);
        CHECK(__wt_atomic_load_uint64_v(&txn_global->oldest_id) <= stalled_id);
    }

    /* Leave the slot as a completed allocation so the rollback clears it. */
    __wt_atomic_store_uint8_v(&writer_shared->is_allocating, WT_TXN_ID_ALLOC_DONE);
    writer->txn->time_point.id = __wt_atomic_load_uint64_v(&writer_shared->id);
    F_SET(&writer->txn->time_point, WT_TXN_TIME_POINT_HAS_ID);
    REQUIRE(writer->iface.rollback_transaction(&writer->iface, nullptr) == 0);
}
