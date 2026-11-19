/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * [block_handle_type]: block_open.c, block_disagg_open.c
 *
 * The connection's block lists hold both local and disaggregated handles as WT_BLOCK pointers, so
 * each lookup must check the handle type before reading a type-specific field or adopting the
 * handle. These tests fabricate one handle of each type with the same name in one bucket and check
 * that each lookup only matches its own type.
 */

#include <catch2/catch.hpp>
#include <cstddef>
#include <cstdint>
#include <cstring>

#include "wt_internal.h"
#include "../wrappers/connection_wrapper.h"

namespace {

constexpr const char *BLOCK_NAME = "block_handle_type";
constexpr uint32_t OBJECT_ID = 7;

/* A page log whose handle open always fails. */
int
stub_open_handle_error(WT_PAGE_LOG *, WT_SESSION *, uint64_t, WT_PAGE_LOG_HANDLE **)
{
    return (EINVAL);
}

WT_PAGE_LOG_HANDLE stub_plh{};

int
stub_plh_close(WT_PAGE_LOG_HANDLE *, WT_SESSION *)
{
    return (0);
}

int
stub_open_handle_ok(WT_PAGE_LOG *, WT_SESSION *, uint64_t, WT_PAGE_LOG_HANDLE **plhp)
{
    stub_plh.plh_close = stub_plh_close;
    *plhp = &stub_plh;
    return (0);
}

/*
 * The disaggregated table ID must fall inside a local handle, so a fabricated local handle can
 * offer a matching table ID to a lookup that skips the type check.
 */
static_assert(offsetof(WT_BLOCK_DISAGG, tableid) + sizeof(uint64_t) <= sizeof(WT_BLOCK),
  "the disagg table ID must fall inside a local handle");

struct block_handle_type_fixture {
    connection_wrapper conn_wrapper;
    WT_CONNECTION_IMPL *conn;
    WT_SESSION_IMPL *session;
    WT_BLOCK local{};
    WT_BLOCK_DISAGG disagg{};
    WT_BTREE btree{};
    WT_DATA_HANDLE dhandle{};
    WT_PAGE_LOG page_log{};
    WT_DATA_HANDLE *saved_dhandle;
    uint64_t bucket;

    block_handle_type_fixture() : conn_wrapper("WT_TEST.block_handle_type", "create")
    {
        conn = conn_wrapper.get_wt_connection_impl();
        session = conn_wrapper.create_session();

        /* Give the session a btree whose table ID the disaggregated lookup matches. */
        btree.id = 42;
        dhandle.handle = &btree;
        saved_dhandle = session->dhandle;
        session->dhandle = &dhandle;

        local.type = WT_BLOCK_TYPE_LOCAL;
        local.name = BLOCK_NAME;
        local.objectid = OBJECT_ID;
        local.ref = 1;

        disagg.type = WT_BLOCK_TYPE_DISAGG;
        disagg.name = BLOCK_NAME;
        /* Match the local object ID and table ID, so a type-blind lookup false-matches. */
        disagg.objectid = OBJECT_ID;
        disagg.tableid = btree.id;
        disagg.ref = 1;

        const uint64_t tableid = btree.id;
        memcpy(reinterpret_cast<uint8_t *>(&local) + offsetof(WT_BLOCK_DISAGG, tableid), &tableid,
          sizeof(tableid));

        const uint64_t hash = __wt_hash_city64(BLOCK_NAME, strlen(BLOCK_NAME));
        bucket = hash & (conn->hash_size - 1);
        link_handles(false);
    }

    ~block_handle_type_fixture()
    {
        unlink_handles();
        session->dhandle = saved_dhandle;
    }

    /* Link both handles; the flag puts the disaggregated handle at the head of the bucket. */
    void
    link_handles(bool disagg_at_head)
    {
        __wt_spin_lock(session, &conn->block_lock);
        if (disagg_at_head) {
            WT_CONN_BLOCK_INSERT(conn, &local, bucket);
            WT_CONN_BLOCK_INSERT(conn, reinterpret_cast<WT_BLOCK *>(&disagg), bucket);
        } else {
            WT_CONN_BLOCK_INSERT(conn, reinterpret_cast<WT_BLOCK *>(&disagg), bucket);
            WT_CONN_BLOCK_INSERT(conn, &local, bucket);
        }
        __wt_spin_unlock(session, &conn->block_lock);
    }

    void
    unlink_handles()
    {
        __wt_spin_lock(session, &conn->block_lock);
        WT_CONN_BLOCK_REMOVE(conn, &local, bucket);
        WT_CONN_BLOCK_REMOVE(conn, reinterpret_cast<WT_BLOCK *>(&disagg), bucket);
        __wt_spin_unlock(session, &conn->block_lock);
    }
};

} // namespace

TEST_CASE_METHOD(
  block_handle_type_fixture, "block handle type discriminator", "[block_handle_type]")
{
    WT_BLOCK *block = nullptr;

    /*
     * The local handle is at the bucket head and its fabricated table ID aliases the disaggregated
     * field. The disaggregated lookup must skip it and find the disaggregated handle.
     */
    REQUIRE(__wti_block_disagg_open(session, BLOCK_NAME, nullptr, false, false, &block) == 0);
    CHECK(block == reinterpret_cast<WT_BLOCK *>(&disagg));
    CHECK(disagg.ref == 2);
    CHECK(local.ref == 1);

    /* Reverse the bucket order: the local lookup must now skip the disaggregated handle. */
    unlink_handles();
    link_handles(true);
    block = nullptr;
    REQUIRE(__wt_block_open(session, BLOCK_NAME, OBJECT_ID, nullptr, false, false, false, 0,
              nullptr, &block) == 0);
    CHECK(block == &local);
    CHECK(local.ref == 2);
    CHECK(disagg.ref == 2);
}

TEST_CASE_METHOD(block_handle_type_fixture, "block manager open cleans up after a page log failure",
  "[block_handle_type]")
{
    /*
     * A failed page log open leaves the block handle unset. The guard in __bmd_close is only
     * observable in diagnostic builds: without it, the cast asserts on the NULL handle.
     */
    page_log.pl_open_handle = stub_open_handle_error;
    btree.page_log = &page_log;

    WT_BM *bm = nullptr;
    CHECK(__wt_block_disagg_manager_open(
            session, "file:block_handle_type_failure", nullptr, false, false, &bm) == EINVAL);
    CHECK(bm == nullptr);
}

TEST_CASE_METHOD(
  block_handle_type_fixture, "disagg block manager open tags the new handle", "[block_handle_type]")
{
    page_log.pl_open_handle = stub_open_handle_ok;
    btree.page_log = &page_log;

    WT_BM *bm = nullptr;
    REQUIRE(__wt_block_disagg_manager_open(
              session, "file:block_handle_type_ok", nullptr, false, false, &bm) == 0);
    CHECK(WT_BLOCK_IS_DISAGG(bm->block));
    CHECK(__wt_block_as_disagg(session, bm->block)->tableid == btree.id);
    REQUIRE(bm->close(bm, session) == 0);
}
