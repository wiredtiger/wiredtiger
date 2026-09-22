/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <algorithm>
#include <cstdint>
#include <catch2/catch.hpp>
#include "wt_internal.h"

#include "../wrappers/connection_wrapper.h"

#ifdef HAVE_MEM_TRACK

static int64_t
session_bytes(WT_CONNECTION_IMPL *conn, WT_SESSION_IMPL *session)
{
    return (__wt_atomic_load_int64_relaxed(&conn->stats[session->stat_conn_bucket]->memory_bytes));
}

static uint64_t
published_bytes(WT_SESSION *session)
{
    WT_CURSOR *cursor;
    const char *desc;
    uint64_t value;

    REQUIRE(session->open_cursor(session, "statistics:", nullptr, nullptr, &cursor) == 0);
    cursor->set_key(cursor, WT_STAT_CONN_MEMORY_BYTES);
    REQUIRE(cursor->search(cursor) == 0);
    REQUIRE(cursor->get_value(cursor, &desc, nullptr, &value) == 0);
    REQUIRE(cursor->close(cursor) == 0);
    return (value);
}

TEST_CASE("memory tracking: gauge follows the requested size", "[memory]")
{
    const std::string home = "WT_TEST.mem_track";
    connection_wrapper conn(home, "create,statistics=(fast)");
    WT_CONNECTION_IMPL *conn_impl = conn.get_wt_connection_impl();
    WT_SESSION_IMPL *session_impl = conn.create_session();
    WT_SESSION *session = &session_impl->iface;

    SECTION("malloc and free")
    {
        const size_t bytes = 4096;
        int64_t before = session_bytes(conn_impl, session_impl);
        void *p = nullptr;

        REQUIRE(__wt_malloc(session_impl, bytes, &p) == 0);
        CHECK(reinterpret_cast<uintptr_t>(p) % alignof(std::max_align_t) == 0);
        memset(p, 0xab, bytes);
        CHECK(session_bytes(conn_impl, session_impl) - before == (int64_t)bytes);
        CHECK(published_bytes(session) >= bytes);

        __wt_free(session_impl, p);
        CHECK(p == nullptr);
        CHECK(session_bytes(conn_impl, session_impl) == before);
    }

    SECTION("calloc")
    {
        const size_t bytes = 64;
        int64_t before = session_bytes(conn_impl, session_impl);
        void *p = nullptr;

        REQUIRE(__wt_calloc(session_impl, 1, bytes, &p) == 0);
        auto raw = static_cast<const uint8_t *>(p);
        CHECK(std::all_of(raw, raw + bytes, [](uint8_t b) { return b == 0; }));
        CHECK(session_bytes(conn_impl, session_impl) - before == (int64_t)bytes);
        __wt_free(session_impl, p);
        CHECK(session_bytes(conn_impl, session_impl) == before);
    }

    SECTION("realloc grows by the added bytes")
    {
        size_t sz = 0;
        int64_t before = session_bytes(conn_impl, session_impl);
        void *p = nullptr;

        REQUIRE(__wt_realloc(session_impl, &sz, 100, &p) == 0);
        CHECK(sz == 100);
        CHECK(session_bytes(conn_impl, session_impl) - before == 100);
        REQUIRE(__wt_realloc(session_impl, &sz, 180, &p) == 0);
        CHECK(sz == 180);
        CHECK(session_bytes(conn_impl, session_impl) - before == 180);
        __wt_free(session_impl, p);
        CHECK(session_bytes(conn_impl, session_impl) == before);
    }

    SECTION("realloc without a caller size uses the tracked size")
    {
        int64_t before = session_bytes(conn_impl, session_impl);
        void *p = nullptr;

        REQUIRE(__wt_realloc_noclear(session_impl, nullptr, 40, &p) == 0);
        CHECK(session_bytes(conn_impl, session_impl) - before == 40);
        REQUIRE(__wt_realloc_noclear(session_impl, nullptr, 90, &p) == 0);
        CHECK(session_bytes(conn_impl, session_impl) - before == 90);
        __wt_free(session_impl, p);
        CHECK(session_bytes(conn_impl, session_impl) == before);
    }

    SECTION("realloc forces a new allocation")
    {
        struct flag_guard {
            WT_CONNECTION_IMPL *conn;
            explicit flag_guard(WT_CONNECTION_IMPL *c) : conn(c)
            {
                FLD_SET(conn->debug.flags, WT_CONN_DEBUG_REALLOC_MALLOC);
            }
            ~flag_guard()
            {
                FLD_CLR(conn->debug.flags, WT_CONN_DEBUG_REALLOC_MALLOC);
            }
        } guard(conn_impl);
        size_t sz = 0;
        int64_t before = session_bytes(conn_impl, session_impl);
        void *p = nullptr;

        REQUIRE(__wt_realloc(session_impl, &sz, 128, &p) == 0);
        CHECK(session_bytes(conn_impl, session_impl) - before == 128);
        REQUIRE(__wt_realloc(session_impl, &sz, 256, &p) == 0);
        CHECK(sz == 256);
        CHECK(session_bytes(conn_impl, session_impl) - before == 256);
        __wt_free(session_impl, p);
        CHECK(session_bytes(conn_impl, session_impl) == before);
    }

    SECTION("statistics clear leaves the gauge")
    {
        const size_t bytes = 512;
        WT_CURSOR *cursor;
        int64_t before = session_bytes(conn_impl, session_impl);
        int64_t held;
        void *p = nullptr;

        REQUIRE(__wt_malloc(session_impl, bytes, &p) == 0);
        held = session_bytes(conn_impl, session_impl);
        CHECK(held - before == (int64_t)bytes);
        REQUIRE(session->open_cursor(
                  session, "statistics:", nullptr, "statistics=(clear)", &cursor) == 0);
        cursor->set_key(cursor, WT_STAT_CONN_MEMORY_BYTES);
        REQUIRE(cursor->search(cursor) == 0);
        REQUIRE(cursor->close(cursor) == 0);
        CHECK(session_bytes(conn_impl, session_impl) == held);
        __wt_free(session_impl, p);
        CHECK(session_bytes(conn_impl, session_impl) == before);
    }

    SECTION("NULL session bytes fold into the connection")
    {
        const size_t bytes = 2048;
        int64_t before, after;
        void *p = nullptr;

        __wt_mem_track_fold_null(session_impl);
        before = __wt_atomic_load_int64_relaxed(&conn_impl->stats[0]->memory_bytes);
        REQUIRE(__wt_malloc(nullptr, bytes, &p) == 0);
        CHECK(__wt_atomic_load_int64_relaxed(&conn_impl->stats[0]->memory_bytes) == before);
        __wt_mem_track_fold_null(session_impl);
        after = __wt_atomic_load_int64_relaxed(&conn_impl->stats[0]->memory_bytes);
        CHECK(after - before == (int64_t)bytes);
        __wt_free(nullptr, p);
        __wt_mem_track_fold_null(session_impl);
        CHECK(__wt_atomic_load_int64_relaxed(&conn_impl->stats[0]->memory_bytes) == before);
    }
}

#endif
