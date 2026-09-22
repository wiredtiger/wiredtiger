/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * Poisoning freed memory happens immediately before the block is handed back to the allocator,
 * where nothing can safely look at it. These tests drive the decision and the fill directly, on a
 * buffer the test still owns, so the byte pattern can be inspected without reading freed memory.
 */

#include <catch2/catch.hpp>

#include "wt_internal.h"
#include "../wrappers/connection_wrapper.h"

namespace {

#define POISON_TEST_LEN 512
#define POISON_TEST_FILL 0x5a

/*
 * all_bytes_are --
 *     Return whether every byte of a block has the given value.
 */
bool
all_bytes_are(const uint8_t *buf, size_t len, uint8_t v)
{
    for (size_t i = 0; i < len; i++)
        if (buf[i] != v)
            return (false);
    return (true);
}

} // namespace

TEST_CASE("Poison before free: fill is applied according to configuration", "[poison]")
{
    connection_wrapper conn_wrapper(".", "create");
    WT_SESSION_IMPL *session = conn_wrapper.create_session();
    WT_CONNECTION_IMPL *conn = conn_wrapper.get_wt_connection_impl();

    uint8_t *buf = nullptr;
    REQUIRE(__wt_calloc(session, 1, POISON_TEST_LEN, &buf) == 0);

    SECTION("configured on, the block is filled with the debug byte")
    {
        memset(buf, POISON_TEST_FILL, POISON_TEST_LEN);
        FLD_SET(conn->debug.flags, WT_CONN_DEBUG_OVERWRITE_FREE);

        __wt_poison_before_free(session, buf, POISON_TEST_LEN);
        CHECK(all_bytes_are(buf, POISON_TEST_LEN, WT_DEBUG_BYTE));
    }

    SECTION("configured off, the block is left alone in a release build")
    {
        memset(buf, POISON_TEST_FILL, POISON_TEST_LEN);
        FLD_CLR(conn->debug.flags, WT_CONN_DEBUG_OVERWRITE_FREE);

        __wt_poison_before_free(session, buf, POISON_TEST_LEN);
#ifdef HAVE_DIAGNOSTIC
        /* A diagnostic build poisons unconditionally, so the setting cannot turn it off. */
        CHECK(all_bytes_are(buf, POISON_TEST_LEN, WT_DEBUG_BYTE));
#else
        CHECK(all_bytes_are(buf, POISON_TEST_LEN, POISON_TEST_FILL));
#endif
    }

    SECTION("a null session is tolerated, because freeing one does not require a session")
    {
        memset(buf, POISON_TEST_FILL, POISON_TEST_LEN);

        __wt_poison_before_free(nullptr, buf, POISON_TEST_LEN);
#ifdef HAVE_DIAGNOSTIC
        CHECK(all_bytes_are(buf, POISON_TEST_LEN, WT_DEBUG_BYTE));
#else
        CHECK(all_bytes_are(buf, POISON_TEST_LEN, POISON_TEST_FILL));
#endif
    }

    __wt_free(session, buf);
}

/*
 * These macros stand in for one another at a call site, so each must evaluate its argument exactly
 * once, even where sizing the fill names the pointer a second time.
 */
TEST_CASE("Poison before free: the free macros evaluate their argument once", "[poison]")
{
    connection_wrapper conn_wrapper(".", "create");
    WT_SESSION_IMPL *session = conn_wrapper.create_session();
    WT_CONNECTION_IMPL *conn = conn_wrapper.get_wt_connection_impl();

    FLD_SET(conn->debug.flags, WT_CONN_DEBUG_OVERWRITE_FREE);

    uint8_t *bufs[2] = {nullptr, nullptr};
    REQUIRE(__wt_calloc(session, 1, POISON_TEST_LEN, &bufs[0]) == 0);
    REQUIRE(__wt_calloc(session, 1, POISON_TEST_LEN, &bufs[1]) == 0);
    memset(bufs[1], POISON_TEST_FILL, POISON_TEST_LEN);

    int i = 0;
    __wt_overwrite_and_free_len(session, bufs[i++], POISON_TEST_LEN);
    CHECK(i == 1);
    CHECK(bufs[0] == nullptr);
    REQUIRE(bufs[1] != nullptr);
    CHECK(all_bytes_are(bufs[1], POISON_TEST_LEN, POISON_TEST_FILL));
    __wt_free(session, bufs[1]);

    WT_ITEM *items[2] = {nullptr, nullptr};
    REQUIRE(__wt_calloc_one(session, &items[0]) == 0);
    REQUIRE(__wt_calloc_one(session, &items[1]) == 0);

    int j = 0;
    __wt_overwrite_and_free(session, items[j++]);
    CHECK(j == 1);
    CHECK(items[0] == nullptr);
    CHECK(items[1] != nullptr);
    __wt_free(session, items[1]);

    /* Freeing a null pointer is a no-op, matching ANSI C free semantics. */
    __wt_overwrite_and_free(session, items[0]);
    CHECK(items[0] == nullptr);
}
