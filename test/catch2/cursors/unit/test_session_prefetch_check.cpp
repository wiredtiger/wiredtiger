/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>

#include "wt_internal.h"
#include "../../utils.h"
#include "../../wrappers/connection_wrapper.h"

/*
 * [test_session_prefetch_check]: session_prefetch.c
 * Test the precedence between a cursor's pre-fetch setting, passed in the read flags, and the
 * session's pre-fetch setting, and pin which checks the cursor setting bypasses.
 */

TEST_CASE("Session check: __wt_session_prefetch_check", "[prefetch]")
{
    connection_wrapper conn(DB_HOME, "create,prefetch=(available=true,default=false)");
    WT_SESSION_IMPL *session = conn.create_session();
    WT_SESSION *wt_session = &session->iface;

    REQUIRE(wt_session->create(wt_session, "file:prefetch_check", "key_format=S,value_format=S") == 0);
    WT_CURSOR *cursor = nullptr;
    REQUIRE(wt_session->open_cursor(wt_session, "file:prefetch_check", nullptr, nullptr, &cursor) == 0);
    cursor->set_key(cursor, "k1");
    cursor->set_value(cursor, "v1");
    REQUIRE(cursor->insert(cursor) == 0);
    REQUIRE(cursor->reset(cursor) == 0);
    REQUIRE(cursor->next(cursor) == 0);

    /*
     * Use the positioned cursor's leaf page, and give the session enough sequential disk reads that
     * only the on/off gates decide the outcome. The API resets the session's data handle when next
     * returns, so set and later restore it by hand.
     */
    WT_CURSOR_BTREE *cbt = (WT_CURSOR_BTREE *)cursor;
    WT_REF *ref = cbt->ref;
    WT_DATA_HANDLE *saved_dhandle = session->dhandle;
    session->dhandle = cbt->dhandle;
    session->pf.prefetch_disk_read_count = 2;

    /* Without a cursor setting, the session's setting decides. */
    REQUIRE(__wt_session_prefetch_check(session, ref, 0) == false);
    F_SET(session, WT_SESSION_PREFETCH_ENABLED);
    REQUIRE(__wt_session_prefetch_check(session, ref, 0) == true);

    /* The cursor's setting overrides the session's in both directions. */
    REQUIRE(__wt_session_prefetch_check(session, ref, WT_READ_PREFETCH_DISABLED) == false);
    F_CLR(session, WT_SESSION_PREFETCH_ENABLED);
    REQUIRE(__wt_session_prefetch_check(session, ref, WT_READ_PREFETCH_ENABLED) == true);

    /*
     * The cursor setting only passes the on/off gate: the sequential-read heuristic still applies
     * to a cursor that enables pre-fetching.
     */
    session->pf.prefetch_disk_read_count = 0;
    REQUIRE(__wt_session_prefetch_check(session, ref, WT_READ_PREFETCH_ENABLED) == false);
    session->pf.prefetch_disk_read_count = 2;

    /* A disabled cursor wins even inside a declared scan. */
    session->pf.scan_hint = true;
    F_SET(session, WT_SESSION_PREFETCH_ENABLED);
    REQUIRE(__wt_session_prefetch_check(session, ref, 0) == true);
    REQUIRE(__wt_session_prefetch_check(session, ref, WT_READ_PREFETCH_DISABLED) == false);
    session->pf.scan_hint = false;

    session->dhandle = saved_dhandle;

    REQUIRE(cursor->close(cursor) == 0);
    REQUIRE(wt_session->close(wt_session, nullptr) == 0);
}
