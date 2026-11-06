/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <algorithm>
#include <catch2/catch.hpp>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "wt_internal.h"
#include "../wrappers/connection_wrapper.h"

/*
 * Before the eviction server sleeps it releases the walk points it holds. These tests call that
 * release directly with the eviction pass lock held, so the eviction server cannot run alongside
 * them, and check the state it leaves behind: no walk point, no hazard pointer in the walk session,
 * no end count on the saved walk tree, no saved walk tree, and no tree visited after the last walk
 * point.
 */

namespace {

const int NTABLES = 10;

/*
 * Hold the eviction pass lock for the lifetime of this object, so a failed check cannot leave it
 * held: the eviction threads take it to stop, and closing the connection would then hang.
 */
struct pass_lock {
    WT_SESSION_IMPL *session;

    explicit pass_lock(WT_SESSION_IMPL *s) : session(s)
    {
        __wt_spin_lock(session, &S2C(session)->evict->evict_pass_lock);
        FLD_SET(session->lock_flags, WT_SESSION_LOCKED_PASS);
    }

    ~pass_lock()
    {
        FLD_CLR(session->lock_flags, WT_SESSION_LOCKED_PASS);
        __wt_spin_unlock(session, &S2C(session)->evict->evict_pass_lock);
    }

    pass_lock(const pass_lock &) = delete;
    pass_lock &operator=(const pass_lock &) = delete;
};

/*
 * A database directory of its own, emptied before the test and removed after the connection closes,
 * so the test's tables never meet files from an earlier run.
 */
struct test_home {
    std::string path;

    explicit test_home(const char *p) : path(p)
    {
        std::filesystem::remove_all(path);
    }

    ~test_home()
    {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }
};

struct walk_test {
    /* Declared before the connection so it is removed after the connection closes. */
    test_home home{"WT_TEST.evict_clear_walks"};
    connection_wrapper conn_wrapper{
      home.path, "create,statistics=(all),eviction=(evict_use_softptr=false)"};
    WT_SESSION_IMPL *session = nullptr;
    WT_CONNECTION_IMPL *conn = nullptr;
    WT_EVICT *evict = nullptr;
    std::vector<WT_CURSOR *> cursors;
    std::vector<WT_DATA_HANDLE *> dhandles;
    /* Declared last so it is released first, including when the constructor fails. */
    std::unique_ptr<pass_lock> locked;

    walk_test()
    {
        WT_SESSION *wt_session;
        WT_CONNECTION *wt_conn = conn_wrapper.get_wt_connection();
        REQUIRE(wt_conn->open_session(wt_conn, nullptr, nullptr, &wt_session) == 0);
        session = (WT_SESSION_IMPL *)wt_session;
        conn = S2C(session);
        evict = conn->evict;

        /* Small trees with a resident leaf page under the root, each kept open by a cursor. */
        for (int i = 0; i < NTABLES; ++i) {
            std::string uri = "file:test_evict_clear_walks" + std::to_string(i) + ".wt";
            WT_CURSOR *cursor;
            REQUIRE(
              wt_session->create(wt_session, uri.c_str(), "key_format=i,value_format=S") == 0);
            REQUIRE(
              wt_session->open_cursor(wt_session, uri.c_str(), nullptr, nullptr, &cursor) == 0);
            for (int k = 1; k <= 10; ++k) {
                cursor->set_key(cursor, k);
                cursor->set_value(cursor, "value");
                REQUIRE(cursor->insert(cursor) == 0);
            }
            REQUIRE(cursor->reset(cursor) == 0);
            cursors.push_back(cursor);
            dhandles.push_back(((WT_CURSOR_BTREE *)cursor)->dhandle);
        }

        /* Keep the eviction server out, as it is whenever the release runs. */
        locked.reset(new pass_lock(session));

        /* Start from a clean state in case the server left anything behind. */
        clear_all();
        REQUIRE(hazards() == 0);
        for (WT_DATA_HANDLE *dhandle : dhandles)
            ((WT_BTREE *)dhandle->handle)->evict_walk_ends = 0;
    }

    ~walk_test()
    {
        locked.reset();
        for (WT_CURSOR *cursor : cursors)
            (void)cursor->close(cursor);
    }

    /* The release runs with the handle list lock held, as the eviction server calls it. */
    void
    clear_all()
    {
        __wt_readlock(session, &conn->dhandle_lock);
        int ret = __wti_evict_clear_all_walks_and_saved_tree(session);
        __wt_readunlock(session, &conn->dhandle_lock);
        REQUIRE(ret == 0);
    }

    uint32_t
    hazards() const
    {
        return (__wt_atomic_load_uint32_relaxed(&evict->walk_session->hazards.num_active));
    }

    /* Position of a handle on the connection's handle list, counting from the head. */
    int64_t
    position(WT_DATA_HANDLE *target)
    {
        WT_DATA_HANDLE *dhandle;
        int64_t pos = 0;

        __wt_readlock(session, &conn->dhandle_lock);
        TAILQ_FOREACH (dhandle, &conn->dhqh, q) {
            if (dhandle == target)
                break;
            ++pos;
        }
        __wt_readunlock(session, &conn->dhandle_lock);
        REQUIRE(dhandle == target);
        return (pos);
    }

    /*
     * Leave a walk point in a tree the way the eviction walk does: the walk session walks to a leaf
     * page and keeps its hazard pointer, and the tree remembers the page.
     */
    void
    make_walk_point(WT_DATA_HANDLE *dhandle)
    {
        WT_SESSION_IMPL *walk_session = evict->walk_session;
        WT_BTREE *btree = (WT_BTREE *)dhandle->handle;
        WT_REF *ref = nullptr;
        int ret;

        WT_WITH_DHANDLE(walk_session, dhandle,
          ret = __wt_tree_walk(walk_session, &ref,
            WT_READ_EVICT_WALK_FLAGS | WT_READ_SKIP_INTL | WT_READ_VISIBLE_ALL));
        REQUIRE(ret == 0);
        REQUIRE(ref != nullptr);
        __wt_atomic_store_ptr_relaxed(&btree->evict_ref, ref);
    }
};

WT_BTREE *
btree_of(WT_DATA_HANDLE *dhandle)
{
    return ((WT_BTREE *)dhandle->handle);
}

} // namespace

TEST_CASE("Releasing walk points resets the end count of the saved walk tree", "[evict]")
{
    walk_test t;

    /*
     * The scan stopped on a tree it had walked to the end: the tree is the saved walk tree and has
     * an end count, but holds no walk point, so nothing makes the release visit it.
     */
    WT_DATA_HANDLE *saved = t.dhandles[NTABLES / 2];
    btree_of(saved)->evict_walk_ends = 2;
    __wti_evict_set_saved_walk_tree(t.session, saved);
    REQUIRE(btree_of(saved)->evict_ref == nullptr);
    REQUIRE(t.hazards() == 0);

    t.clear_all();

    CHECK(btree_of(saved)->evict_walk_ends == 0);
    CHECK(t.evict->walk_tree == nullptr);
}

TEST_CASE("Releasing walk points stops after the last walk point", "[evict]")
{
    walk_test t;

    /*
     * Walk points in three trees; the release has to reach the one furthest down the list. Newer
     * handles go to the head of the list, so the first table created lies beyond all of them.
     */
    std::vector<WT_DATA_HANDLE *> walked = {
      t.dhandles[NTABLES - 1], t.dhandles[NTABLES - 4], t.dhandles[NTABLES - 7]};
    int64_t last = 0;
    for (WT_DATA_HANDLE *dhandle : walked) {
        t.make_walk_point(dhandle);
        btree_of(dhandle)->evict_walk_ends = 1;
        last = std::max(last, t.position(dhandle));
    }
    REQUIRE(t.hazards() == walked.size());

    /*
     * Visiting a tree resets its end count, so an end count on a tree past the last walk point
     * shows whether the release went on after it.
     */
    WT_DATA_HANDLE *beyond = t.dhandles[0];
    REQUIRE(t.position(beyond) > last);
    btree_of(beyond)->evict_walk_ends = 1;

    t.clear_all();

    for (WT_DATA_HANDLE *dhandle : walked) {
        CHECK(btree_of(dhandle)->evict_ref == nullptr);
        CHECK(btree_of(dhandle)->evict_walk_ends == 0);
    }
    CHECK(t.hazards() == 0);
    CHECK(btree_of(beyond)->evict_walk_ends == 1);
}
