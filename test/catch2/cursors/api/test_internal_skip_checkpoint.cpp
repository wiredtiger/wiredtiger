/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#ifndef _WIN32

#include <catch2/catch.hpp>

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "../../wrappers/connection_wrapper.h"

namespace {

class temporary_home {
public:
    temporary_home()
    {
        if (mkdtemp(path) == nullptr)
            throw std::runtime_error("Cannot create the checkpoint test directory");
    }

    ~temporary_home()
    {
        std::filesystem::remove_all(path);
    }

    char path[40] = "internal_skip_checkpoint.XXXXXX";
};

/* Pause a real checkpoint after clearing the tree flag, before visiting any pages. */
class checkpoint_pause {
public:
    checkpoint_pause(WT_BM *bm, WT_SESSION *session)
        : _bm(bm), _original(bm->checkpoint_start)
    {
        _active = this;
        _bm->checkpoint_start = checkpoint_start;
        _thread = std::thread([this, session] { result = session->checkpoint(session, nullptr); });
    }

    ~checkpoint_pause()
    {
        finish();
    }

    bool
    wait()
    {
        std::unique_lock<std::mutex> lock(_mutex);
        return _cv.wait_for(lock, std::chrono::seconds(30), [this] { return _reached; });
    }

    void
    finish()
    {
        {
            std::lock_guard<std::mutex> lock(_mutex);
            _released = true;
        }
        _cv.notify_all();
        if (_thread.joinable())
            _thread.join();
        _bm->checkpoint_start = _original;
        _active = nullptr;
    }

    int result = 0;

private:
    static int
    checkpoint_start(WT_BM *bm, WT_SESSION_IMPL *session)
    {
        checkpoint_pause *pause = _active;
        int ret = pause->_original(bm, session);
        if (ret != 0)
            return ret;
        std::unique_lock<std::mutex> lock(pause->_mutex);
        pause->_reached = true;
        pause->_cv.notify_all();
        if (!pause->_cv.wait_for(
              lock, std::chrono::seconds(30), [pause] { return pause->_released; }))
            return ETIMEDOUT;
        return 0;
    }

    static checkpoint_pause *_active;
    WT_BM *_bm;
    int (*_original)(WT_BM *, WT_SESSION_IMPL *);
    std::mutex _mutex;
    std::condition_variable _cv;
    bool _reached = false;
    bool _released = false;
    std::thread _thread;
};

checkpoint_pause *checkpoint_pause::_active = nullptr;

} // namespace

TEST_CASE("Cursor preserves reinserts while checkpoint clears the tree modified flag",
  "[cursor][internal_skip_checkpoint]")
{
    bool forward = GENERATE(true, false);
    CAPTURE(forward);
    const char *uri = "table:internal_skip_checkpoint";
    temporary_home home;
    connection_wrapper wrapper(home.path,
      "create,cache_size=1GB,statistics=(all),precise_checkpoint=true,"
      "checkpoint_cleanup=(wait=100000),"
      "extensions=[./ext/page_log/palite/libwiredtiger_palite.so],"
      "disaggregated=(role=leader,page_log=palite,lose_all_my_data=true)");
    WT_CONNECTION *conn = wrapper.get_wt_connection();
    WT_SESSION_IMPL *session_impl = wrapper.create_session();
    WT_SESSION *session = &session_impl->iface;
    REQUIRE(session->create(session, uri,
              "key_format=i,value_format=S,block_manager=disagg,log=(enabled=false),"
              "allocation_size=512,leaf_page_max=512,internal_page_max=512,"
              "memory_page_max=4096") == 0);
    REQUIRE(conn->set_timestamp(conn, "oldest_timestamp=1") == 0);

    WT_CURSOR *cursor;
    REQUIRE(session->open_cursor(session, uri, nullptr, nullptr, &cursor) == 0);
    REQUIRE(session->begin_transaction(session, nullptr) == 0);
    std::string value(50, 'a');
    for (int key = 1; key <= 1000; ++key) {
        cursor->set_key(cursor, key);
        cursor->set_value(cursor, value.c_str());
        REQUIRE(cursor->insert(cursor) == 0);
    }
    REQUIRE(session->commit_transaction(session, "commit_timestamp=a") == 0);
    REQUIRE(conn->set_timestamp(conn, "stable_timestamp=a") == 0);
    REQUIRE(session->checkpoint(session, nullptr) == 0);
    REQUIRE(cursor->close(cursor) == 0);

    REQUIRE(session->open_cursor(session, uri, nullptr, "debug=(release_evict)", &cursor) == 0);
    REQUIRE(session->begin_transaction(session, nullptr) == 0);
    for (int key = 1; key <= 1000; ++key) {
        cursor->set_key(cursor, key);
        REQUIRE(cursor->search(cursor) == 0);
        REQUIRE(cursor->reset(cursor) == 0);
    }
    REQUIRE(session->rollback_transaction(session, nullptr) == 0);
    REQUIRE(cursor->close(cursor) == 0);

    WT_CURSOR *stop;
    REQUIRE(session->open_cursor(session, uri, nullptr, nullptr, &cursor) == 0);
    REQUIRE(session->open_cursor(session, uri, nullptr, nullptr, &stop) == 0);
    REQUIRE(session->begin_transaction(session, nullptr) == 0);
    cursor->set_key(cursor, 100);
    stop->set_key(stop, 900);
    REQUIRE(session->truncate(session, nullptr, cursor, stop, nullptr) == 0);
    REQUIRE(session->commit_transaction(session, "commit_timestamp=14") == 0);
    REQUIRE(cursor->close(cursor) == 0);
    REQUIRE(stop->close(stop) == 0);
    REQUIRE(conn->set_timestamp(conn, "stable_timestamp=14") == 0);
    REQUIRE(session->checkpoint(session, nullptr) == 0);

    REQUIRE(session->open_cursor(session, uri, nullptr, nullptr, &cursor) == 0);
    REQUIRE(session->begin_transaction(session, nullptr) == 0);
    cursor->set_key(cursor, 500);
    cursor->set_value(cursor, "new value");
    REQUIRE(cursor->insert(cursor) == 0);
    REQUIRE(session->commit_transaction(session, "commit_timestamp=18") == 0);

    /* Keep the updated leaf pinned so its parent cannot be evicted during the checkpoint. */
    REQUIRE(session->begin_transaction(session, "read_timestamp=19") == 0);
    cursor->set_key(cursor, 500);
    REQUIRE(cursor->search(cursor) == 0);
    WT_CURSOR_BTREE *cbt = reinterpret_cast<WT_CURSOR_BTREE *>(cursor);
    WT_BTREE *btree = static_cast<WT_BTREE *>(cbt->dhandle->handle);
    WT_REF *leaf = cbt->ref;
    WT_REF *parent = leaf->home->pg_intl_parent_ref;
    REQUIRE(parent != &btree->root);
    REQUIRE(WT_REF_GET_STATE(parent) == WT_REF_MEM);
    REQUIRE_FALSE(__wt_page_is_modified(parent->page));
    REQUIRE(__wt_page_is_modified(leaf->page));
    REQUIRE(btree->modified);
    REQUIRE_FALSE(F_ISSET_ATOMIC_32(btree, WT_BTREE_READONLY));

    WT_SESSION *reader = &wrapper.create_session()->iface;
    WT_CURSOR *scan;
    REQUIRE(reader->open_cursor(reader, uri, nullptr, nullptr, &scan) == 0);
    WT_SESSION *checkpointer = &wrapper.create_session()->iface;
    checkpoint_pause pause(btree->bm, checkpointer);
    REQUIRE(pause.wait());
    REQUIRE_FALSE(btree->modified);
    REQUIRE_FALSE(__wt_page_is_modified(parent->page));
    REQUIRE(__wt_page_is_modified(leaf->page));

    session_impl->dhandle = cbt->dhandle;
    WT_ADDR_COPY addr;
    WT_TIME_AGGREGATE *ta = nullptr;
    bool copied;
    WT_ENTER_PAGE_INDEX(session_impl);
    copied = __wt_ref_addr_copy(session_impl, parent, &addr);
    if (!copied && __wt_get_page_modify_ta(session_impl, parent->page, &ta)) {
        WT_TIME_AGGREGATE_COPY(&addr.ta, ta);
        copied = true;
    }
    WT_LEAVE_PAGE_INDEX(session_impl);
    session_impl->dhandle = nullptr;
    REQUIRE(copied);
    REQUIRE(WT_TIME_AGGREGATE_HAS_STOP(&addr.ta));
    REQUIRE(addr.ta.newest_stop_ts == 20);

    REQUIRE(reader->begin_transaction(reader, "read_timestamp=19") == 0);
    std::vector<int> keys;
    int ret;
    while ((ret = (forward ? scan->next(scan) : scan->prev(scan))) == 0) {
        int key;
        REQUIRE(scan->get_key(scan, &key) == 0);
        keys.push_back(key);
        if (key == 500) {
            const char *found;
            REQUIRE(scan->get_value(scan, &found) == 0);
            REQUIRE(std::string(found) == "new value");
        }
    }
    REQUIRE(ret == WT_NOTFOUND);
    REQUIRE(reader->rollback_transaction(reader, nullptr) == 0);
    REQUIRE(scan->close(scan) == 0);
    REQUIRE(session->rollback_transaction(session, nullptr) == 0);
    REQUIRE(cursor->close(cursor) == 0);
    pause.finish();
    REQUIRE(pause.result == 0);

    std::vector<int> expected;
    for (int key = 1; key <= 1000; ++key)
        if (key < 100 || key > 900 || key == 500)
            expected.push_back(key);
    if (!forward)
        std::reverse(expected.begin(), expected.end());
    REQUIRE(keys == expected);
}

#endif