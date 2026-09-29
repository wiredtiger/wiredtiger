/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * Diff two checkpoints of a btree and check the differences against the contents of the two
 * checkpoints read through checkpoint cursors.
 */

#include <catch2/catch.hpp>

#include <algorithm>
#include <cinttypes>
#include <cstring>
#include <filesystem>
#include <map>
#include <optional>
#include <random>
#include <string>
#include <vector>

#include "wiredtiger.h"
#include "wt_internal.h"
#include "../wrappers/connection_wrapper.h"
#ifndef _WIN32
#include "layered_disagg_utils.h"
#endif

namespace {

const char *const uri = "file:diff.wt";
const char *const precise_cfg = "create,cache_size=200MB,precise_checkpoint=true,statistics=(fast)";

struct difference {
    WT_BTREE_DIFF_TYPE type;
    std::string key;
    std::optional<std::string> old_value;
    std::optional<std::string> new_value;

    bool
    operator==(const difference &other) const
    {
        return (type == other.type && key == other.key && old_value == other.old_value &&
          new_value == other.new_value);
    }
};

std::ostream &
operator<<(std::ostream &os, const difference &d)
{
    return (os << "{type " << d.type << ", key " << d.key << "}");
}

WT_ITEM
make_item(const std::string &s)
{
    WT_ITEM item;
    memset(&item, 0, sizeof(item));
    item.data = s.data();
    item.size = s.size();
    return (item);
}

/* Remove the whole home directory, not only the files the shared cleanup helper knows about. */
void
cleanup(const std::string &home)
{
    std::filesystem::remove_all(home);
}

std::string
make_key(uint64_t n)
{
    char buf[32];
    snprintf(buf, sizeof(buf), "key%010" PRIu64, n);
    return (buf);
}

std::string
ts_config(const char *name, uint64_t ts)
{
    char buf[64];
    snprintf(buf, sizeof(buf), "%s=%" PRIx64, name, ts);
    return (buf);
}

/* Read an object through a cursor, as the raw bytes the diff returns. */
std::map<std::string, std::string>
read_contents(WT_SESSION *session, const std::string &object, const std::string &cfg)
{
    std::map<std::string, std::string> contents;
    WT_CURSOR *cursor;
    WT_ITEM key, value;
    int ret;

    REQUIRE(
      session->open_cursor(session, object.c_str(), nullptr, (cfg + ",raw").c_str(), &cursor) == 0);
    while ((ret = cursor->next(cursor)) == 0) {
        REQUIRE(cursor->get_key(cursor, &key) == 0);
        REQUIRE(cursor->get_value(cursor, &value) == 0);
        contents.emplace(std::string(static_cast<const char *>(key.data), key.size),
          std::string(static_cast<const char *>(value.data), value.size));
    }
    REQUIRE(ret == WT_NOTFOUND);
    REQUIRE(cursor->close(cursor) == 0);
    return (contents);
}

/* A table in a timestamped database. Each checkpoint captures everything committed so far. */
class timestamped_table {
public:
    explicit timestamped_table(connection_wrapper &conn, uint64_t ts = 1)
        : _conn(conn.get_wt_connection()), _session_impl(conn.create_session()),
          _session(&_session_impl->iface), _ts(ts)
    {
        /* Set the initial timestamps only in a new database. */
        if (_ts == 1)
            REQUIRE(_conn->set_timestamp(_conn, "oldest_timestamp=1,stable_timestamp=1") == 0);

        /* Small pages make a tree several levels deep. */
        REQUIRE(_session->create(_session, uri,
                  "key_format=u,value_format=u,allocation_size=512B,leaf_page_max=4KB,"
                  "internal_page_max=512B,leaf_value_max=1KB") == 0);
        REQUIRE(_session->open_cursor(_session, uri, nullptr, nullptr, &_cursor) == 0);
    }

    ~timestamped_table()
    {
        _cursor->close(_cursor);
    }

    WT_SESSION_IMPL *
    session_impl() const
    {
        return (_session_impl);
    }

    uint64_t
    ts() const
    {
        return (_ts);
    }

    void
    begin()
    {
        REQUIRE(_session->begin_transaction(_session, nullptr) == 0);
    }

    void
    commit()
    {
        REQUIRE(_session->commit_transaction(
                  _session, ts_config("commit_timestamp", ++_ts).c_str()) == 0);
    }

    void
    put(const std::string &key, const std::string &value)
    {
        WT_ITEM k = make_item(key), v = make_item(value);
        _cursor->set_key(_cursor, &k);
        _cursor->set_value(_cursor, &v);
        REQUIRE(_cursor->insert(_cursor) == 0);
    }

    void
    remove(const std::string &key)
    {
        WT_ITEM k = make_item(key);
        _cursor->set_key(_cursor, &k);
        REQUIRE(_cursor->remove(_cursor) == 0);
    }

    void
    truncate(const std::string &start, const std::string &stop)
    {
        WT_CURSOR *start_cursor, *stop_cursor;
        WT_ITEM start_item = make_item(start), stop_item = make_item(stop);

        REQUIRE(_session->open_cursor(_session, uri, nullptr, nullptr, &start_cursor) == 0);
        REQUIRE(_session->open_cursor(_session, uri, nullptr, nullptr, &stop_cursor) == 0);
        start_cursor->set_key(start_cursor, &start_item);
        stop_cursor->set_key(stop_cursor, &stop_item);

        begin();
        REQUIRE(_session->truncate(_session, nullptr, start_cursor, stop_cursor, nullptr) == 0);
        commit();

        REQUIRE(start_cursor->close(start_cursor) == 0);
        REQUIRE(stop_cursor->close(stop_cursor) == 0);
    }

    /* Take a checkpoint, named unless the name is null. */
    void
    checkpoint(const char *name)
    {
        REQUIRE(_conn->set_timestamp(_conn, ts_config("stable_timestamp", _ts).c_str()) == 0);
        std::string cfg = name == nullptr ? "" : std::string("name=") + name;
        REQUIRE(_session->checkpoint(_session, cfg.c_str()) == 0);
    }

    int64_t
    conn_stat(int key)
    {
        WT_CURSOR *cursor;
        const char *desc, *pvalue;
        int64_t value;

        REQUIRE(_session->open_cursor(_session, "statistics:", nullptr, nullptr, &cursor) == 0);
        cursor->set_key(cursor, key);
        REQUIRE(cursor->search(cursor) == 0);
        REQUIRE(cursor->get_value(cursor, &desc, &pvalue, &value) == 0);
        REQUIRE(cursor->close(cursor) == 0);
        return (value);
    }

    std::map<std::string, std::string>
    read_checkpoint(const std::string &checkpoint)
    {
        return (read_contents(_session, uri, "checkpoint=" + checkpoint));
    }

private:
    WT_CONNECTION *_conn;
    WT_SESSION_IMPL *_session_impl;
    WT_SESSION *_session;
    WT_CURSOR *_cursor;
    uint64_t _ts;
};

bool
in_range(const std::string &key, const std::string *lower_bound, const std::string *upper_bound)
{
    return ((lower_bound == nullptr || key >= *lower_bound) &&
      (upper_bound == nullptr || key < *upper_bound));
}

/* Compute the differences between two checkpoints' contents, as the diff should return them. */
std::vector<difference>
expected_differences(const std::map<std::string, std::string> &old_contents,
  const std::map<std::string, std::string> &new_contents, const std::string *lower_bound = nullptr,
  const std::string *upper_bound = nullptr)
{
    std::vector<difference> differences;
    auto o = old_contents.begin(), n = new_contents.begin();

    while (o != old_contents.end() || n != new_contents.end()) {
        difference d;
        if (n == new_contents.end() || (o != old_contents.end() && o->first < n->first)) {
            d = {WT_BTREE_DIFF_DELETED, o->first, o->second, std::nullopt};
            ++o;
        } else if (o == old_contents.end() || n->first < o->first) {
            d = {WT_BTREE_DIFF_ADDED, n->first, std::nullopt, n->second};
            ++n;
        } else {
            d = {WT_BTREE_DIFF_MODIFIED, o->first, o->second, n->second};
            ++o;
            ++n;
            if (*d.old_value == *d.new_value)
                continue;
        }

        if (in_range(d.key, lower_bound, upper_bound))
            differences.push_back(d);
    }
    return (differences);
}

std::optional<std::string>
item_string(const WT_ITEM *item)
{
    if (item == nullptr)
        return (std::nullopt);
    return (std::string(static_cast<const char *>(item->data), item->size));
}

/* Append the next difference to the list; return WT_NOTFOUND at the end. */
int
next_difference(WT_BTREE_DIFF *diff, std::vector<difference> &differences)
{
    WT_BTREE_DIFF_ENTRY entry;
    int ret;

    if ((ret = __wt_btree_diff_next(diff, &entry)) != 0)
        return (ret);

    REQUIRE(entry.key != nullptr);
    CHECK((entry.old_value == nullptr) == (entry.type == WT_BTREE_DIFF_ADDED));
    CHECK((entry.new_value == nullptr) == (entry.type == WT_BTREE_DIFF_DELETED));

    differences.push_back({entry.type, *item_string(entry.key), item_string(entry.old_value),
      item_string(entry.new_value)});
    return (0);
}

WT_BTREE_DIFF *
open_diff(WT_SESSION_IMPL *session, const char *old_checkpoint, const char *new_checkpoint,
  const std::string *lower_bound = nullptr, const std::string *upper_bound = nullptr)
{
    WT_BTREE_DIFF *diff;
    WT_ITEM lower_item, upper_item;

    if (lower_bound != nullptr)
        lower_item = make_item(*lower_bound);
    if (upper_bound != nullptr)
        upper_item = make_item(*upper_bound);

    REQUIRE(__wt_btree_diff_open_checkpoints(session, uri, old_checkpoint, new_checkpoint,
              lower_bound == nullptr ? nullptr : &lower_item,
              upper_bound == nullptr ? nullptr : &upper_item, &diff) == 0);
    return (diff);
}

/* Return every difference, and close the diff. */
std::vector<difference>
drain_diff(WT_BTREE_DIFF *diff)
{
    std::vector<difference> differences;
    int ret;

    while ((ret = next_difference(diff, differences)) == 0)
        ;
    REQUIRE(ret == WT_NOTFOUND);

    /* Calls after the end keep returning WT_NOTFOUND. */
    REQUIRE(next_difference(diff, differences) == WT_NOTFOUND);

    REQUIRE(__wt_btree_diff_close(&diff) == 0);
    REQUIRE(diff == nullptr);
    return (differences);
}

std::vector<difference>
run_diff(WT_SESSION_IMPL *session, const char *old_checkpoint, const char *new_checkpoint,
  const std::string *lower_bound = nullptr, const std::string *upper_bound = nullptr)
{
    return (
      drain_diff(open_diff(session, old_checkpoint, new_checkpoint, lower_bound, upper_bound)));
}

std::string
random_value(std::mt19937_64 &rng, bool overflow = true)
{
    /* Mostly small values, some larger than the maximum leaf value so they go overflow. */
    size_t size = overflow && rng() % 20 == 0 ? 1024 + rng() % 2048 : 1 + rng() % 200;

    std::string value(size, ' ');
    for (auto &c : value)
        c = static_cast<char>('a' + rng() % 26);
    return (value);
}

/* Load even keys 0, 2, ..., 2 * (count - 1), committing every thousand keys. */
void
load(timestamped_table &table, std::mt19937_64 &rng, uint64_t count, bool overflow = true)
{
    for (uint64_t i = 0; i < count; ++i) {
        if (i % 1000 == 0)
            table.begin();
        table.put(make_key(2 * i), random_value(rng, overflow));
        if (i % 1000 == 999 || i == count - 1)
            table.commit();
    }
}

} // namespace

TEST_CASE("btree diff matches checkpoint cursors", "[btree_diff]")
{
    const std::string home = "WT_TEST.btree_diff_random";
    const uint64_t seed = std::random_device{}();
    const uint64_t count = 20 * WT_THOUSAND;
    CAPTURE(seed);

    cleanup(home);
    {
        connection_wrapper conn(home, precise_cfg);
        timestamped_table table(conn);
        std::mt19937_64 rng(seed);

        load(table, rng, count);
        table.checkpoint("A");

        /* Update and remove loaded keys, and insert new odd keys. */
        std::map<uint64_t, bool> removed;
        table.begin();
        for (int i = 0; i < 3000; ++i) {
            uint64_t n = rng() % (2 * count);
            if (n % 2 == 1)
                table.put(make_key(n), random_value(rng));
            else if (!removed[n]) {
                if (rng() % 2 == 0) {
                    table.remove(make_key(n));
                    removed[n] = true;
                } else
                    table.put(make_key(n), random_value(rng));
            }
        }
        table.commit();
        table.checkpoint("B");

        /* Diff in both directions. */
        auto a = table.read_checkpoint("A");
        auto b = table.read_checkpoint("B");
        auto expected = expected_differences(a, b);
        REQUIRE(!expected.empty());
        CHECK(run_diff(table.session_impl(), "A", "B") == expected);
        CHECK(run_diff(table.session_impl(), "B", "A") == expected_differences(b, a));

        /* Diff with bounds at, between and beyond the keys. */
        for (int i = 0; i < 20; ++i) {
            std::string lower = make_key(rng() % (2 * count + 10));
            std::string upper = make_key(rng() % (2 * count + 10));
            if (upper < lower)
                std::swap(lower, upper);
            CAPTURE(lower, upper);

            CHECK(run_diff(table.session_impl(), "A", "B", &lower, nullptr) ==
              expected_differences(a, b, &lower, nullptr));
            CHECK(run_diff(table.session_impl(), "A", "B", nullptr, &upper) ==
              expected_differences(a, b, nullptr, &upper));
            CHECK(run_diff(table.session_impl(), "A", "B", &lower, &upper) ==
              expected_differences(a, b, &lower, &upper));
        }
    }
    cleanup(home);
}

TEST_CASE("btree diff descends only into differing subtrees", "[btree_diff]")
{
    const std::string home = "WT_TEST.btree_diff_skip";
    const uint64_t seed = std::random_device{}();
    CAPTURE(seed);

    cleanup(home);
    uint64_t ts;
    {
        connection_wrapper conn(home, precise_cfg);
        timestamped_table table(conn);
        std::mt19937_64 rng(seed);

        load(table, rng, 20 * WT_THOUSAND);
        table.checkpoint(nullptr);
        ts = table.ts();
        conn.clear_do_cleanup();
    }
    {
        /*
         * Reopen, so that a write dirties only the pages on its path. Otherwise closing rewrites
         * the loaded pages, including unchanged ones.
         */
        connection_wrapper conn(home, precise_cfg);
        timestamped_table table(conn, ts);
        table.checkpoint("A");

        /* Change one key. */
        table.begin();
        table.put(make_key(12346), "changed");
        table.commit();
        table.checkpoint("B");

        /* A checkpoint compared with itself reads no pages. */
        WT_BTREE_DIFF *diff = open_diff(table.session_impl(), "A", "A");
        std::vector<difference> differences;
        CHECK(next_difference(diff, differences) == WT_NOTFOUND);
        CHECK(diff->pages_read == 0);
        CHECK(diff->identical_skips > 0);
        REQUIRE(__wt_btree_diff_close(&diff) == 0);

        /* One changed key reads about one path down each tree. */
        diff = open_diff(table.session_impl(), "A", "B");
        REQUIRE(next_difference(diff, differences) == 0);
        REQUIRE(next_difference(diff, differences) == WT_NOTFOUND);
        CHECK(differences.size() == 1);
        CHECK(differences[0].type == WT_BTREE_DIFF_MODIFIED);
        CHECK(differences[0].key == make_key(12346));
        CHECK(differences[0].new_value == "changed");
        CHECK(diff->pages_read <= 16);
        CHECK(diff->identical_skips > 0);
        REQUIRE(__wt_btree_diff_close(&diff) == 0);
    }
    cleanup(home);
}

TEST_CASE("btree diff objects on the same checkpoints are independent", "[btree_diff]")
{
    const std::string home = "WT_TEST.btree_diff_multi";
    const uint64_t seed = std::random_device{}();
    const uint64_t count = 10 * WT_THOUSAND;
    CAPTURE(seed);

    cleanup(home);
    {
        connection_wrapper conn(home, precise_cfg);
        timestamped_table table(conn);
        std::mt19937_64 rng(seed);

        load(table, rng, count);
        table.checkpoint("A");

        table.begin();
        for (int i = 0; i < 500; ++i)
            table.put(make_key(rng() % (2 * count)), random_value(rng));
        table.commit();
        table.checkpoint("B");

        /* Open three diffs that split the key range. */
        const std::string split1 = make_key(count / 2), split2 = make_key(count);
        WT_BTREE_DIFF *diffs[3] = {open_diff(table.session_impl(), "A", "B", nullptr, &split1),
          open_diff(table.session_impl(), "A", "B", &split1, &split2),
          open_diff(table.session_impl(), "A", "B", &split2, nullptr)};

        /* Step them round-robin until all three are done. */
        std::vector<difference> differences[3];
        bool done[3] = {false, false, false};
        while (!done[0] || !done[1] || !done[2]) {
            for (int i = 0; i < 3; ++i) {
                if (done[i])
                    continue;
                int ret = next_difference(diffs[i], differences[i]);
                REQUIRE((ret == 0 || ret == WT_NOTFOUND));
                done[i] = ret == WT_NOTFOUND;
            }
        }
        for (auto &diff : diffs)
            REQUIRE(__wt_btree_diff_close(&diff) == 0);

        /* Together they return the whole diff. */
        std::vector<difference> all = differences[0];
        all.insert(all.end(), differences[1].begin(), differences[1].end());
        all.insert(all.end(), differences[2].begin(), differences[2].end());
        CHECK(all == expected_differences(table.read_checkpoint("A"), table.read_checkpoint("B")));
    }
    cleanup(home);
}

TEST_CASE("btree diff against the newest unnamed checkpoint", "[btree_diff]")
{
    const std::string home = "WT_TEST.btree_diff_unnamed";
    const uint64_t seed = std::random_device{}();
    CAPTURE(seed);

    cleanup(home);
    {
        connection_wrapper conn(home, precise_cfg);
        timestamped_table table(conn);
        std::mt19937_64 rng(seed);

        load(table, rng, 5 * WT_THOUSAND);
        table.checkpoint("A");

        table.begin();
        table.remove(make_key(100));
        table.put(make_key(101), "inserted");
        table.commit();
        table.checkpoint(nullptr);

        /* WiredTigerCheckpoint names the newest unnamed checkpoint. */
        CHECK(run_diff(table.session_impl(), "A", "WiredTigerCheckpoint") ==
          expected_differences(
            table.read_checkpoint("A"), table.read_checkpoint("WiredTigerCheckpoint")));
    }
    cleanup(home);
}

TEST_CASE("btree diff over a fast-truncated range", "[btree_diff]")
{
    const std::string home = "WT_TEST.btree_diff_truncate";
    const uint64_t seed = std::random_device{}();
    const uint64_t count = 20 * WT_THOUSAND;
    uint64_t ts;
    CAPTURE(seed);

    cleanup(home);
    std::mt19937_64 rng(seed);
    {
        connection_wrapper conn(home, precise_cfg);
        timestamped_table table(conn);

        /* Load without overflow values: pages with overflow items cannot be fast-truncated. */
        load(table, rng, count, false);
        table.checkpoint("A");
        ts = table.ts();
        conn.clear_do_cleanup();
    }
    {
        /* Reopen, so the pages in the truncated range are on disk and can be fast-truncated. */
        connection_wrapper conn(home, precise_cfg);
        timestamped_table table(conn, ts);
        table.truncate(make_key(count / 2), make_key(count + count / 2));
        table.checkpoint("B");

        /* Make sure some pages were fast-truncated. */
        REQUIRE(table.conn_stat(WT_STAT_CONN_REC_PAGE_DELETE_FAST) > 0);

        CHECK(run_diff(table.session_impl(), "A", "B") ==
          expected_differences(table.read_checkpoint("A"), table.read_checkpoint("B")));
    }
    cleanup(home);
}

TEST_CASE("btree diff requires precise checkpoints", "[btree_diff]")
{
    const std::string home = "WT_TEST.btree_diff_not_precise";

    cleanup(home);
    {
        connection_wrapper conn(home, "create");
        timestamped_table table(conn);

        table.begin();
        table.put(make_key(1), "value");
        table.commit();
        table.checkpoint("A");
        table.checkpoint("B");

        WT_BTREE_DIFF *diff;
        CHECK(__wt_btree_diff_open_checkpoints(
                table.session_impl(), uri, "A", "B", nullptr, nullptr, &diff) == ENOTSUP);
        CHECK(diff == nullptr);
    }
    cleanup(home);
}

TEST_CASE("btree diff rejects prepared updates", "[btree_diff]")
{
    const std::string home = "WT_TEST.btree_diff_prepared";

    cleanup(home);
    {
        connection_wrapper conn(home, "create,precise_checkpoint=true,preserve_prepared=true");
        timestamped_table table(conn);

        table.begin();
        table.put(make_key(1), "value");
        table.commit();
        table.checkpoint("A");

        /* Prepare an update in another session. */
        WT_SESSION *prepared = &conn.create_session()->iface;
        WT_CURSOR *cursor;
        std::string key = make_key(1), value = "prepared";
        WT_ITEM k = make_item(key), v = make_item(value);
        REQUIRE(prepared->open_cursor(prepared, uri, nullptr, nullptr, &cursor) == 0);
        REQUIRE(prepared->begin_transaction(prepared, nullptr) == 0);
        cursor->set_key(cursor, &k);
        cursor->set_value(cursor, &v);
        REQUIRE(cursor->update(cursor) == 0);
        REQUIRE(
          prepared->prepare_transaction(prepared,
            (ts_config("prepare_timestamp", table.ts() + 1) + ",prepared_id=1").c_str()) == 0);

        /* Commit past the prepare timestamp, so the checkpoint is stable past it and writes it. */
        table.begin();
        table.put(make_key(2), "value");
        table.commit();
        table.checkpoint("B");

        WT_BTREE_DIFF *diff;
        CHECK(__wt_btree_diff_open_checkpoints(
                table.session_impl(), uri, "A", "B", nullptr, nullptr, &diff) == ENOTSUP);
        CHECK(diff == nullptr);

        REQUIRE(prepared->rollback_transaction(prepared, nullptr) == 0);
        REQUIRE(cursor->close(cursor) == 0);
    }
    cleanup(home);
}

#ifndef _WIN32
namespace {

std::string
disagg_cfg(const std::string &role)
{
    return (layered_disagg_build_cfg(role) + ",precise_checkpoint=true");
}

void
layered_put(WT_SESSION *session, const std::string &table, int count, const char *commit_ts)
{
    WT_CURSOR *cursor;

    REQUIRE(session->open_cursor(session, table.c_str(), nullptr, nullptr, &cursor) == 0);
    REQUIRE(session->begin_transaction(session, nullptr) == 0);
    for (int i = 0; i < count; ++i) {
        std::string key = make_key(static_cast<uint64_t>(i));
        cursor->set_key(cursor, key.c_str());
        cursor->set_value(cursor, commit_ts);
        REQUIRE(cursor->insert(cursor) == 0);
    }
    REQUIRE(session->commit_transaction(
              session, (std::string("commit_timestamp=") + commit_ts).c_str()) == 0);
    REQUIRE(cursor->close(cursor) == 0);
}

/*
 * Acquire the data handle of the shared metadata table's newest checkpoint. As in disaggregated
 * storage, the handle has the checkpoint in its name, after a slash.
 */
WT_DATA_HANDLE *
shared_metadata_checkpoint(WT_SESSION_IMPL *session, std::string &object)
{
    WT_DATA_HANDLE *dhandle, *saved_dhandle;
    const char *name;

    REQUIRE(__wt_meta_checkpoint_last_name(
              session, WT_DISAGG_METADATA_URI, &name, nullptr, nullptr) == 0);
    object = std::string(WT_DISAGG_METADATA_URI) + "/" + name;
    __wt_free(session, name);

    /* Getting the handle sets the session's data handle, so restore it. */
    saved_dhandle = session->dhandle;
    REQUIRE(__wt_session_get_dhandle(session, object.c_str(), nullptr, nullptr, 0) == 0);
    dhandle = session->dhandle;
    session->dhandle = saved_dhandle;
    REQUIRE(dhandle->checkpoint == nullptr);
    return (dhandle);
}

void
release_dhandle(WT_SESSION_IMPL *session, WT_DATA_HANDLE *dhandle)
{
    int ret;

    WT_WITH_DHANDLE(session, dhandle, ret = __wt_session_release_dhandle(session));
    REQUIRE(ret == 0);
}

} // namespace

TEST_CASE("btree diff of shared metadata checkpoints on a follower", "[btree_diff]")
{
    const std::string leader_home = "WT_TEST.btree_diff_leader";
    const std::string follower_home = "WT_TEST.btree_diff_follower";
    const char *table_cfg = "key_format=S,value_format=S,block_manager=disagg,type=layered";

    /* Fresh homes with the page log store shared between leader and follower. */
    testutil_system("rm -rf %s %s && mkdir -p %s/kv_home %s && ln -s ../%s/kv_home %s/kv_home",
      leader_home.c_str(), follower_home.c_str(), leader_home.c_str(), follower_home.c_str(),
      leader_home.c_str(), follower_home.c_str());
    {
        /* Write a table on the leader and pick up its checkpoint on the follower. */
        connection_wrapper leader(leader_home, disagg_cfg("leader").c_str());
        WT_CONNECTION *leader_conn = leader.get_wt_connection();
        WT_SESSION *leader_session = &leader.create_session()->iface;
        REQUIRE(leader_conn->set_timestamp(leader_conn, "oldest_timestamp=1") == 0);
        REQUIRE(leader_session->create(leader_session, "table:kept", table_cfg) == 0);
        layered_put(leader_session, "table:kept", 100, "10");
        layered_disagg_leader_checkpoint(leader_conn, leader_session, 0x10);

        connection_wrapper follower(follower_home, disagg_cfg("follower").c_str());
        WT_CONNECTION *follower_conn = follower.get_wt_connection();
        WT_SESSION_IMPL *follower_session = follower.create_session();
        layered_disagg_pickup_latest_checkpoint(follower_conn, &follower_session->iface);

        /* Acquire the handle now: the next pickup removes the checkpoint's name. */
        std::string old_object;
        WT_DATA_HANDLE *old_dhandle = shared_metadata_checkpoint(follower_session, old_object);
        auto old_contents = read_contents(&follower_session->iface, old_object, "");

        /* Add a table, change the existing one, and pick up the new checkpoint. */
        REQUIRE(leader_session->create(leader_session, "table:added", table_cfg) == 0);
        layered_put(leader_session, "table:added", 10, "20");
        layered_put(leader_session, "table:kept", 200, "20");
        layered_disagg_leader_checkpoint(leader_conn, leader_session, 0x20);
        layered_disagg_pickup_latest_checkpoint(follower_conn, &follower_session->iface);

        std::string new_object;
        WT_DATA_HANDLE *new_dhandle = shared_metadata_checkpoint(follower_session, new_object);
        REQUIRE(new_object != old_object);
        auto new_contents = read_contents(&follower_session->iface, new_object, "");

        WT_BTREE_DIFF *diff;
        REQUIRE(__wt_btree_diff_open(
                  follower_session, old_dhandle, new_dhandle, nullptr, nullptr, &diff) == 0);
        auto differences = drain_diff(diff);
        CHECK(differences == expected_differences(old_contents, new_contents));

        /* Check for the metadata entries of both tables. Raw S-format keys include the nul byte. */
        auto has = [&differences](WT_BTREE_DIFF_TYPE type, const char *key) {
            std::string raw_key(key, strlen(key) + 1);
            return (std::any_of(differences.begin(), differences.end(),
              [&](const difference &d) { return (d.type == type && d.key == raw_key); }));
        };
        CHECK(has(WT_BTREE_DIFF_ADDED, "layered:added"));
        CHECK(has(WT_BTREE_DIFF_MODIFIED, "file:kept.wt_stable"));

        release_dhandle(follower_session, old_dhandle);
        release_dhandle(follower_session, new_dhandle);
    }
    testutil_system("rm -rf %s %s", leader_home.c_str(), follower_home.c_str());
}
#endif /* !_WIN32 */
