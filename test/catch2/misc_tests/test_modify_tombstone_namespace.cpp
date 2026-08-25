/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * Cross-check __wt_modify_result_in_tombstone_namespace against actually applying the modify
 * vector: the predicted result size must always match __wt_modify_apply_api, and the namespace
 * prediction must never miss a result that is in the namespace. The prediction is exact unless an
 * entry can shift a byte of unknown provenance into the marker positions, in which case it may
 * over-report; vectors are checked accordingly.
 */

#include <catch2/catch.hpp>

#include <random>
#include <string>
#include <vector>

#include "wiredtiger.h"
#include "wt_internal.h"
#include "../utils.h"
#include "../wrappers/connection_wrapper.h"

namespace {

struct mod_entry {
    std::string data;
    size_t offset;
    size_t size;
};

/*
 * The prediction is exact unless a shrinking entry can shift a byte from beyond the marker
 * positions into them.
 */
bool
prediction_is_exact(const std::vector<mod_entry> &entries)
{
    for (const auto &e : entries)
        if (e.offset + e.data.size() <= 1 && e.size > e.data.size())
            return false;
    return true;
}

/* Predict via the function under test, apply via the cursor API, and compare. */
void
check_case(WT_SESSION_IMPL *session, WT_CURSOR *cursor, const std::string &base,
  std::vector<mod_entry> &case_entries)
{
    std::vector<WT_MODIFY> entries(case_entries.size());
    for (size_t i = 0; i < case_entries.size(); ++i) {
        entries[i].data.data = case_entries[i].data.data();
        entries[i].data.size = case_entries[i].data.size();
        entries[i].offset = case_entries[i].offset;
        entries[i].size = case_entries[i].size;
    }

    WT_ITEM base_item;
    WT_CLEAR(base_item);
    base_item.data = base.data();
    base_item.size = base.size();

    bool predicted_ns = false;
    size_t predicted_size = 0;
    __wt_modify_result_in_tombstone_namespace(session, cursor->value_format, &base_item,
      entries.data(), (int)entries.size(), &predicted_ns, &predicted_size);

    cursor->value.data = base.data();
    cursor->value.size = base.size();
    REQUIRE(__wt_modify_apply_api(cursor, entries.data(), (int)entries.size()) == 0);

    const uint8_t *result = static_cast<const uint8_t *>(cursor->value.data);
    bool actual_ns =
      cursor->value.size >= 2 && result[0] == (uint8_t)0x14 && result[1] == (uint8_t)0x14;

    CHECK(predicted_size == cursor->value.size);
    if (prediction_is_exact(case_entries))
        CHECK(predicted_ns == actual_ns);
    else
        /* Conservative vectors may over-report the namespace, never miss it. */
        CHECK((predicted_ns || !actual_ns));
}

} // namespace

TEST_CASE("Modify tombstone namespace prediction matches application",
  "[modify][modify_tombstone_namespace]")
{
    const std::string home = "WT_TEST.modify_tombstone_namespace";
    utils::wiredtiger_cleanup(home);

    {
        connection_wrapper conn(home);

        WT_SESSION_IMPL *session_impl = conn.create_session();
        WT_SESSION *session = &session_impl->iface;
        REQUIRE(session->create(session, "file:modify_u.wt", "key_format=S,value_format=u") == 0);
        REQUIRE(session->create(session, "file:modify_s.wt", "key_format=S,value_format=S") == 0);

        WT_CURSOR *cursor_u = nullptr, *cursor_s = nullptr;
        REQUIRE(
          session->open_cursor(session, "file:modify_u.wt", nullptr, nullptr, &cursor_u) == 0);
        REQUIRE(
          session->open_cursor(session, "file:modify_s.wt", nullptr, nullptr, &cursor_s) == 0);

        const std::string ts = "\x14\x14";

        {
            std::vector<std::pair<std::string, std::vector<mod_entry>>> cases = {
              /* Rewrite the leading bytes into the namespace. */
              {"abcdef", {{ts, 0, 2}}},
              /* Pad past the end of the value; leading bytes untouched. */
              {"ab", {{"\x14", 5, 3}}},
              /* Append onto an empty value, result exactly the two tombstone bytes. */
              {"", {{ts, 0, 0}}},
              /* Replace through (and past) the end. */
              {ts + "abc", {{"", 1, 100}}},
              /* Delete shifts trailing bytes into the leading positions. */
              {std::string("zz\x14\x14", 4), {{"", 0, 2}}},
              /* Delete shifts non-marker bytes in: an over-report is allowed, a miss is not. */
              {"abcd", {{"", 0, 2}}},
              /* Result shorter than the namespace prefix. */
              {ts, {{"", 0, 1}}},
              /* Grow then shrink: the intermediate exceeds both base and result sizes. */
              {std::string("\x14\x14rest", 6), {{std::string(100, 'x'), 0, 0}, {"", 0, 100}}},
              /* Cumulative offsets: a leading insert shifts what the second entry sees. */
              {"abcd", {{"\x14", 0, 0}, {"\x14", 1, 1}}},
              /* Empty data, zero size: a no-op entry. */
              {"abcd", {{"", 2, 0}}},
            };
            for (auto &c : cases) {
                CAPTURE(c.first, c.second.size());
                check_case(session_impl, cursor_u, c.first, c.second);
            }
        }

        {
            std::random_device rd;
            const unsigned seed = rd();
            CAPTURE(seed);
            std::mt19937 rng(seed);

            auto rand_byte = [&](void) -> char {
                /* Bias toward the tombstone byte so namespace results are common. */
                return (rng() % 5 == 0) ? '\x14' : (char)(rng() % 256);
            };

            for (int trial = 0; trial < 5000; ++trial) {
                CAPTURE(trial);
                std::string base;
                for (size_t i = rng() % 48; i > 0; --i)
                    base.push_back(rand_byte());

                std::vector<mod_entry> entries(1 + rng() % 6);
                for (auto &e : entries) {
                    for (size_t i = rng() % 24; i > 0; --i)
                        e.data.push_back(rand_byte());
                    e.offset = rng() % 56;
                    e.size = rng() % 24;
                }
                check_case(session_impl, cursor_u, base, entries);
            }
        }

        {
            std::random_device rd;
            const unsigned seed = rd();
            CAPTURE(seed);
            std::mt19937 rng(seed);

            auto rand_char = [&](void) -> char {
                return (rng() % 5 == 0) ? '\x14' : (char)(1 + rng() % 255);
            };

            for (int trial = 0; trial < 2000; ++trial) {
                CAPTURE(trial);
                std::string base;
                for (size_t i = rng() % 32; i > 0; --i)
                    base.push_back(rand_char());
                base.push_back('\0');

                std::vector<mod_entry> entries(1 + rng() % 4);
                for (auto &e : entries) {
                    for (size_t i = rng() % 16; i > 0; --i)
                        e.data.push_back(rand_char());
                    e.offset = rng() % 40;
                    e.size = rng() % 16;
                }
                check_case(session_impl, cursor_s, base, entries);
            }
        }

        REQUIRE(cursor_u->close(cursor_u) == 0);
        REQUIRE(cursor_s->close(cursor_s) == 0);
    }

    utils::wiredtiger_cleanup(home);
}
