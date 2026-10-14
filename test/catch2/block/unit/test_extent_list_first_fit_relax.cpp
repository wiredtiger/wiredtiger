/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * [extent_list]: block_ext.c
 * Test when a first-fit search may relax.
 *
 * A first-fit search takes the lowest-offset extent that is large enough. While compaction is
 * relocating pages out of the end of the file, an extent anywhere in front of the region being
 * emptied is good enough, so the search may settle for one once it has inspected the configured
 * number of by-size buckets. These tests cover that threshold, and the rule that a search may only
 * relax while compaction is active: see __wti_block_compact_relax_limit.
 *
 * The search itself is covered by the block_first_srch tests in test_extent_list.cpp.
 */

#include <memory>
#include <random>
#include <vector>

#include <catch2/catch.hpp>

#include "../utils_extlist.h"
#include "wt_internal.h"
#include "../../wrappers/mock_session.h"

using namespace utils;

namespace {

constexpr wt_off_t unit = 4096;

/*
 * Save the configured bucket cap and put it back when the test leaves, whether it ran to the end or
 * returned early from a failed assertion.
 */
class cap_guard {
public:
    explicit cap_guard(WT_CONNECTION_IMPL *conn)
        : _conn(conn), _saved(__wt_atomic_load_uint64_relaxed(&conn->block_first_fit_bucket_cap))
    {
    }
    ~cap_guard()
    {
        __wt_atomic_store_uint64_relaxed(&_conn->block_first_fit_bucket_cap, _saved);
    }
    void
    set(uint64_t cap)
    {
        __wt_atomic_store_uint64_relaxed(&_conn->block_first_fit_bucket_cap, cap);
    }

private:
    WT_CONNECTION_IMPL *_conn;
    uint64_t _saved;
};

/*
 * Build an extent list that tracks both the by-offset and the by-size skiplists, which is how the
 * live available list is configured. The by-size skiplist is only maintained by the insert path, so
 * these lists have to be built with __ut_block_off_insert rather than by hand.
 */
void
insert_extents(WT_SESSION_IMPL *session, WT_EXTLIST *extlist, const std::vector<off_size> &extents)
{
    extlist->track_size = 1;
    for (const off_size &to_insert : extents) {
        INFO("insert off " << to_insert.off << " size " << to_insert.size);
        REQUIRE(__ut_block_off_insert(session, extlist, to_insert.off, to_insert.size) == 0);
    }
}

/*
 * Compute the expected first-fit result directly from the input, independently of the
 * implementation: the lowest-offset extent that is large enough.
 */
wt_off_t
expected_first_fit(const std::vector<off_size> &extents, wt_off_t request)
{
    wt_off_t best = -1;

    for (const off_size &one : extents)
        if (one.size >= request && (best == -1 || one.off < best))
            best = one.off;

    return (best);
}

/*
 * Generate a pseudo-random, non-overlapping extent list. Gaps keep the extents non-adjacent,
 * modeling a fragmented available extent list.
 */
std::vector<off_size>
random_extents(std::mt19937 &rng, int count, wt_off_t *endp = nullptr)
{
    std::uniform_int_distribution<int> size_units(1, 20);
    std::uniform_int_distribution<int> gap_units(1, 5);
    std::vector<off_size> extents;
    wt_off_t off = unit;

    for (int i = 0; i < count; i++) {
        wt_off_t size = size_units(rng) * unit;
        extents.push_back(off_size(off, size));
        off += size + gap_units(rng) * unit;
    }

    if (endp != nullptr)
        *endp = off;

    return (extents);
}

} // namespace

TEST_CASE("Extent Lists: first-fit bucket cap", "[extent_list]")
{
    std::shared_ptr<mock_session> mock_session = mock_session::build_test_mock_session();
    WT_SESSION_IMPL *session = mock_session->get_wt_session_impl();
    WT_CONNECTION_IMPL *conn = S2C(session);

    /*
     * Restore the configured cap however a section leaves the test: a failed REQUIRE returns from
     * the section, so an assignment at the end of the test case would be skipped.
     */
    cap_guard guard(conn);

    std::vector<WT_EXT **> stack(WT_SKIP_MAXDEPTH, nullptr);

    /*
     * Stopping early is the only thing that makes the result differ from strict first-fit, so every
     * section below also checks whether the statistic that reports it moved.
     *
     * Read the slot the statistic is written to rather than assuming it is the first one, otherwise
     * these checks would silently compare an untouched counter against itself.
     */
    auto relaxed_count = [&]() {
        return (__wt_atomic_load_int64_relaxed(
          &conn->stats[session->stat_conn_bucket]->block_first_srch_bucket_cap_relaxed));
    };

    /*
     * The cap only applies when the caller supplies a non-zero limit, which only happens while
     * compaction is emptying the front of the file.
     *
     * The lowest-offset extent must be too small for the request, otherwise the head fast path
     * answers without consulting the buckets at all. Beyond that, the qualifying extents sit in
     * distinct buckets, and because buckets are visited in increasing size order while these
     * offsets decrease with size, the strictly lowest offset is in the last bucket the search would
     * visit.
     */
    std::vector<off_size> extents{
      off_size(unit, unit), /* Lowest offset, too small: forces the by-size path. */
      off_size(400 * unit, 2 * unit),
      off_size(300 * unit, 3 * unit),
      off_size(250 * unit, 4 * unit),
      off_size(150 * unit, 5 * unit),
      off_size(10 * unit, 6 * unit),
    };

    SECTION("a zero limit keeps the search strict")
    {
        WT_EXTLIST extlist = {};
        insert_extents(session, &extlist, extents);
        guard.set(1);
        int64_t before = relaxed_count();

        /* Without a limit the cap is ignored, so the lowest offset still wins. */
        REQUIRE(__ut_block_first_srch(session, &extlist, 2 * unit, 0, &stack[0]) == true);
        REQUIRE((*stack[0])->off == 10 * unit);
        REQUIRE(relaxed_count() == before);

        extlist_free(session, extlist);
    }

    SECTION("a zero cap keeps the search strict")
    {
        WT_EXTLIST extlist = {};
        insert_extents(session, &extlist, extents);
        guard.set(0);
        int64_t before = relaxed_count();

        REQUIRE(__ut_block_first_srch(session, &extlist, 2 * unit, 500 * unit, &stack[0]) == true);
        REQUIRE((*stack[0])->off == 10 * unit);
        REQUIRE(relaxed_count() == before);

        extlist_free(session, extlist);
    }

    SECTION("the cap stops the search once a candidate is in front of the limit")
    {
        WT_EXTLIST extlist = {};
        insert_extents(session, &extlist, extents);
        guard.set(1);
        int64_t before = relaxed_count();

        /*
         * The first bucket visited holds the extent at 400 units, which is in front of the limit,
         * so the search stops there rather than walking on to the lower offset at 10 units.
         */
        REQUIRE(__ut_block_first_srch(session, &extlist, 2 * unit, 500 * unit, &stack[0]) == true);
        REQUIRE((*stack[0])->off == 400 * unit);
        REQUIRE(relaxed_count() == before + 1);

        extlist_free(session, extlist);
    }

    SECTION("the cap does not stop the search outside the limit")
    {
        WT_EXTLIST extlist = {};
        insert_extents(session, &extlist, extents);
        guard.set(1);
        int64_t before = relaxed_count();

        /*
         * Only the extent at 10 units is in front of this limit, so reaching the cap is not enough
         * to stop: the search continues until it holds a candidate that qualifies.
         */
        REQUIRE(__ut_block_first_srch(session, &extlist, 2 * unit, 100 * unit, &stack[0]) == true);
        REQUIRE((*stack[0])->off == 10 * unit);

        /*
         * The candidate only became eligible in the last bucket, so the search ran to completion
         * and the result is the strict answer: that must not be reported as a relaxation.
         */
        REQUIRE(relaxed_count() == before);

        extlist_free(session, extlist);
    }

    SECTION("a cap larger than the bucket count is strict")
    {
        WT_EXTLIST extlist = {};
        insert_extents(session, &extlist, extents);
        guard.set(1000);
        int64_t before = relaxed_count();

        REQUIRE(__ut_block_first_srch(session, &extlist, 2 * unit, 500 * unit, &stack[0]) == true);
        REQUIRE((*stack[0])->off == 10 * unit);
        REQUIRE(relaxed_count() == before);

        extlist_free(session, extlist);
    }

    SECTION("the head fast path never relaxes")
    {
        /*
         * The lowest-offset extent is large enough here, so the search answers from the by-offset
         * head without looking at a single bucket. That is the strict answer by definition.
         */
        WT_EXTLIST extlist = {};
        insert_extents(
          session, &extlist, {off_size(unit, 8 * unit), off_size(50 * unit, 8 * unit)});
        guard.set(1);
        int64_t before = relaxed_count();

        REQUIRE(__ut_block_first_srch(session, &extlist, 2 * unit, 500 * unit, &stack[0]) == true);
        REQUIRE((*stack[0])->off == unit);
        REQUIRE(relaxed_count() == before);

        extlist_free(session, extlist);
    }

    SECTION("an unsuccessful search never relaxes")
    {
        WT_EXTLIST extlist = {};
        insert_extents(session, &extlist, extents);
        guard.set(1);
        int64_t before = relaxed_count();

        /* Larger than every extent in the list. */
        REQUIRE(
          __ut_block_first_srch(session, &extlist, 64 * unit, 500 * unit, &stack[0]) == false);
        REQUIRE(relaxed_count() == before);

        extlist_free(session, extlist);
    }

    SECTION("a cap of N inspects exactly N buckets")
    {
        /*
         * Walking the buckets in increasing size order visits offsets 400, 300, 250, 150 and then
         * 10. With a cap of N the search folds in the first N of those and stops, so the result
         * names the lowest offset among them.
         */
        const wt_off_t expected[] = {400 * unit, 300 * unit, 250 * unit, 150 * unit, 10 * unit};

        for (uint64_t cap = 1; cap <= 5; cap++) {
            INFO("cap " << cap);

            WT_EXTLIST extlist = {};
            insert_extents(session, &extlist, extents);
            guard.set(cap);
            int64_t before = relaxed_count();

            REQUIRE(
              __ut_block_first_srch(session, &extlist, 2 * unit, 500 * unit, &stack[0]) == true);
            REQUIRE((*stack[0])->off == expected[cap - 1]);

            /* The last bucket holds the strict answer, so that search is not a relaxation. */
            REQUIRE(relaxed_count() == before + (cap == 5 ? 0 : 1));

            extlist_free(session, extlist);
        }
    }

    SECTION("a capped search does not settle for an extent that crosses the limit")
    {
        /*
         * An extent that starts in front of the limit but ends past it is not a candidate the
         * search may settle for: the block written into it would reach into the region compaction
         * is emptying, so compaction could not then truncate the file. Checking only where the
         * candidate starts is not enough.
         *
         * The first bucket visited here holds exactly such an extent, and the cap allows the search
         * to stop at the first bucket, so the search has to keep going regardless.
         */
        WT_EXTLIST extlist = {};
        insert_extents(session, &extlist,
          {
            off_size(unit, unit),           /* Lowest offset, too small: forces the by-size path. */
            off_size(90 * unit, 2 * unit),  /* Crosses the limit below: 90 + 2 > 91. */
            off_size(200 * unit, 3 * unit), /* Past the limit entirely. */
            off_size(50 * unit, 4 * unit),  /* Fits in front of the limit. */
          });
        guard.set(1);
        int64_t before = relaxed_count();

        REQUIRE(__ut_block_first_srch(session, &extlist, 2 * unit, 91 * unit, &stack[0]) == true);

        /*
         * Neither of the first two buckets qualifies, so the search runs on to the extent at 50
         * units, which both satisfies the request and ends before the limit.
         */
        REQUIRE((*stack[0])->off == 50 * unit);
        REQUIRE(relaxed_count() == before);

        extlist_free(session, extlist);
    }

    SECTION("a capped search still satisfies the request")
    {
        /*
         * Whatever the cap does to the choice of offset, the extent it returns must still be large
         * enough, and must still come from the list.
         *
         * Each list is searched twice: once with no limit, where the result has to be the strict
         * first-fit answer, and once with a limit, where the result is allowed to be worse but an
         * extent the search settled for early has to fit entirely in front of that limit. Writing a
         * block that merely starts in front of the limit would leave part of it inside the region
         * compaction is emptying.
         */
        std::mt19937 rng(99);

        for (int iteration = 0; iteration < 10; iteration++) {
            INFO("iteration " << iteration);

            wt_off_t end = 0;
            std::vector<off_size> random = random_extents(rng, 100, &end);
            wt_off_t limit = end / 2;

            WT_EXTLIST extlist = {};
            insert_extents(session, &extlist, random);
            guard.set(1 + (uint64_t)(iteration % 5));

            for (int units = 1; units <= 21; units++) {
                wt_off_t request = units * unit;
                wt_off_t strict = expected_first_fit(random, request);
                INFO("request " << request);

                /* A search that is not allowed to relax has to find the strict answer. */
                REQUIRE(__ut_block_first_srch(session, &extlist, request, 0, &stack[0]) ==
                  (strict != -1));
                if (strict != -1)
                    REQUIRE((*stack[0])->off == strict);

                int64_t before = relaxed_count();
                bool found =
                  __ut_block_first_srch(session, &extlist, request, limit, &stack[0]) == true;

                /* The cap never changes whether a large-enough extent exists. */
                REQUIRE(found == (strict != -1));
                if (!found)
                    continue;

                WT_EXT *ext = *stack[0];
                REQUIRE(ext != nullptr);
                REQUIRE(ext->size >= request);

                /* The result is never better than the strict answer. */
                REQUIRE(ext->off >= strict);

                if (relaxed_count() == before) {
                    /* The search ran to completion, so it must have returned the strict answer. */
                    REQUIRE(ext->off == strict);
                    continue;
                }

                /*
                 * The search stopped early. That is only sound for an extent that ends before the
                 * limit, so a relaxed result that reaches into the region being emptied is a bug
                 * even though it satisfies the size request.
                 */
                REQUIRE(ext->off < limit);
                REQUIRE(ext->size <= limit - ext->off);
            }

            extlist_free(session, extlist);
        }
    }
}

TEST_CASE("Extent Lists: relaxing is confined to active compaction", "[extent_list]")
{
    /*
     * A first-fit search may only stop early while compaction is emptying the front of a file, and
     * the region compaction chose is what says whether that is the case.
     *
     * The compaction passes do not reset that region themselves -- a pass that stops because it
     * made no progress leaves the last region it chose behind -- so the region has to be cleared
     * when compaction ends. Otherwise a file left in first-fit mode would keep relaxing its
     * searches long after compaction finished.
     */
    std::shared_ptr<mock_session> mock_session = mock_session::build_test_mock_session();
    WT_SESSION_IMPL *session = mock_session->get_wt_session_impl();

    WT_BLOCK block = {};
    REQUIRE(__wt_spin_init(session, &block.live_lock, "relax limit") == 0);
    block.size = 100 * unit;
    block.compact_session_id = WT_SESSION_ID_INVALID;

    SECTION("no region, no relaxation")
    {
        block.compact_pct_tenths = 0;

        __wt_spin_lock(session, &block.live_lock);
        REQUIRE(__wti_block_compact_relax_limit(session, &block) == 0);
        __wt_spin_unlock(session, &block.live_lock);
    }

    SECTION("a chosen region relaxes up to that region")
    {
        __wt_spin_lock(session, &block.live_lock);

        block.compact_pct_tenths = 1;
        REQUIRE(__wti_block_compact_relax_limit(session, &block) == 90 * unit);

        block.compact_pct_tenths = 2;
        REQUIRE(__wti_block_compact_relax_limit(session, &block) == 80 * unit);

        __wt_spin_unlock(session, &block.live_lock);
    }

    SECTION("ending compaction stops the relaxation")
    {
        /*
         * Drive the real state transition rather than a hand-built one: a file that was relaxing
         * its searches must stop doing so as soon as compaction ends, without the file being
         * reopened.
         */
        block.compact_session_id = session->id;
        block.compact_pct_tenths = 2;

        __wt_spin_lock(session, &block.live_lock);
        REQUIRE(__wti_block_compact_relax_limit(session, &block) == 80 * unit);
        __wt_spin_unlock(session, &block.live_lock);

        /* The allocation plan is restored by the same call, so account for it first. */
        __wti_block_configure_first_fit(&block, true);
        REQUIRE(__wt_block_compact_end(session, &block) == 0);

        __wt_spin_lock(session, &block.live_lock);
        REQUIRE(__wti_block_compact_relax_limit(session, &block) == 0);
        __wt_spin_unlock(session, &block.live_lock);
    }

    __wt_spin_destroy(session, &block.live_lock);
}
