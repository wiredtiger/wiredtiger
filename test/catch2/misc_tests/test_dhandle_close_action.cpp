/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * The handle close decides what to do with a tree's pages from five independent conditions, and the
 * consequences of that decision are acted on much further down the close. These tests pin the
 * decision for every combination of those conditions, so a restructuring of the close cannot
 * quietly move a case from one action to another.
 *
 * The cases are grouped the way the decision itself is ordered. Each group fixes the conditions
 * that put a tree in that group and varies the rest, so every group is exhaustive over what it
 * leaves free, and the groups together cover all 32 combinations.
 */

#include <catch2/catch.hpp>

#include "wiredtiger.h"
#include "wt_internal.h"

namespace {

struct close_case {
    bool dhandle_dead;
    bool is_mapped;
    bool final_close;
    bool mark_dead;
    bool skips_checkpoint;
    WT_DHANDLE_CLOSE_ACTION expected;
    const char *why;
};

/*
 * Only a read-only checkpoint handle over a single local file is ever mapped, so a mapped tree
 * always has a checkpoint to write. Its pages must go before the underlying handle is closed,
 * because closing it releases the mapping they point into. It can never be left for sweep, so being
 * asked to mark it dead changes nothing.
 */
close_case
mapped(bool dhandle_dead, bool final_close, bool mark_dead, WT_DHANDLE_CLOSE_ACTION expected,
  const char *why)
{
    return close_case{dhandle_dead, true, final_close, mark_dead, false, expected, why};
}

const close_case mapped_cases[] = {
  mapped(/* dead */ true, /* final */ false, /* mark_dead */ false, WT_DHANDLE_CLOSE_DISCARD_EARLY,
    "dead, so only the pages are left to deal with"),
  mapped(/* dead */ true, /* final */ false, /* mark_dead */ true, WT_DHANDLE_CLOSE_DISCARD_EARLY,
    "already dead, the request is redundant"),
  mapped(/* dead */ true, /* final */ true, /* mark_dead */ false, WT_DHANDLE_CLOSE_DISCARD_EARLY,
    "dead at shutdown"),
  mapped(/* dead */ true, /* final */ true, /* mark_dead */ true, WT_DHANDLE_CLOSE_DISCARD_EARLY,
    "dead at shutdown, the request is redundant"),
  mapped(/* dead */ false, /* final */ false, /* mark_dead */ false, WT_DHANDLE_CLOSE_CHECKPOINT,
    "live, so it is flushed first"),
  mapped(/* dead */ false, /* final */ false, /* mark_dead */ true, WT_DHANDLE_CLOSE_CHECKPOINT,
    "cannot be marked dead, so it is flushed instead"),
  mapped(/* dead */ false, /* final */ true, /* mark_dead */ false, WT_DHANDLE_CLOSE_CHECKPOINT,
    "live at shutdown"),
  mapped(/* dead */ false, /* final */ true, /* mark_dead */ true, WT_DHANDLE_CLOSE_CHECKPOINT,
    "cannot be marked dead at shutdown either"),
};

/*
 * A handle that is already dead has been through this close once. Nothing is left to decide: the
 * pages go, after the handle close because the tree is not mapped, and neither the request to mark
 * it dead nor whether it writes checkpoints can change that.
 */
close_case
already_dead(bool final_close, bool mark_dead, bool skips_checkpoint, const char *why)
{
    return close_case{
      true, false, final_close, mark_dead, skips_checkpoint, WT_DHANDLE_CLOSE_DISCARD_LATE, why};
}

const close_case dead_cases[] = {
  already_dead(/* final */ false, /* mark_dead */ false, /* skips_ckpt */ false, "plain revisit"),
  already_dead(/* final */ false, /* mark_dead */ false, /* skips_ckpt */ true, "no checkpoint"),
  already_dead(/* final */ false, /* mark_dead */ true, /* skips_ckpt */ false, "request moot"),
  already_dead(/* final */ false, /* mark_dead */ true, /* skips_ckpt */ true, "both moot"),
  already_dead(/* final */ true, /* mark_dead */ false, /* skips_ckpt */ false, "at shutdown"),
  already_dead(/* final */ true, /* mark_dead */ false, /* skips_ckpt */ true, "no checkpoint"),
  already_dead(/* final */ true, /* mark_dead */ true, /* skips_ckpt */ false, "request moot"),
  already_dead(/* final */ true, /* mark_dead */ true, /* skips_ckpt */ true, "both moot"),
};

/*
 * Marking a live tree dead is the only action that leaves the handle open, which is what lets sweep
 * come back and free the pages later. The final close cannot take it: there is no later sweep, so
 * the pages have to go now. Whether the tree writes checkpoints is irrelevant either way, because
 * nothing is being flushed.
 */
close_case
asked_to_defer(
  bool final_close, bool skips_checkpoint, WT_DHANDLE_CLOSE_ACTION expected, const char *why)
{
    return close_case{false, false, final_close, true, skips_checkpoint, expected, why};
}

const close_case defer_cases[] = {
  asked_to_defer(/* final */ false, /* skips_ckpt */ false, WT_DHANDLE_CLOSE_MARK_DEAD,
    "sweep will finish the job"),
  asked_to_defer(/* final */ false, /* skips_ckpt */ true, WT_DHANDLE_CLOSE_MARK_DEAD,
    "deferring wins over having no checkpoint to write"),
  asked_to_defer(/* final */ true, /* skips_ckpt */ false, WT_DHANDLE_CLOSE_DISCARD_LATE,
    "shutdown has no later sweep to defer to"),
  asked_to_defer(/* final */ true, /* skips_ckpt */ true, WT_DHANDLE_CLOSE_DISCARD_LATE,
    "shutdown, and nothing to flush"),
};

/*
 * Nothing was asked of a live tree, so the only question left is whether it has a checkpoint to
 * write. A tree that does is flushed; one that does not has nothing to preserve, so its pages go
 * straight out. The final close makes no difference here -- both actions already happen now.
 */
close_case
nothing_asked(
  bool final_close, bool skips_checkpoint, WT_DHANDLE_CLOSE_ACTION expected, const char *why)
{
    return close_case{false, false, final_close, false, skips_checkpoint, expected, why};
}

const close_case live_cases[] = {
  nothing_asked(/* final */ false, /* skips_ckpt */ false, WT_DHANDLE_CLOSE_CHECKPOINT,
    "durable tree is flushed"),
  nothing_asked(
    /* final */ false, /* skips_ckpt */ true, WT_DHANDLE_CLOSE_DISCARD_LATE, "nothing to flush"),
  nothing_asked(/* final */ true, /* skips_ckpt */ false, WT_DHANDLE_CLOSE_CHECKPOINT,
    "durable tree at shutdown"),
  nothing_asked(/* final */ true, /* skips_ckpt */ true, WT_DHANDLE_CLOSE_DISCARD_LATE,
    "nothing to flush at shutdown"),
};

/*
 * A mapped tree that skips checkpoints, which no caller can produce. Mapping needs a read-only
 * checkpoint handle over a single local file, and none of the three things that make a tree skip
 * checkpoints can be one: an in-memory tree has no file to map, a disaggregated tree is never
 * reported as mapped, and a tiered object -- the only tree flagged WT_BTREE_NO_CHECKPOINT -- uses
 * the multi-handle block manager, which the mapping code excludes. Reaching the close in this state
 * means one of those three facts has changed, so the close aborts rather than guessing.
 */
close_case
cannot_happen(bool dhandle_dead, bool final_close, bool mark_dead)
{
    return close_case{dhandle_dead, true, final_close, mark_dead, true,
      WT_DHANDLE_CLOSE_DISCARD_INVALID, "mapped tree that skips checkpoints"};
}

const close_case invalid_cases[] = {
  cannot_happen(/* dead */ false, /* final */ false, /* mark_dead */ false),
  cannot_happen(/* dead */ false, /* final */ false, /* mark_dead */ true),
  cannot_happen(/* dead */ false, /* final */ true, /* mark_dead */ false),
  cannot_happen(/* dead */ false, /* final */ true, /* mark_dead */ true),
  cannot_happen(/* dead */ true, /* final */ false, /* mark_dead */ false),
  cannot_happen(/* dead */ true, /* final */ false, /* mark_dead */ true),
  cannot_happen(/* dead */ true, /* final */ true, /* mark_dead */ false),
  cannot_happen(/* dead */ true, /* final */ true, /* mark_dead */ true),
};

WT_DHANDLE_CLOSE_ACTION
action_for(const close_case &c)
{
    return (__ut_conn_dhandle_close_action(
      c.dhandle_dead, c.is_mapped, c.final_close, c.mark_dead, c.skips_checkpoint));
}

void
check_group(const close_case *group, size_t count)
{
    for (size_t i = 0; i < count; ++i) {
        const close_case &c = group[i];
        INFO("dead=" << c.dhandle_dead << " mapped=" << c.is_mapped << " final=" << c.final_close
                     << " mark_dead=" << c.mark_dead << " skips_ckpt=" << c.skips_checkpoint << " ("
                     << c.why << ")");
        CHECK(action_for(c) == c.expected);
    }
}

#define CHECK_GROUP(group) check_group(group, sizeof(group) / sizeof((group)[0]))

} // namespace

TEST_CASE("dhandle close action: every combination", "[dhandle][dhandle_close_action]")
{
    /* The groups must partition the input space, or they are not pinning all of it. */
    REQUIRE(sizeof(mapped_cases) + sizeof(dead_cases) + sizeof(defer_cases) + sizeof(live_cases) +
        sizeof(invalid_cases) ==
      32 * sizeof(close_case));

    SECTION("a mapped tree is discarded before the close and never left for sweep")
    {
        CHECK_GROUP(mapped_cases);
    }
    SECTION("a handle that is already dead only has pages left to discard")
    {
        CHECK_GROUP(dead_cases);
    }
    SECTION("leaving the pages for sweep, which the final close cannot do")
    {
        CHECK_GROUP(defer_cases);
    }
    SECTION("a live tree is flushed unless it has no checkpoint to write")
    {
        CHECK_GROUP(live_cases);
    }
    SECTION("a combination no caller can produce is reported rather than acted on")
    {
        CHECK_GROUP(invalid_cases);
    }
}

TEST_CASE("dhandle close action: invariants", "[dhandle][dhandle_close_action]")
{
    const close_case *groups[] = {mapped_cases, dead_cases, defer_cases, live_cases};
    const size_t counts[] = {sizeof(mapped_cases) / sizeof(close_case),
      sizeof(dead_cases) / sizeof(close_case), sizeof(defer_cases) / sizeof(close_case),
      sizeof(live_cases) / sizeof(close_case)};

    for (size_t g = 0; g < sizeof(groups) / sizeof(groups[0]); ++g)
        for (size_t i = 0; i < counts[g]; ++i) {
            const close_case &c = groups[g][i];
            WT_DHANDLE_CLOSE_ACTION action = action_for(c);

            INFO("dead=" << c.dhandle_dead << " mapped=" << c.is_mapped
                         << " final=" << c.final_close << " mark_dead=" << c.mark_dead
                         << " skips_ckpt=" << c.skips_checkpoint);

            /*
             * Leaving the pages for sweep is what keeps a handle open after the close, so getting
             * it wrong strands pages that nothing will ever free. It needs all of: a handle that is
             * not already dead, a caller that asked for it, and a tree reachable again later.
             */
            if (action == WT_DHANDLE_CLOSE_MARK_DEAD) {
                CHECK(!c.final_close);
                CHECK(c.mark_dead);
                CHECK(!c.is_mapped);
                CHECK(!c.dhandle_dead);
            }

            /* Whether a discard runs before or after the close is decided by mapping alone. */
            if (action == WT_DHANDLE_CLOSE_DISCARD_EARLY)
                CHECK(c.is_mapped);
            if (action == WT_DHANDLE_CLOSE_DISCARD_LATE)
                CHECK(!c.is_mapped);

            /* A dead handle has nothing left to flush, and never keeps its pages. */
            if (c.dhandle_dead)
                CHECK((action == WT_DHANDLE_CLOSE_DISCARD_EARLY ||
                  action == WT_DHANDLE_CLOSE_DISCARD_LATE));

            /* A reachable combination always resolves to something the close can act on. */
            CHECK(action != WT_DHANDLE_CLOSE_NONE);
            CHECK(action != WT_DHANDLE_CLOSE_DISCARD_INVALID);
        }
}
