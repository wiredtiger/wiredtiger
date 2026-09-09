/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>

#include "wt_internal.h"
#include "../wrappers/mock_session.h"

/*
 * Address free either bumps the split generation per call, or defers the bump and reclamation while
 * split_stash_batch is set.
 */

static void
init_generations(WT_SESSION_IMPL *session, WT_SESSION_IMPL *slot)
{
    WT_CONNECTION_IMPL *conn;

    conn = S2C(session);
    memset(slot, 0, sizeof(*slot));
    conn->session_array.__array = slot;
    conn->session_array.size = 1;
    conn->session_array.cnt = 0;
    __wt_gen_init(session);
}

static size_t
stash_live(WT_SESSION_IMPL *session, int which)
{
    size_t i, n;

    for (i = 0, n = 0; i < session->stash[which].cnt; ++i)
        if (session->stash[which].list[i].p != NULL)
            ++n;
    return (n);
}

static void *
alloc_buf(WT_SESSION_IMPL *session)
{
    void *p;

    REQUIRE(__wt_calloc(session, 1, 16, &p) == 0);
    return (p);
}

static void
free_stash_list(WT_SESSION_IMPL *session, int which)
{
    __wt_free(session, session->stash[which].list);
    session->stash[which].cnt = session->stash[which].alloc = 0;
}

TEST_CASE("Address free: batch defers generation advancement and reclamation", "[stash_batch]")
{
    std::shared_ptr<mock_session> mock = mock_session::build_test_mock_session();
    WT_SESSION_IMPL *session = mock->get_wt_session_impl();
    WT_SESSION_IMPL slot;
    uint64_t start, batch_gen;

    init_generations(session, &slot);
    start = __wt_gen(session, WT_GEN_SPLIT);

    session->split_stash_batch = true;
    __wti_ref_addr_safe_free(session, alloc_buf(session), 16);
    __wti_ref_addr_safe_free(session, alloc_buf(session), 16);
    __wti_ref_addr_safe_free(session, alloc_buf(session), 16);

    REQUIRE(__wt_gen(session, WT_GEN_SPLIT) == start);
    REQUIRE(stash_live(session, WT_GEN_SPLIT) == 3);
    batch_gen = session->stash[WT_GEN_SPLIT].list[0].gen;
    REQUIRE(batch_gen == start);
    REQUIRE(session->stash[WT_GEN_SPLIT].list[1].gen == batch_gen);
    REQUIRE(session->stash[WT_GEN_SPLIT].list[2].gen == batch_gen);

    /* A connection-wide generation advance must not trigger intermediate reclamation. */
    __wt_gen_next(session, WT_GEN_SPLIT, NULL);
    __wt_stash_discard(session);
    REQUIRE(stash_live(session, WT_GEN_SPLIT) == 3);

    session->split_stash_batch = false;
    __wt_gen_next(session, WT_GEN_SPLIT, NULL);
    __wt_stash_discard(session);

    REQUIRE(stash_live(session, WT_GEN_SPLIT) == 0);
    free_stash_list(session, WT_GEN_SPLIT);
}

TEST_CASE("Address free: without batch each address advances the generation", "[stash_batch]")
{
    std::shared_ptr<mock_session> mock = mock_session::build_test_mock_session();
    WT_SESSION_IMPL *session = mock->get_wt_session_impl();
    WT_SESSION_IMPL slot;
    uint64_t start;

    init_generations(session, &slot);
    start = __wt_gen(session, WT_GEN_SPLIT);

    __wti_ref_addr_safe_free(session, alloc_buf(session), 16);
    REQUIRE(__wt_gen(session, WT_GEN_SPLIT) == start + 1);
    REQUIRE(stash_live(session, WT_GEN_SPLIT) == 1);

    __wti_ref_addr_safe_free(session, alloc_buf(session), 16);
    REQUIRE(__wt_gen(session, WT_GEN_SPLIT) == start + 2);
    /* The older stash is reclaimed; the newest one still matches the previous generation. */
    REQUIRE(stash_live(session, WT_GEN_SPLIT) == 1);

    __wt_stash_discard(session);
    REQUIRE(stash_live(session, WT_GEN_SPLIT) == 0);

    free_stash_list(session, WT_GEN_SPLIT);
}

TEST_CASE("Address free: batched stash stays until the pinning generation drains", "[stash_batch]")
{
    std::shared_ptr<mock_session> mock = mock_session::build_test_mock_session();
    WT_SESSION_IMPL *session = mock->get_wt_session_impl();
    WT_CONNECTION_IMPL *conn = S2C(session);
    WT_SESSION_IMPL slot;
    WT_HAZARD hazard;

    memset(&hazard, 0, sizeof(hazard));
    init_generations(session, &slot);

    slot.active = 1;
    slot.hazards.arr = &hazard;
    slot.generations[WT_GEN_SPLIT] = __wt_gen(session, WT_GEN_SPLIT);
    conn->session_array.cnt = 1;

    session->split_stash_batch = true;
    __wti_ref_addr_safe_free(session, alloc_buf(session), 16);
    __wti_ref_addr_safe_free(session, alloc_buf(session), 16);
    session->split_stash_batch = false;
    __wt_gen_next(session, WT_GEN_SPLIT, NULL);
    __wt_stash_discard(session);
    REQUIRE(stash_live(session, WT_GEN_SPLIT) == 2);

    slot.generations[WT_GEN_SPLIT] = 0;
    __wt_stash_discard(session);
    REQUIRE(stash_live(session, WT_GEN_SPLIT) == 0);

    free_stash_list(session, WT_GEN_SPLIT);
}

TEST_CASE("Address free: split batch does not defer other stash types", "[stash_batch]")
{
    std::shared_ptr<mock_session> mock = mock_session::build_test_mock_session();
    WT_SESSION_IMPL *session = mock->get_wt_session_impl();
    WT_SESSION_IMPL slot;
    uint64_t hazard_gen;

    init_generations(session, &slot);
    hazard_gen = __wt_gen(session, WT_GEN_HAZARD);
    session->split_stash_batch = true;

    REQUIRE(__wt_stash_add(session, WT_GEN_HAZARD, hazard_gen, alloc_buf(session), 16) == 0);
    __wt_gen_next(session, WT_GEN_HAZARD, &hazard_gen);
    REQUIRE(__wt_stash_add(session, WT_GEN_HAZARD, hazard_gen, alloc_buf(session), 16) == 0);
    REQUIRE(stash_live(session, WT_GEN_HAZARD) == 1);

    session->split_stash_batch = false;
    __wt_gen_next(session, WT_GEN_HAZARD, NULL);
    __wt_stash_discard(session);
    REQUIRE(stash_live(session, WT_GEN_HAZARD) == 0);
    free_stash_list(session, WT_GEN_HAZARD);
}
