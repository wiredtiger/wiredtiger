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

extern bool __ut_compact_fail_strndup;

static std::shared_ptr<mock_session>
compact_addr_replace_fail_setup(WT_REF *ref, WT_ADDR *addr, const char *cookie_data)
{
    std::shared_ptr<mock_session> session_mock = mock_session::build_test_mock_session();
    session_mock->setup_block_manager_file_operations();
    WT_SESSION_IMPL *session = session_mock->get_wt_session_impl();

    memset(addr, 0, sizeof(*addr));

    size_t cookie_len = strlen(cookie_data) + 1;
    void *cookie = nullptr;
    REQUIRE(__wt_strndup(session, cookie_data, cookie_len, &cookie) == 0);
    addr->block_cookie = (uint8_t *)cookie;
    addr->block_cookie_size = (uint8_t)cookie_len;
    addr->type = WT_ADDR_LEAF;

    ref->addr = addr;

    return (session_mock);
}

TEST_CASE("compact_page_replace_addr: allocation failure leaves existing address state untouched",
  "[compact_addr_replace_fail]")
{
    WT_PAGE home_page;
    memset(&home_page, 0, sizeof(home_page));
    home_page.dsk = nullptr;

    WT_REF ref;
    memset(&ref, 0, sizeof(ref));
    ref.home = &home_page;

    WT_ADDR addr;
    const char *original_cookie = "original_test_block_cookie";
    std::shared_ptr<mock_session> session_mock =
      compact_addr_replace_fail_setup(&ref, &addr, original_cookie);
    WT_SESSION_IMPL *session = session_mock->get_wt_session_impl();

    uint8_t *saved_cookie = addr.block_cookie;
    uint8_t saved_cookie_size = addr.block_cookie_size;

    WT_ADDR_COPY copy;
    memset(&copy, 0, sizeof(copy));
    copy.type = WT_ADDR_LEAF;
    copy.size = 123;
    const char *new_cookie_data = "new_dummy_cookie";
    memcpy(copy.addr, new_cookie_data, strlen(new_cookie_data) + 1);

    WT_SPINLOCK *flush_lock = &S2BT(session)->flush_lock;
    __wt_spin_lock(session, flush_lock);

    __ut_compact_fail_strndup = true;
    int result = __ut_compact_page_replace_addr(session, &ref, &copy);
    __ut_compact_fail_strndup = false;

    __wt_spin_unlock(session, flush_lock);

    REQUIRE(result == ENOMEM);
    REQUIRE(ref.addr == &addr);
    REQUIRE(addr.block_cookie == saved_cookie);
    REQUIRE(addr.block_cookie_size == saved_cookie_size);

    __wt_free(session, addr.block_cookie);
}
