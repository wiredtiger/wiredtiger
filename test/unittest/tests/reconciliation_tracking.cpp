#include <catch2/catch.hpp>

#include "wiredtiger.h"
#include "wrappers/connection_wrapper.h"
#include "wt_internal.h"

TEST_CASE("ofvl_track_init", "[reconciliation]")
{
    auto conn = ConnectionWrapper();
    auto session = conn.createSession();

    WT_PAGE p;
    memset(&p, sizeof(p), 0);

    WT_PAGE_MODIFY m;
    memset(&m, sizeof(m), 0);

    p.modify = &m;

    REQUIRE(__wt_ovfl_track_init(session, &p) == 0);
    REQUIRE(m.ovfl_track != nullptr);
}

TEST_CASE("ovfl_discard_verbose", "[reconciliation]")
{
    auto conn = ConnectionWrapper();
    auto session = conn.createSession();

    SECTION("handle null page and tag")
    {
        REQUIRE(__ut_ovfl_discard_verbose(session, nullptr, nullptr, nullptr) == 0);
    }
}

TEST_CASE("ovfl_discard_wrapup", "[reconciliation]")
{
    auto conn = ConnectionWrapper();
    auto session = conn.createSession();

    WT_PAGE p;
    memset(&p, sizeof(p), 0);

    WT_PAGE_MODIFY m;
    memset(&m, sizeof(m), 0);
    p.modify = &m;

    REQUIRE(__wt_ovfl_track_init(session, &p) == 0);

    SECTION("handle empty overflow entry list")
    {
        REQUIRE(__ut_ovfl_discard_wrapup(session, &p) == 0);
    }
}
