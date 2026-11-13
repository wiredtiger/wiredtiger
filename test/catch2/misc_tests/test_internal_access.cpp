/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <array>
#include <catch2/catch.hpp>
#include <string>

#include "wt_internal.h"
#include "../wrappers/mock_session.h"

namespace {

#ifdef HAVE_DIAGNOSTIC
const bool diagnostic_build = true;
#else
const bool diagnostic_build = false;
#endif

struct name_case {
    const char *name;
    bool global_internal;
    bool constituent_pattern;
};

const std::array<const char *, 13> prefixes = {"",
  "file:", "table:", "layered:", "colgroup:", "index:", "tiered:", "custom:", "statistics:file:",
  "statistics:table:", "statistics:layered:", "colgroup:parent:", "index:parent:"};

const std::array<name_case, 13> protected_names = {
  {{"orders", false, false}, {"orders.wt", false, false}, {"orders.wt_ingest", false, true},
    {"orders.wt_stable", false, true}, {"orders.wt_stable/WiredTigerCheckpoint.1", false, true},
    {WT_METAFILE, true, false}, {WT_HS_FILE, true, false}, {WT_DISAGG_METADATA_FILE, true, true},
    {WT_HS_FILE_SHARED, true, true}, {"WiredTiger.wt/WiredTigerCheckpoint.1", true, false},
    {"WiredTigerHS.wt/WiredTigerCheckpoint.1", true, false},
    {"WiredTigerShared.wt_stable/WiredTigerCheckpoint.1", true, true},
    {"WiredTigerSharedHS.wt_stable/WiredTigerCheckpoint.1", true, true}}};

const std::array<name_case, 9> lookalikes = {
  {{"", false, false}, {"orders.wt_ingest_other", false, false},
    {"orders.wt_stable_other", false, true}, {"WiredTiger.wt_other", false, false},
    {"WiredTigerHS.wt_other", false, false}, {"WiredTigerShared.wt_stable_other", false, true},
    {"WiredTigerSharedHS.wt_stable_other", false, true}, {"wiredtiger.wt", false, false},
    {"orders.WT_STABLE", false, false}}};

class access_fixture {
public:
    access_fixture()
        : mock(mock_session::build_test_mock_session()), session(mock->get_wt_session_impl())
    {
        session->api_call_counter = 1;
    }

    ~access_fixture()
    {
        S2C(session)->disaggregated_storage.page_log_meta = nullptr;
    }

    void
    set_disaggregated(bool enabled)
    {
        /* Only the presence of the handle identifies the mode; these tests perform no page I/O. */
        S2C(session)->disaggregated_storage.page_log_meta = enabled ? &page_log : nullptr;
    }

    std::shared_ptr<mock_session> mock;
    WT_SESSION_IMPL *session;

private:
    WT_PAGE_LOG_HANDLE page_log{};
};

} // namespace

TEST_CASE("Internal access extracts object names from URI prefixes", "[internal_access]")
{
    const auto input = GENERATE(Catch::Generators::table<const char *, const char *>(
      {{nullptr, nullptr}, {"", ""}, {"orders", "orders"}, {"file:", ""},
        {"file:orders.wt", "orders.wt"}, {"table:orders", "orders"}, {"layered:orders", "orders"},
        {"colgroup:orders:group", "group"}, {"index:orders:index", "index"},
        {"statistics:file:WiredTigerHS.wt", "WiredTigerHS.wt"},
        {"file:orders.wt_stable/WiredTigerCheckpoint.1", "orders.wt_stable/WiredTigerCheckpoint.1"},
        {"custom:parent:child:orders", "orders"}, {"table:orders(value)", "orders(value)"}}));
    const char *uri = std::get<0>(input);
    const char *expected = std::get<1>(input);
    CAPTURE(uri);
    const char *actual = __wt_uri_name(uri);
    if (expected == nullptr)
        REQUIRE(actual == nullptr);
    else {
        REQUIRE(actual != nullptr);
        REQUIRE(std::string(actual) == expected);
    }
}

TEST_CASE("Internal access classifies protected names across prefixes and storage modes",
  "[internal_access]")
{
    const auto input = GENERATE(Catch::Generators::from_range(protected_names));
    const char *prefix = GENERATE(Catch::Generators::from_range(prefixes));
    const bool disaggregated = GENERATE(false, true);
    access_fixture fixture;
    fixture.set_disaggregated(disaggregated);
    const std::string uri = std::string(prefix) + input.name;
    const char *name = __wt_uri_name(uri.c_str());
    CAPTURE(uri, disaggregated);
    REQUIRE(__wt_session_is_global_internal(name) == input.global_internal);
    REQUIRE(__wt_session_is_layered_internal(fixture.session, name) ==
      (disaggregated && input.constituent_pattern));
}

TEST_CASE("Internal access distinguishes lookalikes from exact system names", "[internal_access]")
{
    const auto input = GENERATE(Catch::Generators::from_range(lookalikes));
    const char *prefix = GENERATE(Catch::Generators::from_range(prefixes));
    const bool disaggregated = GENERATE(false, true);
    access_fixture fixture;
    fixture.set_disaggregated(disaggregated);
    const std::string uri = std::string(prefix) + input.name;
    const char *name = __wt_uri_name(uri.c_str());
    CAPTURE(uri, disaggregated);
    REQUIRE(__wt_session_is_global_internal(name) == input.global_internal);
    REQUIRE(__wt_session_is_layered_internal(fixture.session, name) ==
      (disaggregated && input.constituent_pattern));
    REQUIRE_FALSE(__wt_session_is_global_internal(nullptr));
    REQUIRE_FALSE(__wt_session_is_layered_internal(fixture.session, nullptr));
}

TEST_CASE("Internal access validates direct URI targets without an opt-in", "[internal_access]")
{
    const auto input = GENERATE(Catch::Generators::from_range(protected_names));
    const char *prefix = GENERATE(Catch::Generators::from_range(prefixes));
    const bool disaggregated = GENERATE(false, true);
    access_fixture fixture;
    fixture.set_disaggregated(disaggregated);
    const std::string uri = std::string(prefix) + input.name;
    const bool restricted = input.global_internal || (disaggregated && input.constituent_pattern);
    CAPTURE(uri, disaggregated);
    REQUIRE(__wt_uri_name_check(fixture.session, uri.c_str()) ==
      (diagnostic_build && restricted ? EINVAL : 0));
    if (diagnostic_build && restricted) {
        const std::string &message = fixture.mock->get_last_message();
        REQUIRE(
          message.find("direct access to an internal table is not allowed") != std::string::npos);
        REQUIRE(message.find("debug=(allow_internal_access=true)") != std::string::npos);
        REQUIRE(message.find(uri) != std::string::npos);
    }
}

TEST_CASE(
  "Internal access respects operation policy and session entry context", "[internal_access]")
{
    const auto input = GENERATE(Catch::Generators::from_range(protected_names));
    const bool disaggregated = GENERATE(false, true);
    const bool schema = GENERATE(false, true);
    const unsigned int depth = GENERATE(0u, 1u, 2u);
    const uint32_t flags = GENERATE(uint32_t{0}, uint32_t{WT_SESSION_INTERNAL},
      uint32_t{WT_SESSION_DEBUG_ALLOW_INTERNAL_ACCESS},
      uint32_t{WT_SESSION_INTERNAL | WT_SESSION_DEBUG_ALLOW_INTERNAL_ACCESS});
    access_fixture fixture;
    fixture.set_disaggregated(disaggregated);
    fixture.session->api_call_counter = depth;
    F_SET(fixture.session, flags);
    const std::string uri = std::string("file:") + input.name;
    const bool global = __wt_session_is_global_internal(input.name);
    const bool layered = __wt_session_is_layered_internal(fixture.session, input.name);
    const bool condition = schema ? global || layered : layered && !global;
    const bool expected_condition = schema ?
      input.global_internal || (disaggregated && input.constituent_pattern) :
      disaggregated && input.constituent_pattern && !input.global_internal;
    const bool reject = diagnostic_build && expected_condition && depth == 1 && flags == 0;
    CAPTURE(uri, disaggregated, schema, depth, flags);
    REQUIRE(condition == expected_condition);
    REQUIRE(__wt_session_check_internal_uri(fixture.session, uri.c_str(), condition) ==
      (reject ? EINVAL : 0));
}

TEST_CASE("Internal access checks the supplied length of a configuration URI", "[internal_access]")
{
    const auto input = GENERATE(Catch::Generators::table<const char *, int>(
      {{"file:orders.wt", 0}, {"file:WiredTiger.wt", EINVAL}, {"file:WiredTigerHS.wt", EINVAL},
        {"file:orders.wt_stable", EINVAL}}));
    access_fixture fixture;
    fixture.set_disaggregated(true);
    const char *uri = std::get<0>(input);
    const std::string buffer = std::string(uri) + ",other_config=value";
    CAPTURE(uri);
    REQUIRE(__wt_name_check(fixture.session, buffer.data(), strlen(uri), true) ==
      (diagnostic_build ? std::get<1>(input) : 0));
}

TEST_CASE("Internal access reserves colon-qualified namespace names", "[internal_access]")
{
    const auto input = GENERATE(Catch::Generators::table<const char *, bool>(
      {{"WiredTigerApp", false}, {"file:WiredTigerApp.wt", true}, {"table:WiredTigerApp", true},
        {"layered:WiredTigerApp", true}, {"colgroup:orders:WiredTigerGroup", true},
        {"index:orders:WiredTigerIndex", true}, {"statistics:file:WiredTigerApp.wt", true},
        {"custom:WiredTigerApp", true}, {"file:ordinary.wt", false}}));
    const unsigned int depth = GENERATE(0u, 1u, 2u);
    const uint32_t flags = GENERATE(uint32_t{0}, uint32_t{WT_SESSION_INTERNAL},
      uint32_t{WT_SESSION_DEBUG_ALLOW_INTERNAL_ACCESS},
      uint32_t{WT_SESSION_INTERNAL | WT_SESSION_DEBUG_ALLOW_INTERNAL_ACCESS});
    access_fixture fixture;
    fixture.session->api_call_counter = depth;
    F_SET(fixture.session, flags);
    const char *uri = std::get<0>(input);
    const bool reject = diagnostic_build && std::get<1>(input) && depth == 1 && flags == 0;
    CAPTURE(uri, depth, flags);
    REQUIRE(__wt_uri_name_check(fixture.session, uri) == (reject ? EINVAL : 0));
    if (reject)
        REQUIRE(
          fixture.mock->get_last_message().find("name space may not be used") != std::string::npos);
}

TEST_CASE("Internal access opt-in preserves URI syntax validation", "[internal_access]")
{
    access_fixture fixture;
    F_SET(fixture.session, WT_SESSION_DEBUG_ALLOW_INTERNAL_ACCESS);
    REQUIRE(__wt_uri_name_check(fixture.session, "table:WiredTigerBad,name") == EINVAL);
    REQUIRE(fixture.mock->get_last_message().find("grouping characters") != std::string::npos);
}
