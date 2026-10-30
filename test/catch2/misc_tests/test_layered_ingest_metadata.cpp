/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#ifndef _WIN32

#include <catch2/catch.hpp>
#include <string>

#include "wt_internal.h"
#include "../wrappers/connection_wrapper.h"

static const char *conn_cfg =
  "create,cache_cursors=false,statistics=(all),"
  "extensions=[./ext/page_log/palite/libwiredtiger_palite.so],"
  "disaggregated=(role=follower,page_log=palite)";
static const char *file_cfg =
  "key_format=S,value_format=S,allocation_size=512,internal_page_max=4096,leaf_page_max=8192";

/*
 * open_ingest_id --
 *     Open an ingest cursor and return its runtime file ID.
 */
static uint32_t
open_ingest_id(WT_SESSION *session, const char *uri, uint32_t allocation_size = 512,
  uint32_t leaf_page_max = 8192)
{
    WT_CURSOR *cursor = nullptr;
    REQUIRE(session->open_cursor(session, uri, nullptr, nullptr, &cursor) == 0);
    WT_BTREE *btree = CUR2BT(cursor);
    CHECK(F_ISSET(btree, WT_BTREE_IN_MEMORY));
    CHECK_FALSE(F_ISSET(btree, WT_BTREE_DISAGGREGATED | WT_BTREE_LOGGED));
    CHECK(btree->allocsize == allocation_size);
    CHECK(btree->maxintlpage == 4096);
    CHECK(btree->maxleafpage == leaf_page_max);
    uint32_t id = btree->id;
    REQUIRE(cursor->close(cursor) == 0);
    return (id);
}

/*
 * check_file_config --
 *     Check file settings and the absence of persistent file identity and checkpoint state.
 */
static void
check_file_config(WT_SESSION_IMPL *session, const char *config, uint32_t allocation_size = 512,
  uint32_t leaf_page_max = 8192)
{
    WT_CONFIG_ITEM cval;
    REQUIRE(__wt_config_getones(session, config, "allocation_size", &cval) == 0);
    CHECK(cval.val == allocation_size);
    REQUIRE(__wt_config_getones(session, config, "leaf_page_max", &cval) == 0);
    CHECK(cval.val == leaf_page_max);
    CHECK(__wt_config_getones(session, config, "id", &cval) == WT_NOTFOUND);
    CHECK(__wt_config_getones(session, config, "checkpoint", &cval) == WT_NOTFOUND);
}

/*
 * metadata_file_id --
 *     Return a persistent constituent's file ID.
 */
static uint32_t
metadata_file_id(WT_SESSION_IMPL *session, const char *uri)
{
    char *config = nullptr;
    WT_CONFIG_ITEM id;
    REQUIRE(__wt_metadata_search(session, uri, &config) == 0);
    REQUIRE(__wt_config_getones(session, config, "id", &id) == 0);
    uint32_t file_id = static_cast<uint32_t>(id.val);
    __wt_free(session, config);
    return (file_id);
}

TEST_CASE("Stable file identity is independent of ingest opens", "[layered_ingest_metadata]")
{
    uint32_t leader_id;
    const char *uri = "layered:stable_identity";
    const char *stable_uri = "file:stable_identity.wt_stable";
    {
        std::string cfg = std::string(conn_cfg) + ",disaggregated=(role=leader,page_log=palite)";
        connection_wrapper conn("WT_TEST.ingest_id_leader", cfg.c_str());
        WT_SESSION_IMPL *session = conn.create_session();
        REQUIRE(session->iface.create(&session->iface, uri, file_cfg) == 0);
        leader_id = metadata_file_id(session, stable_uri);
    }
    {
        connection_wrapper conn("WT_TEST.ingest_id_follower", conn_cfg);
        WT_SESSION_IMPL *session = conn.create_session();
        REQUIRE(session->iface.create(&session->iface, uri, file_cfg) == 0);
        open_ingest_id(&session->iface, "file:stable_identity.wt_ingest");
        WT_CONNECTION *iface = conn.get_wt_connection();
        REQUIRE(iface->reconfigure(iface, "disaggregated=(role=leader)") == 0);
        CHECK(metadata_file_id(session, stable_uri) == leader_id);
    }
}

TEST_CASE("Metadata-free ingest identity survives handle replacement", "[layered_ingest_metadata]")
{
    connection_wrapper conn("WT_TEST.ingest_metadata", conn_cfg);
    WT_SESSION_IMPL *session = conn.create_session();
    WT_SESSION *iface = &session->iface;
    const char *layered_uri = "layered:identity";
    const char *ingest_uri = "file:identity.wt_ingest";

    REQUIRE(iface->create(iface, layered_uri, file_cfg) == 0);
    WT_CURSOR *cursor = nullptr;
    REQUIRE(iface->open_cursor(iface, layered_uri, nullptr, nullptr, &cursor) == 0);
    REQUIRE(cursor->close(cursor) == 0);
    uint32_t first_id = open_ingest_id(iface, ingest_uri);
    CHECK(WT_BTREE_ID_NAMESPACE_ID(first_id) == WT_BTREE_ID_NAMESPACE_INGEST);
    CHECK_FALSE(WT_BTREE_ID_SHARED(first_id));

    bool mark_dead = GENERATE(false, true);
    int ret;
    WT_WITH_SCHEMA_LOCK(session,
      WT_WITH_HANDLE_LIST_WRITE_LOCK(
        session, ret = __wt_conn_dhandle_close_all(session, ingest_uri, false, mark_dead, false)));
    REQUIRE(ret == 0);
    CHECK(open_ingest_id(iface, ingest_uri) == first_id);

    REQUIRE(iface->create(iface, "layered:other", file_cfg) == 0);
    CHECK(open_ingest_id(iface, "file:other.wt_ingest") != first_id);
    char *metadata = nullptr;
    REQUIRE(__wt_metadata_search(session, ingest_uri, &metadata) == WT_NOTFOUND);
    REQUIRE(__wt_metadata_search(session, "file:other.wt_ingest", &metadata) == WT_NOTFOUND);
}

TEST_CASE("Older shared layered metadata supplies ingest settings", "[layered_ingest_metadata]")
{
    bool have_stable = GENERATE(false, true);
    std::string open_cfg = std::string(conn_cfg) +
      ",disaggregated=(page_log=palite,role=" + (have_stable ? "leader" : "follower") + ")";
    connection_wrapper conn("WT_TEST.ingest_old_parent", open_cfg.c_str());
    WT_SESSION_IMPL *session = conn.create_session();
    WT_SESSION *iface = &session->iface;
    const char *layered_uri = "layered:old_parent";
    const char *ingest_uri = "file:old_parent.wt_ingest";
    const char *old_layered_cfg =
      "key_format=S,value_format=S,ingest=\"file:old_parent.wt_ingest\","
      "stable=\"file:old_parent.wt_stable\",log=(enabled=false)";
    REQUIRE(iface->create(iface, layered_uri, file_cfg) == 0);
    REQUIRE(__wt_metadata_update(session, layered_uri, old_layered_cfg) == 0);

    uint32_t allocation_size = have_stable ? 512 : 4096;
    uint32_t leaf_page_max = have_stable ? 8192 : 32768;
    char *config = nullptr;
    REQUIRE(__wt_schema_layered_file_config(session, old_layered_cfg, &config) == 0);
    check_file_config(session, config, allocation_size, leaf_page_max);
    __wt_free(session, config);
    CHECK(WT_BTREE_ID_NAMESPACE_ID(open_ingest_id(
            iface, ingest_uri, allocation_size, leaf_page_max)) == WT_BTREE_ID_NAMESPACE_INGEST);
    CHECK(__wt_metadata_search(session, ingest_uri, &config) == WT_NOTFOUND);
    REQUIRE(iface->drop(iface, layered_uri, nullptr) == 0);
}

TEST_CASE("Inferred ingest opens require a matching parent", "[layered_ingest_metadata]")
{
    connection_wrapper conn("WT_TEST.ingest_invalid", conn_cfg);
    WT_SESSION_IMPL *session = conn.create_session();
    WT_SESSION *iface = &session->iface;
    WT_CURSOR *cursor = nullptr;
    CHECK(iface->open_cursor(iface, "file:missing.wt_ingest", nullptr, nullptr, &cursor) == ENOENT);
    CHECK(cursor == nullptr);
    CHECK(iface->open_cursor(iface, "file:.wt_ingest", nullptr, nullptr, &cursor) == EINVAL);
    CHECK(cursor == nullptr);

    REQUIRE(iface->create(iface, "layered:mismatch", file_cfg) == 0);
    REQUIRE(__wt_metadata_update(session, "layered:mismatch",
              "key_format=S,value_format=S,ingest=\"file:other.wt_ingest\","
              "stable=\"file:mismatch.wt_stable\",log=(enabled=false)") == 0);
    CHECK(
      iface->open_cursor(iface, "file:mismatch.wt_ingest", nullptr, nullptr, &cursor) == EINVAL);
    CHECK(cursor == nullptr);
    REQUIRE(iface->drop(iface, "layered:mismatch", nullptr) == 0);
}

#endif /* !_WIN32 */
