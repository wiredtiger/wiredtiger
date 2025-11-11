/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/* Unit tests for __rec_upd_select function */
#include <catch2/catch.hpp>
#include <vector>
#include "../wrappers/mock_session.h"
extern "C" {
#include "wt_internal.h"
#include "../../../src/reconcile/reconcile_private.h"
#include "../../../src/reconcile/reconcile_inline.h"
}
/*
 * Helper function to create a test update with specific properties
 */
static WT_UPDATE *
create_test_update(WT_SESSION_IMPL *session, const char *data, uint8_t type, uint64_t txnid,
  wt_timestamp_t start_ts, wt_timestamp_t durable_ts, uint8_t prepare_state = 0)
{
    WT_UPDATE *upd;
    size_t size;
    WT_ITEM value;

    if (data != NULL) {
        value.data = data;
        value.size = strlen(data);
    } else {
        value.data = NULL;
        value.size = 0;
    }

    // Allocate update structure
    if (__wt_upd_alloc(session, &value, type, &upd, &size) != 0)
        return NULL;

    // Set transaction ID and timestamps
    upd->txnid = txnid;
    upd->upd_start_ts = start_ts;
    upd->upd_durable_ts = durable_ts;
    upd->prepare_state = prepare_state;

    return upd;
}

/*
 * Helper function to create a chain of updates (newest first)
 */
static WT_UPDATE *
create_update_chain(WT_SESSION_IMPL *session,
  std::vector<std::tuple<const char *, uint8_t, uint64_t, wt_timestamp_t, wt_timestamp_t, uint8_t>>
    &updates)
{
    WT_UPDATE *head = NULL, *prev = NULL;

    for (auto &upd_info : updates) {
        const char *data = std::get<0>(upd_info);
        uint8_t type = std::get<1>(upd_info);
        uint64_t txnid = std::get<2>(upd_info);
        wt_timestamp_t start_ts = std::get<3>(upd_info);
        wt_timestamp_t durable_ts = std::get<4>(upd_info);
        uint8_t prepare_state = std::get<5>(upd_info);

        WT_UPDATE *upd =
          create_test_update(session, data, type, txnid, start_ts, durable_ts, prepare_state);
        if (upd == NULL)
            return NULL;

        if (head == NULL) {
            head = upd;
        } else {
            prev->next = upd;
        }
        prev = upd;
    }

    return head;
}

/*
 * Helper function to setup minimal reconciliation context
 */
static void
setup_reconcile_context(WTI_RECONCILE *r, WT_SESSION_IMPL *session, WT_PAGE *page,
  uint64_t pinned_id, wt_timestamp_t pinned_ts)
{
    memset(r, 0, sizeof(WTI_RECONCILE));
    r->page = page;
    r->rec_start_pinned_id = pinned_id;
    r->rec_start_pinned_stable_ts = pinned_ts;
    r->rec_start_oldest_id = pinned_id;
    r->rec_start_pinned_ts = pinned_ts;
    r->max_txn = WT_TXN_NONE;
    r->max_ts = WT_TS_NONE;
}

/*
 * Helper function to free update chain and insert
 */
static void
cleanup_test_data(WT_SESSION_IMPL *session, WT_INSERT *ins)
{
    WT_UPDATE *upd, *next;

    if (ins != NULL) {
        // Free update chain
        for (upd = ins->upd; upd != NULL; upd = next) {
            next = upd->next;
            __wt_free(session, upd);
        }
        // Free insert
        __wt_free(session, ins);
    }
}

/*
 * Helper function to create a test WT_INSERT structure
 */
static WT_INSERT *
create_test_insert(WT_SESSION_IMPL *session, WT_UPDATE *upd_chain)
{
    WT_INSERT *ins;
    const char *key_data = "key1";
    size_t key_size = 4;
    u_int skipdepth = 1; // Simple skiplist depth
    size_t ins_size;

    /*
     * Allocate the WT_INSERT structure, next pointers for the skip list, and room for the key. This
     * follows the same pattern as __row_insert_alloc in src/btree/row_modify.c
     */
    ins_size = sizeof(WT_INSERT) + skipdepth * sizeof(WT_INSERT *) + key_size;
    if (__wt_calloc(session, 1, ins_size, &ins) != 0)
        return NULL;

    ins->upd = upd_chain;
    ins->u.key.offset = WT_STORE_SIZE(ins_size - key_size);
    ins->u.key.size = WT_STORE_SIZE(key_size);

    // Copy the key into place
    memcpy(WT_INSERT_KEY(ins), key_data, key_size);

    return ins;
}

TEST_CASE("rec_upd_select: Basic visible update selection", "[reconcile][rec_upd_select]")
{
    // Create mock session
    std::shared_ptr<mock_session> mock = mock_session::build_test_mock_session();
    mock->setup_block_manager_file_operations();
    WT_SESSION_IMPL *session = mock->get_wt_session_impl();

    // Set up session ID and minimal transaction shared list to avoid null pointer
    session->id = 0;
    WT_TXN_SHARED *txn_shared_list;
    if (__wt_calloc(session, 1, sizeof(WT_TXN_SHARED), &txn_shared_list) != 0)
        return;
    S2C(session)->txn_global.txn_shared_list = txn_shared_list;

    // Allocate and set up transaction structure
    if (__wt_calloc(session, 1, sizeof(WT_TXN), &session->txn) != 0)
        return;

    // Set up transaction with snapshot for visibility checks
    F_SET(session->txn, WT_TXN_HAS_SNAPSHOT);
    session->txn->snapshot_data.snap_max = 200; // Set max snapshot transaction ID > our test txns
    session->txn->id = 120;
    session->txn->isolation = WT_ISO_SNAPSHOT; // Use snapshot isolation

    F_SET(S2C(session), WT_CONN_IN_MEMORY);
    F_CLR(session->dhandle, WT_DHANDLE_HS);
    // Create a simple page for the reconciliation context
    WT_PAGE *page;
    if (__wt_calloc(session, 1, sizeof(WT_PAGE), &page) != 0)
        return;
    page->type = WT_PAGE_ROW_LEAF;

    // Setup reconciliation context with pinned transaction ID 120
    WTI_RECONCILE r;
    setup_reconcile_context(&r, session, page, 120, 50);

    SECTION("Select oldest visible update for in memory")
    {
        std::vector<
          std::tuple<const char *, uint8_t, uint64_t, wt_timestamp_t, wt_timestamp_t, uint8_t>>
          updates = {
            std::make_tuple("value3", (uint8_t)WT_UPDATE_STANDARD, (uint64_t)120,
              (wt_timestamp_t)30, (wt_timestamp_t)30, (uint8_t)0),
            std::make_tuple("value2", (uint8_t)WT_UPDATE_STANDARD, (uint64_t)80, (wt_timestamp_t)20,
              (wt_timestamp_t)20, (uint8_t)0), // Visible
            std::make_tuple("value1", (uint8_t)WT_UPDATE_STANDARD, (uint64_t)50, (wt_timestamp_t)10,
              (wt_timestamp_t)10, (uint8_t)0) // Visible
          };

        WT_UPDATE *update_chain = create_update_chain(session, updates);
        if (update_chain == NULL)
            return;

        WT_INSERT *ins = create_test_insert(session, update_chain);
        if (ins == NULL) {
            __wt_free(session, update_chain);
            return;
        }

        // Initialize selection structure
        WTI_UPDATE_SELECT upd_select;
        WTI_UPDATE_SELECT_INIT(&upd_select);

        // Call the function under test
        int ret = __wti_rec_upd_select(session, &r, ins, NULL, NULL, &upd_select);

        // Verify results
        REQUIRE(ret == 0);
        REQUIRE(upd_select.upd != NULL);

        REQUIRE(upd_select.upd->txnid == 50);

        // Should track the newest transaction in max_txn
        REQUIRE(r.max_txn == 120);
        REQUIRE(r.max_ts == 30);

        // Cleanup
        cleanup_test_data(session, ins);
    }

    SECTION("Select single visible update")
    {
        // Create single visible update
        std::vector<
          std::tuple<const char *, uint8_t, uint64_t, wt_timestamp_t, wt_timestamp_t, uint8_t>>
          updates = {std::make_tuple("single_value", (uint8_t)WT_UPDATE_STANDARD, (uint64_t)50,
            (wt_timestamp_t)10, (wt_timestamp_t)10, (uint8_t)0)};

        WT_UPDATE *update_chain = create_update_chain(session, updates);
        if (update_chain == NULL)
            return;

        WT_INSERT *ins = create_test_insert(session, update_chain);
        if (ins == NULL) {
            __wt_free(session, update_chain);
            return;
        }

        // Initialize selection structure
        WTI_UPDATE_SELECT upd_select;

        // Call the function under test
        int ret = __wti_rec_upd_select(session, &r, ins, NULL, NULL, &upd_select);

        // Verify results
        REQUIRE(ret == 0);
        REQUIRE(upd_select.upd != NULL);
        REQUIRE(upd_select.upd->txnid == 50);
        REQUIRE(r.max_txn == 50);
        REQUIRE(r.max_ts == 10);

        // Cleanup
        cleanup_test_data(session, ins);
    }

    // Cleanup page, transaction, and transaction shared list
    __wt_free(session, page);
    __wt_free(session, session->txn);
    __wt_free(session, S2C(session)->txn_global.txn_shared_list);
}

TEST_CASE("rec_upd_select: Precise timestamp", "[reconcile][rec_upd_select]") {}

TEST_CASE("rec_upd_select: In-memory tree", "[reconcile][rec_upd_select]") {}
