/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>

#include "wt_internal.h"
#include "../utils.h"
#include "../wrappers/connection_wrapper.h"

static WT_UPDATE *
allocate_tombstone(WT_SESSION_IMPL *session, bool restored, size_t *sizep)
{
    WT_UPDATE *upd;

    REQUIRE(__wt_upd_alloc(session, nullptr, WT_UPDATE_TOMBSTONE, &upd, sizep) == 0);
    if (restored)
        F_SET(upd, WT_UPDATE_RESTORED_FAST_TRUNCATE);
    return (upd);
}

static WT_INSERT *
allocate_insert(WT_SESSION_IMPL *session, WT_UPDATE *upd, size_t update_size, uint64_t recno,
  size_t *sizep)
{
    WT_INSERT *ins;
    size_t ins_size;

    ins_size = sizeof(WT_INSERT) + sizeof(WT_INSERT *);
    REQUIRE(__wt_calloc(session, 1, ins_size, &ins) == 0);
    ins->upd = upd;
    WT_INSERT_RECNO(ins) = recno;
    *sizep = ins_size + update_size;
    return (ins);
}

static void
free_update_chain(WT_SESSION_IMPL *session, WT_UPDATE *upd)
{
    WT_UPDATE *next;

    while (upd != nullptr) {
        next = upd->next;
        __wt_free(session, upd);
        upd = next;
    }
}

TEST_CASE("Range-truncate accounting excludes only restored fast-truncate updates",
  "[range_truncate_update_accounting]")
{
    const char *uri = "file:cursor_test.wt";
    const std::string home = "WT_TEST.range_truncate_update_accounting";
    utils::wiredtiger_cleanup(home);
    connection_wrapper conn(home, "create,statistics=(all)");
    WT_SESSION_IMPL *session = conn.create_session();
    WT_SESSION *wt_session = &session->iface;
    WT_CURSOR *cursor;
    WT_CURSOR_BTREE *cbt;
    WT_DATA_HANDLE *saved_dhandle;
    WT_PAGE page;
    WT_PAGE_MODIFY modify;
    WT_UPDATE *head, *restored, *tombstone;
    size_t restored_size, tombstone_size, total_size;

    REQUIRE(wt_session->create(wt_session, uri, "key_format=r,value_format=S") == 0);
    REQUIRE(wt_session->open_cursor(wt_session, uri, nullptr, nullptr, &cursor) == 0);
    cbt = (WT_CURSOR_BTREE *)cursor;
    saved_dhandle = session->dhandle;
    session->dhandle = cbt->dhandle;

    WT_CLEAR(page);
    WT_CLEAR(modify);
    page.type = WT_PAGE_COL_VAR;
    page.modify = &modify;
    F_SET(&modify, WT_PAGE_MODIFY_INSTANTIATING);

    REQUIRE(wt_session->begin_transaction(wt_session, nullptr) == 0);
    session->range_truncate = true;

    int64_t truncate_bytes;
    uint64_t txn_bytes;

    truncate_bytes = WT_STAT_CONN_READ(S2C(session)->stats, truncate_slow_path_update_bytes);
    txn_bytes = session->txn->update_dirty_bytes;
    head = nullptr;
    restored = allocate_tombstone(session, true, &restored_size);
    REQUIRE(__wt_update_serial(session, cbt, &page, &head, &restored, restored_size, true) == 0);
    CHECK(WT_STAT_CONN_READ(S2C(session)->stats, truncate_slow_path_update_bytes) ==
      truncate_bytes);
    CHECK(session->txn->update_dirty_bytes == txn_bytes + restored_size);
    __wt_cache_page_inmem_decr(session, &page, restored_size);
    __wt_free(session, head);

    WT_INSERT_HEAD ins_head;
    WT_INSERT **ins_stack[WT_SKIP_MAXDEPTH];
    WT_INSERT *ins;

    WT_CLEAR(ins_head);
    WT_CLEAR(ins_stack);
    ins_stack[0] = &ins_head.head[0];
    restored = allocate_tombstone(session, true, &restored_size);
    tombstone = allocate_tombstone(session, false, &tombstone_size);
    tombstone->next = restored;
    ins = allocate_insert(
      session, tombstone, tombstone_size + restored_size, 1, &total_size);
    truncate_bytes = WT_STAT_CONN_READ(S2C(session)->stats, truncate_slow_path_update_bytes);
    txn_bytes = session->txn->update_dirty_bytes;
    REQUIRE(__wt_insert_serial(
              session, &page, &ins_head, ins_stack, &ins, total_size, 1, true) == 0);
    CHECK(WT_STAT_CONN_READ(S2C(session)->stats, truncate_slow_path_update_bytes) ==
      truncate_bytes + tombstone_size);
    CHECK(session->txn->update_dirty_bytes == txn_bytes + total_size);
    __wt_cache_page_inmem_decr(session, &page, total_size);
    free_update_chain(session, ins_head.head[0]->upd);
    __wt_free(session, ins_head.head[0]);

    WT_CLEAR(ins_head);
    WT_CLEAR(ins_stack);
    ins_stack[0] = &ins_head.head[0];
    restored = allocate_tombstone(session, true, &restored_size);
    tombstone = allocate_tombstone(session, false, &tombstone_size);
    tombstone->next = restored;
    ins = allocate_insert(
      session, tombstone, tombstone_size + restored_size, 2, &total_size);
    truncate_bytes = WT_STAT_CONN_READ(S2C(session)->stats, truncate_slow_path_update_bytes);
    txn_bytes = session->txn->update_dirty_bytes;
    uint64_t recno = 2;
    REQUIRE(__wt_col_append_serial(
              session, &page, &ins_head, ins_stack, &ins, total_size, &recno, 1, true) == 0);
    CHECK(WT_STAT_CONN_READ(S2C(session)->stats, truncate_slow_path_update_bytes) ==
      truncate_bytes + tombstone_size);
    CHECK(session->txn->update_dirty_bytes == txn_bytes + total_size);
    __wt_cache_page_inmem_decr(session, &page, total_size);
    free_update_chain(session, ins_head.head[0]->upd);
    __wt_free(session, ins_head.head[0]);

    session->range_truncate = false;
    REQUIRE(wt_session->rollback_transaction(wt_session, nullptr) == 0);
    session->dhandle = saved_dhandle;
    REQUIRE(cursor->close(cursor) == 0);
}