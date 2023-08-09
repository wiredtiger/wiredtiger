/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *      All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

// #ifndef WT_MOCK_CONNECTION_H
#define WT_MOCK_CONNECTION_H

#include <memory>
#include "wt_internal.h"
// #include "txn.h"
#include "item_wrapper.h"

/*
 * Prefer a mock class over a "real" one when the operations you want to perform don't need a fully
 * fleshed-out connection (or session). There are large speed advantages here, since the real thing
 * will write a bunch of files to disk during the test, which also need to be removed.
 */
class MockTxnOp {
public:
    MockTxnOp()
    {
        op_type = WT_TXN_OP_NONE;
    };
    ~MockTxnOp();

    void set_optype(WT_TXN_TYPE type)
    {
        op_type = type;
    }

    void set_btreeid(int id)
    {
        btree.id = id;
    }

    void set_opid(int id)
    {
        op_identifer = id;
    }

    WT_TXN_TYPE op_type;
    int op_identifer = -1;

    struct {
        int id;
        WT_BTREE_TYPE type;
        WT_COLLATOR *collator = NULL;
    } btree;

    /* WT_TXN_OP_BASIC_ROW, WT_TXN_OP_INMEM_ROW */
    struct {
        item_wrapper *item_key;
    } op_row;

    struct {
        int key;
    } op_row;

    /* WT_TXN_OP_BASIC_COL, WT_TXN_OP_INMEM_COL */
    struct {
        uint64_t recno = 0;
    } op_col;

    /* WT_TXN_OP_REF_DELETE */

    /* WT_TXN_OP_TRUNCATE_COL */
    struct {
        uint64_t start, stop = 0;
    } truncate_col;
    /* WT_TXN_OP_TRUNCATE_ROW */
    struct {
        item_wrapper *start, *stop;
    } truncate_row;
};
