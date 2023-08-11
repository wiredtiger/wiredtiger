/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <algorithm>
#include <list>
#include <vector>

#include <catch2/catch.hpp>

#include "wt_internal.h"
#include "wrappers/item_wrapper.h"

#include <iostream>

namespace {

bool has_key(WT_TXN_TYPE type) {
    switch (type) {
    case (WT_TXN_OP_NONE):
    case (WT_TXN_OP_REF_DELETE):
    case (WT_TXN_OP_TRUNCATE_COL):
    case (WT_TXN_OP_TRUNCATE_ROW):
        return (false);
    case (WT_TXN_OP_BASIC_COL):
    case (WT_TXN_OP_BASIC_ROW):
    case (WT_TXN_OP_INMEM_COL):
    case (WT_TXN_OP_INMEM_ROW):
        return (true);
    }
    return (false);
}

static bool WT_CDECL
__mod_ops_sorted(WT_TXN_OP *ops, int op_count)
{
    WT_TXN_OP *aopt, *bopt;
    int i;

    for(i=0; i < op_count - 1; i++){
        aopt = &ops[i];
        bopt = &ops[i+1];

        /* A non-key'd operation cannot come before a key'd operation. */
        if(has_key(bopt->type) && !has_key(aopt->type))
            return(false);

        // Non key'd operations can separate any modifications with keys.
        if((aopt->btree->id == bopt->btree->id) && (!has_key(bopt->type) || !has_key(aopt->type)))
            return (true);

        /* B-tree ids must be in ascending order.*/
        if((aopt->btree->id > bopt->btree->id) && has_key(bopt->type))
            return (false);

        /* Check the key/recno if btree ids are the same. */
        if(aopt->btree->id == bopt->btree->id){
            // if (aopt->btree.type == BTREE_ROW) {
            //     WT_ITEM a_key = *aopt.op_row.item_key->get_item();
            //     auto a_data = a_key.data;

            //     WT_ITEM b_key = *aopt.op_row.item_key->get_item();
            //     auto b_data = b_key.data;

            //     if(a_data > b_data)
            //         return (false);
            // }

            if (aopt->u.op_col.recno > bopt->u.op_col.recno)
                return (false);
        }
    }
    return (true);
}

void
init_btree(WT_BTREE *btree, WT_BTREE_TYPE type, uint32_t id) {
    btree->type = type;
    btree->id = id;
}

void
init_op(WT_TXN_OP *op, WT_BTREE *btree, WT_TXN_TYPE type, uint64_t recno, WT_ITEM *key) {
    op->btree = btree;
    op->type = type;
    if (op->type == WT_TXN_OP_BASIC_COL || op->type == WT_TXN_OP_INMEM_COL) {
        REQUIRE(recno != WT_RECNO_OOB);
        op->u.op_col.recno = recno;
    } else if (op->type == WT_TXN_OP_BASIC_ROW || op->type == WT_TXN_OP_INMEM_ROW) {
        REQUIRE(key != NULL);
        op->u.op_row.key = *key;
    } else {
        REQUIRE(op->type == WT_TXN_OP_NONE);
    }
}

TEST_CASE("Basic cols and op_none", "[mod_compare]")
{
    WT_BTREE btrees[2];
    WT_TXN_OP ops[2];

    init_btree(&btrees[0], BTREE_ROW, 1);
    init_btree(&btrees[1], BTREE_COL_VAR, 2);
    init_op(&ops[1], &btrees[0], WT_TXN_OP_NONE, WT_RECNO_OOB, NULL);
    init_op(&ops[0], &btrees[1], WT_TXN_OP_BASIC_COL, 54, NULL);

    __wt_qsort_r(ops, 2, sizeof(WT_TXN_OP), __ut_txn_mod_compare, NULL);
    __mod_ops_sorted(ops, 2);
    // item_wrapper op1_key("1");
    // std::vector<MockTxnOp> input{};

    // // &op1->op_type = WT_TXN_OP_NONE;
    // op1.set_optype(WT_TXN_OP_NONE);
    // op1.set_btreeid(2);
    // op1.set_opid(1);

    // op1.op_row.item_key = &op1_key;

    // /* WT_TXN_OP_REF_DELETE */
    // op2.btree.type = BTREE_COL_VAR;
    // op2.op_col.recno = 54;
    // op2.set_btreeid(1);
    // op2.set_optype(WT_TXN_OP_BASIC_COL);
    // op2.set_opid(2);

    // op3.set_btreeid(1);
    // op3.btree.type = BTREE_COL_VAR;
    // op3.op_col.recno = 60;
    // op3.set_optype(WT_TXN_OP_BASIC_COL);
    // op3.set_opid(3);

    // input.push_back(op1);
    // input.push_back(op2);
    // input.push_back(op3);

    // // Should be sorted 2->3->1
    // int count = 0;
    // __wt_qsort_r(&input[0], input.size(), sizeof(input[0]), __ut_txn_mod_compare, &count);
    // std::cout << "First element" << input[0].op_identifer;
    // std::cout << "Second element" << input[1].op_identifer;
    // std::cout << "Third element" << input[2].op_identifer;
    // std::cout << std::endl;
    // CHECK(__mod_ops_sorted(input));
}

// TEST_CASE("Basic rows and op_none", "[mod_compare]")
// {
//     MockTxnOp op1, op2, op3, op4 = MockTxnOp();
//     std::vector<MockTxnOp> input{};

//     // &op1->op_type = WT_TXN_OP_NONE;
//     op1.set_optype(WT_TXN_OP_NONE);
//     // op1.btree->type = BTREE_
//     op1.set_btreeid(1);
//     op1.set_opid(1);

//     /* WT_TXN_OP_REF_DELETE */
//     op2.set_btreeid(1);
//     op2.set_optype(WT_TXN_OP_BASIC_ROW);
//     item_wrapper op2_key("5");
//     op2.op_row.item_key = &op2_key;
//     op2.set_opid(2);

//     op3.set_btreeid(2);
//     op3.set_optype(WT_TXN_OP_BASIC_ROW);
//     item_wrapper op3_key("5");
//     op3.op_row.item_key = &op3_key;
//     op3.set_opid(3);

//     op4.set_btreeid(2);
//     op4.set_optype(WT_TXN_OP_BASIC_ROW);
//     item_wrapper op4_key("1");
//     op4.op_row.item_key = &op4_key;
//     op4.set_opid(4);

//     input.push_back(op1);
//     input.push_back(op2);
//     input.push_back(op3);
//     input.push_back(op4);

//     // expected output
//     // op2, op4, op3, op1
//     int count = 0;
//     __wt_qsort_r(&input[0], input.size(), sizeof(input[0]), __ut_txn_mod_compare, &count);

//     std::cout << "First element" << input[0].op_identifer;
//     std::cout << "Second element" << input[1].op_identifer;
//     std::cout << "Third element" << input[2].op_identifer;
//     std::cout << "Fourth element" << input[3].op_identifer;
//     std::cout << std::endl;
//     CHECK(__mod_ops_sorted(input));
// }

// TEST_CASE("Basic rows and op truncate col", "[mod_compare]")
// {
//     MockTxnOp op1, op2, op3, op4, op5 = MockTxnOp();
//     std::vector<MockTxnOp> input{};

//     // &op1->op_type = WT_TXN_OP_NONE;
//     op1.set_optype(WT_TXN_OP_NONE);
//     // op1.btree->type = BTREE_
//     op1.set_btreeid(1);
//     op1.set_opid(1);

//     /* WT_TXN_OP_REF_DELETE */
//     op2.set_btreeid(5);
//     op2.set_optype(WT_TXN_OP_BASIC_ROW);
//     item_wrapper op2_key("10");
//     op2.op_row.item_key = &op2_key;
//     op2.set_opid(2);

//     op3.set_btreeid(5);
//     op3.set_optype(WT_TXN_OP_BASIC_ROW);
//     item_wrapper op3_key("8");
//     op3.op_row.item_key = &op3_key;
//     op3.set_opid(3);

//     op4.set_btreeid(1);
//     op4.set_optype(WT_TXN_OP_BASIC_ROW);
//     item_wrapper op4_key("1");
//     op4.op_row.item_key = &op4_key;
//     op4.set_opid(4);

//     op5.set_optype(WT_TXN_OP_TRUNCATE_COL);
//     // op1.btree->type = BTREE_
//     op5.set_btreeid(4);
//     op5.set_opid(5);

//     input.push_back(op1);
//     input.push_back(op2);
//     input.push_back(op3);
//     input.push_back(op4);
//     input.push_back(op5);

//     // expected output
//     // 4-> 1-> 2-> 3
//     int count = 0;
//     __wt_qsort_r(&input[0], input.size(), sizeof(input[0]), __ut_txn_mod_compare, &count);

//     std::cout << "First element" << input[0].op_identifer;
//     std::cout << "Second element" << input[1].op_identifer;
//     std::cout << "Third element" << input[2].op_identifer;
//     std::cout << "Fourth element" << input[3].op_identifer;
//     std::cout << "Fifth element" << input[4].op_identifer;
//     std::cout << std::endl;
//     CHECK(__mod_ops_sorted(input));
// }

// TEST_CASE("Basic cols and other non key'd ops", "[mod_compare]")
// {
//     MockTxnOp op1, op2, op3, op4 = MockTxnOp();
//     std::vector<MockTxnOp> input{};

//     // &op1->op_type = WT_TXN_OP_NONE;
//     op1.set_optype(WT_TXN_OP_REF_DELETE);
//     op1.set_btreeid(1);
//     op1.set_opid(1);

//     op2.set_optype(WT_TXN_OP_NONE);
//     op2.set_btreeid(2);
//     op2.set_opid(2);

//     op3.set_btreeid(1);
//     op3.set_optype(WT_TXN_OP_INMEM_COL);
//     op3.btree.type = BTREE_COL_VAR;
//     op3.op_col.recno = 10;
//     op3.set_opid(3);

//     op4.set_btreeid(1);
//     // op4.btree.type = BTREE_COL;
//     op4.set_optype(WT_TXN_OP_INMEM_COL);
//     op4.btree.type = BTREE_COL_VAR;
//     // op4.op_col.recno = 6;
//     op4.op_col.recno = 6;
//     op4.set_opid(4);

//     input.push_back(op1);
//     input.push_back(op2);
//     input.push_back(op3);
//     input.push_back(op4);

//     // expected output
//     // 4->3->1->2
//     // 4 -> 3
//     int count = 0;
//     __wt_qsort_r(&input[0], input.size(), sizeof(input[0]), __ut_txn_mod_compare, &count);

//     std::cout << std::endl;
//     std::cout << "First element" << input[0].op_identifer << std::endl;
//     std::cout << "Second element" << input[1].op_identifer << std::endl;
//     std::cout << "Third element" << input[2].op_identifer << std::endl;
//     std::cout << "Fourth element" << input[3].op_identifer << std::endl;
//     CHECK(__mod_ops_sorted(input));
// }
}