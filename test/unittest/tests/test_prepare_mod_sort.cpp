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

#include "utils.h"
#include "wt_internal.h"
#include "wrappers/item_wrapper.h"
#include "wrappers/connection_wrapper.h"
#include "wiredtiger.h"

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

    std::cout << "Started comparing" << std::endl;

    for(i=0; i < op_count - 1; i++){
        aopt = &ops[i];
        bopt = &ops[i+1];

        // /* A non-key'd operation cannot come before a key'd operation. */
        // if(has_key(bopt->type) && !has_key(aopt->type))
        //     return(false);

        // Non key'd operations can separate any modifications with keys.
        if((aopt->btree->id == bopt->btree->id) && (!has_key(bopt->type) || !has_key(aopt->type)))
            return (true);

        /* B-tree ids must be in ascending order.*/
        if((aopt->btree->id > bopt->btree->id) && has_key(bopt->type))
            return (false);
    
        /* Check the key/recno if btree ids are the same. */
        if(aopt->btree->id == bopt->btree->id){
            if(aopt->btree->type == BTREE_ROW && bopt->btree->type == BTREE_ROW) {
                auto a_key = aopt->u.op_row.key.data;
                auto b_key = bopt->u.op_row.key.data;
                if ( *((char*)a_key) > *((char*)b_key))
                    return (false);           
            }

            if(aopt->btree->type == BTREE_COL_VAR && bopt->btree->type == BTREE_COL_VAR) {
                if ( aopt->u.op_col.recno > bopt->u.op_col.recno)
                    return (false);
            }
        }
    }
    return (true);
}

// Randomly return a non-key'd optype.
WT_TXN_TYPE
rand_non_keyd_type() {
    WT_TXN_TYPE types[4] = { WT_TXN_OP_NONE, WT_TXN_OP_REF_DELETE, WT_TXN_OP_TRUNCATE_COL, WT_TXN_OP_TRUNCATE_ROW };
    return (types[rand()%4]);
}
void
init_btree(WT_BTREE *btree, WT_BTREE_TYPE type, uint32_t id) {
    btree->type = type;
    btree->id = id;
    btree->collator = NULL;
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
        REQUIRE(!(has_key(op->type)));
    }
}

void
init_key(WT_ITEM *key, const char* key_data) {
    key->data = key_data;
    key->size = sizeof(key_data);
}

// Randomly generate alphanumeric keys
const char* random_string( size_t length )
{
    auto randchar = []() -> char
    {
        const char charset[] =
        "0123456789"
        "abcdefghijklmnopqrstuvwxyz";
        const size_t max_index = (sizeof(charset) - 1);
        return charset[ rand() % max_index ];
    };
    std::string str(length,0);
    std::generate_n( str.begin(), length, randchar );
    return str.c_str();
}

TEST_CASE("Basic cols and op_none", "[mod_compare]")
{
    WT_BTREE btrees[2];
    WT_TXN_OP ops[2];

    init_btree(&btrees[0], BTREE_ROW, 1);
    init_btree(&btrees[1], BTREE_COL_VAR, 2);
    init_op(&ops[1], &btrees[0], WT_TXN_OP_NONE, WT_RECNO_OOB, NULL);
    init_op(&ops[0], &btrees[1], WT_TXN_OP_BASIC_COL, 54, NULL);

    __wt_qsort_r(&ops, 2, sizeof(WT_TXN_OP), __ut_txn_mod_compare, NULL);
    REQUIRE(__mod_ops_sorted(ops, 2) == true);
}

TEST_CASE("Basic rows and op_nones", "[mod_compare]")
{
    ConnectionWrapper conn(DB_HOME);
    WT_SESSION_IMPL *session = conn.createSession();

    WT_BTREE btrees[2];
    WT_TXN_OP ops[4];
    WT_ITEM *keys[3];
    int ret;
    // WT_DECL_ITEM(key);

    for (int i=0; i <= 2; i++){
        WT_DECL_ITEM(key);
        REQUIRE(__wt_scr_alloc(session, 0, &key) == 0);
        keys[i] = key;
    }

    init_key(keys[0], "51");
    init_key(keys[1], "4");
    init_key(keys[2], "54");

    init_btree(&btrees[0], BTREE_COL_VAR, 1);
    init_btree(&btrees[1], BTREE_ROW, 2);

    init_op(&ops[0], &btrees[0], WT_TXN_OP_NONE, WT_RECNO_OOB, NULL);
    init_op(&ops[1], &btrees[1], WT_TXN_OP_BASIC_ROW, WT_RECNO_OOB, keys[0]);
    init_op(&ops[2], &btrees[1], WT_TXN_OP_BASIC_ROW, WT_RECNO_OOB, keys[1]);
    init_op(&ops[3], &btrees[1], WT_TXN_OP_BASIC_ROW, WT_RECNO_OOB, keys[2]);

    __wt_qsort_r(&ops, 4, sizeof(WT_TXN_OP), __ut_txn_mod_compare, NULL);
    ret = __mod_ops_sorted(ops, 4);

    // Free the allocated scratch buffers first.
    for (int i=0; i <= 2; i++)
        __wt_scr_free(session, &keys[i]);

    // If not sorted correctly, barf.
    REQUIRE(ret == true);
    
    std::cout << "First element" << ops[0].type << std::endl;
    std::cout << "Second element" << (char*) ops[1].u.op_row.key.data << std::endl;
    std::cout << "Third element" <<  (char*) ops[2].u.op_row.key.data << std::endl;
    std::cout << "Fourth element" << (char*) ops[3].u.op_row.key.data << std::endl;

}

TEST_CASE("Rows, cols, no ops", "[mod_compare]")
{
    ConnectionWrapper conn(DB_HOME);
    WT_SESSION_IMPL *session = conn.createSession();

    WT_BTREE btrees[2];
    WT_TXN_OP ops[4];
    int ret = 0;
    int key_sz = 1;
    WT_ITEM *keys[key_sz];

    // WT_DECL_ITEM(key);

    for (int i=0; i <= key_sz; i++){
        WT_DECL_ITEM(key);
        REQUIRE(__wt_scr_alloc(session, 0, &key) == 0);
        keys[i] = key;
    }

    init_key(keys[0], "51");

    init_btree(&btrees[0], BTREE_COL_VAR, 1);
    init_btree(&btrees[1], BTREE_ROW, 2);

    init_op(&ops[0], &btrees[0], WT_TXN_OP_BASIC_COL, 12, NULL); 
    init_op(&ops[1], &btrees[1], WT_TXN_OP_REF_DELETE, WT_RECNO_OOB, NULL);
    init_op(&ops[2], &btrees[0], WT_TXN_OP_BASIC_COL, 45, NULL); 
    init_op(&ops[3], &btrees[1], WT_TXN_OP_BASIC_ROW, WT_RECNO_OOB, keys[0]);

    __wt_qsort_r(&ops, 4, sizeof(WT_TXN_OP), __ut_txn_mod_compare, NULL);
    ret = __mod_ops_sorted(ops, 4);

    // Free the allocated scratch buffers first.
    for (int i=0; i <= key_sz; i++)
        __wt_scr_free(session, &keys[i]);

    // If not sorted correctly, barf.
    REQUIRE(ret == true);

}

TEST_CASE("Rows, cols, more no ops", "[mod_compare]")
{
    ConnectionWrapper conn(DB_HOME);
    WT_SESSION_IMPL *session = conn.createSession();

    WT_BTREE btrees[2];
    WT_TXN_OP ops[10];
    int ret = 0;
    int key_sz = 6;
    WT_ITEM *keys[key_sz];

    // WT_DECL_ITEM(key);

    for (int i=0; i <= key_sz; i++){
        WT_DECL_ITEM(key);
        REQUIRE(__wt_scr_alloc(session, 0, &key) == 0);
        keys[i] = key;
    }

    init_key(keys[0], "1");
    init_key(keys[1], "11");
    init_key(keys[2], "511");
    init_key(keys[3], "994");
    init_key(keys[4], "78");
    init_key(keys[5], "9");

    init_btree(&btrees[0], BTREE_COL_VAR, 1);
    init_btree(&btrees[1], BTREE_ROW, 2);

    // 2cols
    init_op(&ops[0], &btrees[0], WT_TXN_OP_BASIC_COL, 12, NULL); 
    init_op(&ops[1], &btrees[0], WT_TXN_OP_BASIC_COL, 45, NULL); 

    // 6 rows
    init_op(&ops[2], &btrees[1], WT_TXN_OP_REF_DELETE, WT_RECNO_OOB, keys[0]);
    init_op(&ops[3], &btrees[1], WT_TXN_OP_BASIC_ROW, WT_RECNO_OOB, keys[1]);
    init_op(&ops[4], &btrees[1], WT_TXN_OP_BASIC_ROW, WT_RECNO_OOB, keys[2]);
    init_op(&ops[5], &btrees[1], WT_TXN_OP_BASIC_ROW, WT_RECNO_OOB, keys[3]);
    init_op(&ops[6], &btrees[1], WT_TXN_OP_BASIC_ROW, WT_RECNO_OOB, keys[4]);
    init_op(&ops[7], &btrees[1], WT_TXN_OP_BASIC_ROW, WT_RECNO_OOB, keys[5]);

    // 2 non keyd ops
    init_op(&ops[8], &btrees[0], WT_TXN_OP_TRUNCATE_COL, WT_RECNO_OOB, NULL);
    init_op(&ops[9], &btrees[1], WT_TXN_OP_REF_DELETE, WT_RECNO_OOB, NULL);

    __wt_qsort_r(&ops, 10, sizeof(WT_TXN_OP), __ut_txn_mod_compare, NULL);
    ret = __mod_ops_sorted(ops, 10);

    // Free the allocated scratch buffers first.
    for (int i=0; i <= key_sz; i++)
        __wt_scr_free(session, &keys[i]);

    // If not sorted correctly, barf.
    REQUIRE(ret == true);
}


// Btree ID sort test, give it six randomly sorted mods with each different btree ids and everything else the same
// Column store key sort test

TEST_CASE("Btree ID", "[mod_compare]")
{
    ConnectionWrapper conn(DB_HOME);
    WT_SESSION_IMPL *session = conn.createSession();

    WT_BTREE btrees[6];
    WT_TXN_OP ops[6];
    int ret = 0;
    int key_sz = 1;
    WT_ITEM *keys[key_sz];

    for (int i=0; i <= key_sz; i++){
        WT_DECL_ITEM(key);
        REQUIRE(__wt_scr_alloc(session, 0, &key) == 0);
        keys[i] = key;
    }

    init_key(keys[0], "1");

    for (int i=0; i<6; i++)
        init_btree(&btrees[i], BTREE_ROW, rand() % 400);

    for (int i=0; i < 6; i++)
        init_op(&ops[i], &btrees[i], WT_TXN_OP_BASIC_ROW, WT_RECNO_OOB, keys[0]);

    __wt_qsort_r(&ops, 6, sizeof(WT_TXN_OP), __ut_txn_mod_compare, NULL);
    ret = __mod_ops_sorted(ops, 6);

    // Free the allocated scratch buffers first.
    for (int i=0; i <= key_sz; i++)
        __wt_scr_free(session, &keys[i]);

    // If not sorted correctly, barf.
    REQUIRE(ret == true);

    std::cout << "First element" << ops[1].btree->id  << std::endl;
    std::cout << "Second element" <<  ops[1].btree->id << std::endl;
    std::cout << "Third element" <<  ops[2].btree->id << std::endl;
    std::cout << "Fourth element" << ops[3].btree->id << std::endl;
    std::cout << "Fifth element" <<  ops[4].btree->id << std::endl;
    std::cout << "Sixth element" << ops[5].btree->id << std::endl;
    // std::cout << "Fourth element" << (char*) ops[3].btree->id << std::endl;
}

// Keyedness sort test, give it a bunch of mods that have the same keys and mods that don't have keys. Randomly distributed like with #1
TEST_CASE("Keyedness sort test", "[mod_compare]")
{
    ConnectionWrapper conn(DB_HOME);
    WT_SESSION_IMPL *session = conn.createSession();

    WT_BTREE btrees[12];
    WT_TXN_OP ops[12];
    int ret = 0;
    int key_sz = 1;
    WT_ITEM *keys[key_sz];

    for (int i=0; i <= key_sz; i++){
        WT_DECL_ITEM(key);
        REQUIRE(__wt_scr_alloc(session, 0, &key) == 0);
        keys[i] = key;
    }

    init_key(keys[0], "1");

    // randomly generated mods 
    for (int i=0; i<6; i++)
        init_btree(&btrees[i], BTREE_ROW, rand() % 100);
    for (int i=6; i<12; i++)
        init_btree(&btrees[i], BTREE_COL_VAR, rand() % 100);

    for (int i=6; i < 12; i++)
        init_op(&ops[i], &btrees[rand()%6], rand_non_keyd_type(), WT_RECNO_OOB, NULL);

    init_op(&ops[0], &btrees[rand()%6], WT_TXN_OP_BASIC_ROW, WT_RECNO_OOB, keys[0]);
    init_op(&ops[1], &btrees[rand()%6], WT_TXN_OP_BASIC_ROW, WT_RECNO_OOB, keys[0]);
    init_op(&ops[2], &btrees[rand()%6], WT_TXN_OP_BASIC_ROW, WT_RECNO_OOB, keys[0]);
    init_op(&ops[3], &btrees[rand()%6 + 6], WT_TXN_OP_BASIC_COL, 54, NULL);
    init_op(&ops[4], &btrees[rand()%6 + 6], WT_TXN_OP_BASIC_COL, 54, NULL);
    init_op(&ops[5], &btrees[rand()%6 + 6], WT_TXN_OP_BASIC_COL, 54, NULL);

    __wt_qsort_r(&ops, 12, sizeof(WT_TXN_OP), __ut_txn_mod_compare, NULL);
    ret = __mod_ops_sorted(ops, 12);

    // Free the allocated scratch buffers first.
    for (int i=0; i <= key_sz; i++)
        __wt_scr_free(session, &keys[i]);

    // If not sorted correctly, barf.
    REQUIRE(ret == true);

}

// Row store key sort test, all same mods but different row store keys
TEST_CASE("Different row store keys test", "[mod_compare]")
{
    ConnectionWrapper conn(DB_HOME);
    WT_SESSION_IMPL *session = conn.createSession();

    WT_BTREE btrees[12];
    WT_TXN_OP ops[12];
    int ret = 0;
    int key_sz = 12;
    WT_ITEM *keys[key_sz];

    for (int i=0; i <= key_sz; i++){
        WT_DECL_ITEM(key);
        REQUIRE(__wt_scr_alloc(session, 0, &key) == 0);
        keys[i] = key;
    }

    //randomly generated alphanumeric keys
    for (int i=0; i<12; i++)
        init_key(keys[i], random_string(5));

    // randomly generated mods 
    for (int i=0; i<6; i++)
        init_btree(&btrees[i], BTREE_ROW, 1);
    for (int i=6; i<12; i++)
        init_btree(&btrees[i], BTREE_ROW, 2);

    for (int i=0; i<12; i++)
        init_op(&ops[i], &btrees[rand()%1 + 1], WT_TXN_OP_BASIC_ROW, WT_RECNO_OOB, keys[rand()%12]);

    __wt_qsort_r(&ops, 12, sizeof(WT_TXN_OP), __ut_txn_mod_compare, NULL);
    ret = __mod_ops_sorted(ops, 12);

    // Free the allocated scratch buffers first.
    for (int i=0; i <= key_sz; i++)
        __wt_scr_free(session, &keys[i]);

    // If not sorted correctly, barf.
    REQUIRE(ret == true);

}
}