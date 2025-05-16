/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <algorithm>
#include <list>

#include <catch2/catch.hpp>

#include "wiredtiger.h"
#include "wt_internal.h"
#include "../utils.h"
#include "../wrappers/item_wrapper.h"
#include "../wrappers/connection_wrapper.h"

namespace {

bool
has_key(WT_TXN_TYPE type)
{
    switch (type) {
    case (WT_TXN_OP_NONE):
    case (WT_TXN_OP_REF_DELETE):
    case (WT_TXN_OP_TRUNCATE_ROW):
        return (false);
    case (WT_TXN_OP_BASIC_ROW):
    case (WT_TXN_OP_INMEM_ROW):
        return (true);
    }

    return (false);
}

/* Verify the given modifications are sorted. */
static bool WT_CDECL
__mod_ops_sorted(WT_TXN_OP *ops, int op_count, size_t key_size)
{
    WT_TXN_OP *aopt, *bopt;

    for (int i = 0; i < op_count - 1; i++) {
        aopt = &ops[i];
        bopt = &ops[i + 1];

        /* Non key'd operations can separate any modifications with keys. */
        if ((aopt->btree->id == bopt->btree->id) && (!has_key(bopt->type) || !has_key(aopt->type)))
            return (true);

        /* B-tree ids must be in ascending order. */
        if ((aopt->btree->id > bopt->btree->id) && has_key(bopt->type))
            return (false);

        /* Check the key/recno if btree ids are the same. */
        if (aopt->btree->id == bopt->btree->id) {
            if (aopt->btree->type == BTREE_ROW && bopt->btree->type == BTREE_ROW) {
                auto a_key = aopt->u.op_row.key.data;
                auto b_key = bopt->u.op_row.key.data;

                if (strncmp((char *)a_key, (char *)b_key, key_size) > 0)
                    return (false);
            }
        }
    }
    return (true);
}

/* Return a random non-key'd optype. */
WT_TXN_TYPE
rand_non_keyd_type()
{
    WT_TXN_TYPE types[3] = {WT_TXN_OP_NONE, WT_TXN_OP_REF_DELETE, WT_TXN_OP_TRUNCATE_ROW};
    return (types[rand() % 4]);
}

/* Initialize a b-tree with a given type and ID. */
void
init_btree(WT_BTREE *btree, WT_BTREE_TYPE type, uint32_t id)
{
    btree->type = type;
    btree->id = id;
    btree->collator = NULL;
}

/* Initialize a mod operation. */
void
init_op(WT_TXN_OP *op, WT_BTREE *btree, WT_TXN_TYPE type, WT_ITEM *key)
{
    op->btree = btree;
    op->type = type;
    if (op->type == WT_TXN_OP_BASIC_ROW || op->type == WT_TXN_OP_INMEM_ROW) {
        REQUIRE(key != NULL);
        op->u.op_row.key = *key;
    } else
        REQUIRE(!(has_key(op->type)));
}

/* Initialize a row-store key. */
void
init_key(WT_SESSION_IMPL *session, WT_ITEM *key, std::string key_str)
{
    WT_DECL_RET;

    ret = __wt_buf_init(session, key, key_str.size());
    REQUIRE(ret == 0);
    ret = __wt_buf_set(session, key, key_str.c_str(), key_str.size());
    REQUIRE(ret == 0);
}

/* Generate random keys. */
std::string
random_keys(size_t length)
{
    auto randchar = []() -> char {
        const char charset[] = "abcdefghijklmnopqrstuvwxyz";
        const size_t max_index = (sizeof(charset) - 1);
        return charset[rand() % max_index];
    };
    static std::string str(length, 0);
    std::generate_n(str.begin(), length, randchar);
    return str;
}

/* Allocate space for row-store keys. */
void
allocate_key_space(WT_SESSION_IMPL *session, int key_count, WT_ITEM *keys[])
{
    for (int i = 0; i < key_count; i++) {
        WT_DECL_ITEM(key);
        REQUIRE(__wt_scr_alloc(session, 0, &key) == 0);
        keys[i] = key;
    }
}

// Test sorting with row and non-key'd operations.
TEST_CASE("Basic rows and non key'd op", "[mod_compare]")
{
    connection_wrapper conn(DB_HOME);
    WT_SESSION_IMPL *session = conn.create_session();

    WT_BTREE btrees[2];
    WT_TXN_OP ops[4];
    const int key_count = 3, key_size = 2;
    bool ret;
    WT_ITEM *keys[key_count];

    allocate_key_space(session, key_count, keys);

    init_key(session, keys[0], "51");
    init_key(session, keys[1], "40");
    init_key(session, keys[2], "54");

    init_btree(&btrees[0], BTREE_ROW, 1);
    init_btree(&btrees[1], BTREE_ROW, 2);

    // Initialize row ops with different keys.
    for (int i = 0; i < key_count; i++)
        init_op(&ops[i], &btrees[1], WT_TXN_OP_BASIC_ROW, keys[i]);

    init_op(&ops[3], &btrees[0], WT_TXN_OP_NONE, NULL);

    __wt_qsort(&ops, 4, sizeof(WT_TXN_OP), __ut_txn_mod_compare);
    ret = __mod_ops_sorted(ops, 4, key_size);

    // Free the allocated scratch buffers.
    for (int i = 0; i < key_count; i++)
        __wt_scr_free(session, &keys[i]);

    REQUIRE(ret == true);
}

// Test sorting with row, column and operations with no keys.
TEST_CASE("Row, column, and non key'd operations", "[mod_compare]")
{
    connection_wrapper conn(DB_HOME);
    WT_SESSION_IMPL *session = conn.create_session();

    WT_BTREE btrees[2];
    WT_TXN_OP ops[10];
    const int key_count = 6, key_size = 3;
    bool ret;
    WT_ITEM *keys[key_count];

    allocate_key_space(session, key_count, keys);

    for (int i = 0; i < 6; i++)
        init_key(session, keys[i], random_keys(3));

    init_btree(&btrees[0], BTREE_ROW, 1);
    init_btree(&btrees[1], BTREE_ROW, 2);

    // Row operations.
    init_op(&ops[0], &btrees[1], WT_TXN_OP_REF_DELETE, keys[0]);
    init_op(&ops[1], &btrees[1], WT_TXN_OP_BASIC_ROW, keys[1]);
    init_op(&ops[2], &btrees[1], WT_TXN_OP_BASIC_ROW, keys[2]);
    init_op(&ops[3], &btrees[1], WT_TXN_OP_BASIC_ROW, keys[3]);
    init_op(&ops[4], &btrees[1], WT_TXN_OP_BASIC_ROW, keys[4]);
    init_op(&ops[5], &btrees[1], WT_TXN_OP_BASIC_ROW, keys[5]);

    // Non key'd operations.
    init_op(&ops[6], &btrees[0], WT_TXN_OP_TRUNCATE_ROW, NULL);
    init_op(&ops[7], &btrees[1], WT_TXN_OP_REF_DELETE, NULL);

    __wt_qsort(&ops, 8, sizeof(WT_TXN_OP), __ut_txn_mod_compare);
    ret = __mod_ops_sorted(ops, 8, key_size);

    // Free the allocated scratch buffers.
    for (int i = 0; i < key_count; i++)
        __wt_scr_free(session, &keys[i]);

    REQUIRE(ret == true);
}

// Test sorting by b-tree ID. All operations have the same key.
TEST_CASE("B-tree ID sort test", "[mod_compare]")
{
    connection_wrapper conn(DB_HOME);
    WT_SESSION_IMPL *session = conn.create_session();

    WT_BTREE btrees[6];
    WT_TXN_OP ops[6];
    const int key_count = 1, key_size = 1;
    bool ret;
    WT_ITEM *keys[key_count];
    std::string key_str = "1";

    allocate_key_space(session, key_count, keys);

    init_key(session, keys[0], key_str);

    for (int i = 0; i < 6; i++)
        init_btree(&btrees[i], BTREE_ROW, rand() % 400);

    for (int i = 0; i < 6; i++)
        init_op(&ops[i], &btrees[i], WT_TXN_OP_BASIC_ROW, keys[0]);

    __wt_qsort(&ops, 6, sizeof(WT_TXN_OP), __ut_txn_mod_compare);
    ret = __mod_ops_sorted(ops, 6, key_size);

    // Free the allocated scratch buffers first.
    for (int i = 0; i < key_count; i++)
        __wt_scr_free(session, &keys[i]);

    REQUIRE(ret == true);
}

// Test sorting by keyedness, key'd operations all have the same key and recno.
TEST_CASE("Keyedness sort test", "[mod_compare]")
{
    connection_wrapper conn(DB_HOME);
    WT_SESSION_IMPL *session = conn.create_session();

    WT_BTREE btrees[12];
    WT_TXN_OP ops[12];
    const int key_count = 1, key_size = 1;
    bool ret;
    WT_ITEM *keys[key_count];
    std::string key_str = "1";

    allocate_key_space(session, key_count, keys);

    init_key(session, keys[0], key_str);

    for (int i = 0; i < 6; i++)
        init_btree(&btrees[i], BTREE_ROW, i);
    for (int i = 6; i < 12; i++)
        init_btree(&btrees[i], BTREE_ROW, i);

    for (int i = 0; i < 6; i++)
        init_op(&ops[i], &btrees[i], WT_TXN_OP_BASIC_ROW, keys[0]);

    for (int i = 6; i < 12; i++)
        init_op(&ops[i], &btrees[i], rand_non_keyd_type(), NULL);

    __wt_qsort(&ops, 12, sizeof(WT_TXN_OP), __ut_txn_mod_compare);
    ret = __mod_ops_sorted(ops, 12, key_size);

    for (int i = 0; i < key_count; i++)
        __wt_scr_free(session, &keys[i]);

    REQUIRE(ret == true);
}

// Test sorting with randomly generated keys on 2 row-store b-trees.
TEST_CASE("Many different row-store keys", "[mod_compare]")
{
    connection_wrapper conn(DB_HOME);
    WT_SESSION_IMPL *session = conn.create_session();

    WT_BTREE btrees[12];
    WT_TXN_OP ops[12];
    const int key_count = 12, key_size = 3;
    bool ret;
    WT_ITEM *keys[key_count];

    allocate_key_space(session, key_count, keys);

    for (int i = 0; i < 12; i++)
        init_key(session, keys[i], random_keys(3));

    for (int i = 0; i < 6; i++)
        init_btree(&btrees[i], BTREE_ROW, 1);
    for (int i = 6; i < 12; i++)
        init_btree(&btrees[i], BTREE_ROW, 2);

    // Operations will have randomly chosen btrees and randomly generated keys.
    for (int i = 0; i < 12; i++)
        init_op(&ops[i], &btrees[i], WT_TXN_OP_BASIC_ROW, keys[0]);

    __wt_qsort(&ops, 12, sizeof(WT_TXN_OP), __ut_txn_mod_compare);
    ret = __mod_ops_sorted(ops, 12, key_size);

    for (int i = 0; i < key_count; i++)
        __wt_scr_free(session, &keys[i]);

    REQUIRE(ret == true);
}

} // namespace
