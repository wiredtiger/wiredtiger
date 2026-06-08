/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

/*
 * __wt_btree_usage_op_sample --
 *     Hot path on every cursor op: a shared 1/1000 gate, and nothing else inlined. Only on the rare
 *     firing do we hand off to the out-of-line worker, which classifies the leaf position, records
 *     the op, and (keyed off op) records key size for inserts and value size for inserts/updates --
 *     so the six btcur call sites are all a single void call. key_size/value_size are ignored for
 *     ops that don't carry them; pass 0.
 */
static WT_INLINE void
__wt_btree_usage_op_sample(
  WT_SESSION_IMPL *session, WT_REF *ref, uint8_t op, uint32_t key_size, uint32_t value_size)
{
    if (WT_UNLIKELY(__wt_random_hotpath_1000(session)))
        __wt_btree_usage_op_fire(session, ref, op, key_size, value_size);
}
