/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

/*
 * WT_BTREE_DIFF_TYPE --
 *     How a key differs between two checkpoints of a btree.
 */
typedef enum {
    WT_BTREE_DIFF_ADDED,   /* Only in the new checkpoint */
    WT_BTREE_DIFF_DELETED, /* Only in the old checkpoint */
    WT_BTREE_DIFF_MODIFIED /* In both, with different values */
} WT_BTREE_DIFF_TYPE;

/*
 * WT_BTREE_DIFF_ENTRY --
 *     A difference returned by a diff. Like a cursor's key and value, the items reference pinned
 *     pages or buffers owned by the diff, and are valid until the next call on the diff.
 */
struct __wt_btree_diff_entry {
    WT_BTREE_DIFF_TYPE type;
    const WT_ITEM *key;
    const WT_ITEM *old_value; /* NULL if added */
    const WT_ITEM *new_value; /* NULL if deleted */
};

/*
 * WT_BTREE_DIFF_FRAME --
 *     An internal page on the path from the root to a diff side's position.
 */
struct __wt_btree_diff_frame {
    WT_REF *ref;   /* Page, pinned by a hazard pointer unless it is the root */
    WT_ITEM key;   /* Page's key in its parent, no data if leftmost */
    uint32_t slot; /* Current child */
};

/*
 * WT_BTREE_DIFF_SIDE --
 *     One checkpoint's position in a diff. Between steps a side is at a leaf entry (the leaf is
 *     set), at a subtree it has not read (the subtree is set), or at its end (neither is set).
 */
struct __wt_btree_diff_side {
    WT_DATA_HANDLE *dhandle;

    WT_BTREE_DIFF_FRAME *path; /* The root is the first frame */
    size_t path_allocated;
    uint32_t depth;

    WT_REF *subtree;           /* Next unread subtree, internal or leaf page */
    WT_ITEM subtree_key;       /* Key of the next subtree */
    WT_ADDR_COPY subtree_addr; /* Address of the next subtree */

    WT_REF *leaf; /* Current leaf, pinned by a hazard pointer */
    uint32_t leaf_slot;
    WT_ITEM key;   /* Current leaf entry */
    WT_ITEM value; /* Current leaf entry */

    bool returned; /* The current entry was returned, move past it on the next step */
};

/*
 * WT_BTREE_DIFF --
 *     A walk over the differences between two checkpoints of the same btree, in key order.
 */
struct __wt_btree_diff {
    WT_SESSION_IMPL *session;
    WT_COLLATOR *collator;

    WT_ITEM lower_bound_buf, upper_bound_buf;
    const WT_ITEM *lower_bound; /* Inclusive, NULL if unbounded */
    const WT_ITEM *upper_bound; /* Exclusive, NULL if unbounded */

    WT_BTREE_DIFF_SIDE old_side, new_side;

    uint64_t pages_read;      /* Pages the walk descended into */
    uint64_t identical_skips; /* Pairs of identical subtrees skipped */

    bool own_dhandles; /* Release the data handles on close */
};
