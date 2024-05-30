/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

#include "opaque.h"             /* For verification */

/* This file defines __wt_cursor_btree and related definitions. */

struct __wt_cursor_btree {
    WT_CURSOR iface;

    WT_DATA_HANDLE *dhandle; /* Data handle for the btree */

    /*
     * The following fields are set by the search functions as a precursor to page modification: we
     * have a page, a WT_COL/WT_ROW slot on the page, an insert head, insert list and a skiplist
     * stack (the stack of skiplist entries leading to the insert point). The search functions also
     * return the relationship of the search key to the found key.
     */
    WT_REF *ref;   /* Current page */
    uint32_t slot; /* WT_COL/WT_ROW 0-based slot */

    WT_INSERT_HEAD *ins_head; /* Insert chain head */
    WT_INSERT *ins;           /* Current insert node */
    /* Search stack */
    WT_INSERT **ins_stack[WT_SKIP_MAXDEPTH];

    /* Next item(s) found during search */
    WT_INSERT *next_stack[WT_SKIP_MAXDEPTH];

    uint32_t page_deleted_count; /* Deleted items on the page */

    uint64_t recno; /* Record number */

    /*
     * Next-random cursors can optionally be configured to step through a percentage of the total
     * leaf pages to their next value. Note the configured value and the calculated number of leaf
     * pages to skip.
     */
    uint64_t next_random_leaf_skip;
    u_int next_random_sample_size;

    /*
     * The search function sets compare to:
     *	< 1 if the found key is less than the specified key
     *	  0 if the found key matches the specified key
     *	> 1 if the found key is larger than the specified key
     */
    int compare;

    /*
     * A key returned from a binary search or cursor movement on a row-store page; if we find an
     * exact match on a row-store leaf page in a search operation, keep a copy of key we built
     * during the search to avoid doing the additional work of getting the key again for return to
     * the application. Note, this only applies to exact matches when searching disk-image
     * structures, so it's not, for example, a key from an insert list. Additionally, this structure
     * is used to build keys when moving a cursor through a row-store leaf page.
     */
    WT_ITEM *row_key, _row_key;

    /*
     * It's relatively expensive to calculate the last record on a variable- length column-store
     * page because of the repeat values. Calculate it once per page and cache it. This value
     * doesn't include the skiplist of appended entries on the last page.
     */
    uint64_t last_standard_recno;

    /*
     * For row-store pages, we need a single item that tells us the part of the page we're walking
     * (otherwise switching from next to prev and vice-versa is just too complicated), so we map the
     * WT_ROW and WT_INSERT_HEAD insert array slots into a single name space: slot 1 is the
     * "smallest key insert list", slot 2 is WT_ROW[0], slot 3 is WT_INSERT_HEAD[0], and so on. This
     * means WT_INSERT lists are odd-numbered slots, and WT_ROW array slots are even-numbered slots.
     */
    uint32_t row_iteration_slot; /* Row-store iteration slot */

    /*
     * Variable-length column-store values are run-length encoded and may be overflow values. To
     * avoid repeatedly reading overflow values or decompressing encoded values, process it once and
     * store the result in a temporary buffer. The cip_saved field is used to determine if we've
     * switched columns since our last cursor call. Note however that this result caching is not
     * necessarily safe for all RLE cells. The flag WT_CBT_CACHEABLE_RLE_CELL indicates that the
     * value is uniform across the whole cell. If it is not set (e.g. if the cell is not globally
     * visible yet), the cached values should not be used.
     */
    WT_COL *cip_saved; /* Last iteration reference */

    /*
     * We don't instantiate prefix-compressed keys on pages because we don't want to waste memory if
     * only moving a cursor through the page, and it's faster to build keys while moving through the
     * page than to roll-forward from a previously instantiated key (we don't instantiate all of the
     * keys, just the ones at binary search points). We can't use the application's WT_CURSOR key
     * field as a copy of the last-returned key because it may have been altered by the API layer,
     * for example, dump cursors. Instead we store the last-returned key in a temporary buffer. The
     * rip_saved field is used to determine if the key in the temporary buffer has the prefix needed
     * for building the current key.
     */
    WT_ROW *rip_saved; /* Last-returned key reference */

    /*
     * A temporary buffer, used in a few different ways:
     *
     * 1) For caching RLE values for column-store files (if RLE is non-zero, then we don't unpack
     * the value every time we move to the next cursor position, we re-use the unpacked value we
     * stored here the first time we hit the value).
     *
     * 2) For building on-page keys when searching row-store files.
     *
     * 3) For tracking random return values to avoid repetition.
     */
    WT_ITEM *tmp, _tmp;

    /*
     * The update structure allocated by the row- and column-store modify functions, used to avoid a
     * data copy in the WT_CURSOR.update call.
     */
    WT_UPDATE_VALUE *modify_update, _modify_update;

    /* An intermediate structure to hold the update value to be assigned to the cursor buffer. */
    WT_UPDATE_VALUE *upd_value, _upd_value;

    /*
     * Bits used by checkpoint cursor: a private transaction, used to provide the proper read
     * snapshot; a reference to the corresponding history store checkpoint, which keeps it from
     * disappearing under us if it's unnamed and also tracks its identity for use in history store
     * accesses; a write generation, used to override the tree's base write generation in the
     * unpacking cleanup code; and a checkpoint ID, which is available to applications through an
     * undocumented interface to allow them to open cursors on multiple files and check if they got
     * the same checkpoint in all of them.
     */
    WT_TXN *checkpoint_txn;
    WT_DATA_HANDLE *checkpoint_hs_dhandle;
    uint64_t checkpoint_write_gen;
    uint64_t checkpoint_id;

    /*
     * Fixed-length column-store items are a single byte, and it's simpler and cheaper to allocate
     * the space for it now than keep checking to see if we need to grow the buffer.
     */
    uint8_t v; /* Fixed-length return value */

    uint8_t append_tree; /* Cursor appended to the tree */

    /*
     * We have to restart cursor next/prev after a prepare conflict. Keep the state of the cursor
     * separately so we can restart at exactly the right point.
     */
    enum { WT_CBT_RETRY_NOTSET = 0, WT_CBT_RETRY_INSERT, WT_CBT_RETRY_PAGE } iter_retry;

    /*
     * The random number state is used for random cursor operations. The random number can be seeded
     * by the user or is randomly set based on the time and thread ID.
     */
    wt_shared WT_RAND_STATE rnd; /* Random number generation state */

#ifdef HAVE_DIAGNOSTIC
    /* Check that cursor next/prev never returns keys out-of-order. */
    WT_ITEM *lastkey, _lastkey;
    uint64_t lastrecno;

    /* Record where the last key is when we see it to help debugging out of order issues. */
    WT_REF *lastref;    /* The page where the last key is */
    uint32_t lastslot;  /* WT_COL/WT_ROW 0-based slot */
    WT_INSERT *lastins; /* The last insert list */
#endif

/* AUTOMATIC FLAG VALUE GENERATION START 0 */
#define WT_CBT_ACTIVE 0x001u             /* Active in the tree */
#define WT_CBT_CACHEABLE_RLE_CELL 0x002u /* Col-store: value in RLE cell valid for its keys */
#define WT_CBT_ITERATE_APPEND 0x004u     /* Col-store: iterating append list */
#define WT_CBT_ITERATE_NEXT 0x008u       /* Next iteration configuration */
#define WT_CBT_ITERATE_PREV 0x010u       /* Prev iteration configuration */
#define WT_CBT_ITERATE_RETRY_NEXT 0x020u /* Prepare conflict by next. */
#define WT_CBT_ITERATE_RETRY_PREV 0x040u /* Prepare conflict by prev. */
#define WT_CBT_READ_ONCE 0x080u          /* Page in with WT_READ_WONT_NEED */
#define WT_CBT_SEARCH_SMALLEST 0x100u    /* Row-store: small-key insert list */
#define WT_CBT_VAR_ONPAGE_MATCH 0x200u   /* Var-store: on-page recno match */
    /* AUTOMATIC FLAG VALUE GENERATION STOP 32 */

#define WT_CBT_POSITION_MASK /* Flags associated with position */                      \
    (WT_CBT_ITERATE_APPEND | WT_CBT_ITERATE_NEXT | WT_CBT_ITERATE_PREV |               \
      WT_CBT_ITERATE_RETRY_NEXT | WT_CBT_ITERATE_RETRY_PREV | WT_CBT_SEARCH_SMALLEST | \
      WT_CBT_VAR_ONPAGE_MATCH)

    uint32_t flags;
};

static_assert(sizeof(struct __wt_cursor_btree) == sizeof(WT_CURSOR_BTREE_OPAQUE));

/* Get the WT_BTREE from any WT_CURSOR/WT_CURSOR_BTREE. */
#ifdef INLINE_FUNCTIONS_INSTEAD_OF_MACROS
/*
 * __wt_curbt2bt --
 *     Safely return the WT_BTREE pointed to by the cursor_btree's dhandle.
 */
static WT_INLINE WT_BTREE *
__wt_curbt2bt(WT_CURSOR_BTREE *cursor_btree)
{
    WT_DATA_HANDLE *dhandle;

    dhandle = cursor_btree->dhandle;

    return (dhandle == NULL ? NULL : (WT_BTREE *)(dhandle->handle));
}

#define CUR2BT(c) __wt_curbt2bt((WT_CURSOR_BTREE *)(c))
#else
#define CUR2BT(c)                                \
    (((WT_CURSOR_BTREE *)(c))->dhandle == NULL ? \
        NULL :                                   \
        (WT_BTREE *)((WT_CURSOR_BTREE *)(c))->dhandle->handle)
#endif /* INLINE_FUNCTIONS_INSTEAD_OF_MACROS */

/*
 * A positioned cursor must have a page, this is a requirement of the cursor logic within the
 * wiredtiger API. As such if the page on the cursor is not null we can safely assume that the
 * cursor is positioned.
 *
 * This is primarily used by cursor bound checking logic.
 */
#define WT_CURSOR_IS_POSITIONED(cbt) (cbt->ref != NULL && cbt->ref->page != NULL)
