/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

static int __err_cell_corrupt(int, WT_VERIFY_INFO *);
static int __err_cell_corrupt_or_eof(int, WT_VERIFY_INFO *);
static int __err_cell_type(uint8_t, WT_VERIFY_INFO *);
static int __verify_dsk_chunk(WT_VERIFY_INFO *);
static int __verify_dsk_memsize(WT_CELL *, WT_VERIFY_INFO *);
static int __verify_dsk_row_int(WT_VERIFY_INFO *vi);
static int __verify_dsk_row_leaf(WT_VERIFY_INFO *);

#define WT_ERR_VRFY(session, f, ...)                                       \
    do {                                                                   \
        if (!(F_ISSET(session, WT_SESSION_QUIET_CORRUPT_FILE))) {          \
            __wt_errx(session, __VA_ARGS__);                               \
            /* Easy way to set a breakpoint when tracking corruption */    \
            WT_IGNORE_RET(__wt_session_breakpoint((WT_SESSION *)session)); \
            if (!FLD_ISSET(f, WT_VRFY_DISK_CONTINUE_ON_FAILURE))           \
                WT_ASSERT(session, ret == 0);                              \
        }                                                                  \
        goto err;                                                          \
    } while (0)

#define WT_RET_VRFY_RETVAL(session, ret, ...)                              \
    do {                                                                   \
        if (!(F_ISSET(session, WT_SESSION_QUIET_CORRUPT_FILE))) {          \
            if ((ret) == 0)                                                \
                __wt_errx(session, __VA_ARGS__);                           \
            else                                                           \
                __wt_err(session, ret, __VA_ARGS__);                       \
            /* Easy way to set a breakpoint when tracking corruption */    \
            WT_IGNORE_RET(__wt_session_breakpoint((WT_SESSION *)session)); \
        }                                                                  \
        return ((ret) == 0 ? WT_ERROR : ret);                              \
    } while (0)

#define WT_RET_VRFY(session, ...) WT_RET_VRFY_RETVAL(session, 0, __VA_ARGS__)

/*
 * WT_CELL_FOREACH_VRFY --
 *	Iterate through each cell on a page. Verify-specific version of the
 * WT_CELL_FOREACH macro, created because the loop can't simply unpack cells,
 * verify has to do additional work to ensure that unpack is safe.
 */
#define WT_CELL_FOREACH_VRFY(session, dsk, cell, unpack, i)                                 \
    for ((cell) = WT_PAGE_HEADER_BYTE(S2BT(session), dsk), (i) = (dsk)->u.entries; (i) > 0; \
         (cell) = (WT_CELL *)((uint8_t *)(cell) + (unpack)->__len), --(i))

/*
 * __wt_verify_dsk_image --
 *     Verify a single block as read from disk.
 */
int
__wt_verify_dsk_image(WT_SESSION_IMPL *session, const char *tag, const WT_PAGE_HEADER *dsk,
  size_t size, WT_ADDR *addr, uint32_t verify_flags)
{
    uint8_t flags;
    const uint8_t *end, *p;

    /* Initialize the verify information. */
    WT_VERIFY_INFO vi;
    vi.session = session;
    vi.tag = tag;
    vi.dsk = dsk;
    vi.page_addr = addr;
    vi.page_size = size;
    vi.cell_num = 0;
    vi.flags = verify_flags;

    /* Check the page type. */
    switch (dsk->type) {
    case WT_PAGE_BLOCK_MANAGER:
    case WT_PAGE_OVFL:
    case WT_PAGE_ROW_INT:
    case WT_PAGE_ROW_LEAF:
        break;
    case WT_PAGE_INVALID:
    default:
        WT_RET_VRFY(session, "page at %s has an invalid type of %" PRIu32, tag, dsk->type);
    }

    /* Check the page record number. */
    switch (dsk->type) {
    case WT_PAGE_BLOCK_MANAGER:
    case WT_PAGE_OVFL:
    case WT_PAGE_ROW_INT:
    case WT_PAGE_ROW_LEAF:
        WT_RET_VRFY(session,
          "%s page at %s has a record number, which is illegal for this page type",
          __wt_page_type_string(dsk->type), tag);
    }

    /* Check the page flags. */
    flags = dsk->flags;
    if (LF_ISSET(WT_PAGE_COMPRESSED))
        LF_CLR(WT_PAGE_COMPRESSED);
    if (dsk->type == WT_PAGE_ROW_LEAF) {
        if (LF_ISSET(WT_PAGE_EMPTY_V_ALL) && LF_ISSET(WT_PAGE_EMPTY_V_NONE))
            WT_RET_VRFY(
              session, "page at %s has invalid flags combination: 0x%" PRIx8, tag, dsk->flags);
        if (LF_ISSET(WT_PAGE_EMPTY_V_ALL))
            LF_CLR(WT_PAGE_EMPTY_V_ALL);
        if (LF_ISSET(WT_PAGE_EMPTY_V_NONE))
            LF_CLR(WT_PAGE_EMPTY_V_NONE);
    }
    if (LF_ISSET(WT_PAGE_ENCRYPTED))
        LF_CLR(WT_PAGE_ENCRYPTED);
    if (LF_ISSET(WT_PAGE_UNUSED))
        LF_CLR(WT_PAGE_UNUSED);
    if (LF_ISSET(WT_PAGE_FT_UPDATE))
        LF_CLR(WT_PAGE_FT_UPDATE);
    if (flags != 0)
        WT_RET_VRFY(session, "page at %s has invalid flags set: 0x%" PRIx8, tag, flags);

    /* Check the unused byte. */
    if (dsk->unused != 0)
        WT_RET_VRFY(session, "page at %s has non-zero unused page header bytes", tag);

    /* Check the page version. */
    switch (dsk->version) {
    case WT_PAGE_VERSION_ORIG:
    case WT_PAGE_VERSION_TS:
        break;
    default:
        WT_RET_VRFY(session, "page at %s has an invalid version of %" PRIu8, tag, dsk->version);
    }

    /*
     * Any bytes after the data chunk should be nul bytes; ignore if the size is 0, that allows easy
     * checking of disk images where we don't have the size.
     */
    if (size != 0) {
        p = (uint8_t *)dsk + dsk->mem_size;
        end = (uint8_t *)dsk + size;
        for (; p < end; ++p)
            if (*p != '\0')
                WT_RET_VRFY(session, "%s page at %s has non-zero trailing bytes",
                  __wt_page_type_string(dsk->type), tag);
    }

    /* Check for empty pages, then verify the items on the page. */
    switch (dsk->type) {
    case WT_PAGE_ROW_INT:
    case WT_PAGE_ROW_LEAF:
        if (!FLD_ISSET(vi.flags, WT_VRFY_DISK_EMPTY_PAGE_OK) && dsk->u.entries == 0)
            WT_RET_VRFY(
              session, "%s page at %s has no entries", __wt_page_type_string(dsk->type), tag);
        break;
    case WT_PAGE_BLOCK_MANAGER:
    case WT_PAGE_OVFL:
        if (dsk->u.datalen == 0)
            WT_RET_VRFY(
              session, "%s page at %s has no data", __wt_page_type_string(dsk->type), tag);
        break;
    }
    switch (dsk->type) {
    case WT_PAGE_ROW_INT:
        return (__verify_dsk_row_int(&vi));
    case WT_PAGE_ROW_LEAF:
        return (__verify_dsk_row_leaf(&vi));
    case WT_PAGE_BLOCK_MANAGER:
    case WT_PAGE_OVFL:
        return (__verify_dsk_chunk(&vi));
    default:
        return (__wt_illegal_value(session, dsk->type));
    }
    /* NOTREACHED */
}

/*
 * __wt_verify_dsk --
 *     Verify a single Btree page as read from disk.
 */
int
__wt_verify_dsk(WT_SESSION_IMPL *session, const char *tag, WT_ITEM *buf)
{
    return (__wt_verify_dsk_image(
      session, tag, buf->data, buf->size, NULL, WT_VRFY_DISK_CONTINUE_ON_FAILURE));
}

/*
 * __verify_dsk_addr_validity --
 *     Verify an address cell's validity window.
 */
static int
__verify_dsk_addr_validity(WT_CELL_UNPACK_ADDR *unpack, WT_VERIFY_INFO *vi)
{
    WT_ADDR *addr;
    WT_DECL_RET;

    addr = vi->page_addr;

    if ((ret = __wt_time_aggregate_validate(vi->session, &unpack->ta,
           addr != NULL ? &addr->ta : NULL, F_ISSET(vi->session, WT_SESSION_QUIET_CORRUPT_FILE))) ==
      0)
        return (0);

    WT_RET_VRFY_RETVAL(vi->session, ret,
      "cell %" PRIu32 " on page at %s failed timestamp validation", vi->cell_num - 1, vi->tag);
}

/*
 * __verify_dsk_value_validity --
 *     Verify a value cell's validity window.
 */
static int
__verify_dsk_value_validity(WT_CELL_UNPACK_KV *unpack, WT_VERIFY_INFO *vi)
{
    WT_ADDR *addr;
    WT_DECL_RET;

    addr = vi->page_addr;

    if ((ret = __wt_time_value_validate(vi->session, &unpack->tw, addr != NULL ? &addr->ta : NULL,
           F_ISSET(vi->session, WT_SESSION_QUIET_CORRUPT_FILE))) == 0)
        return (0);

    WT_RET_VRFY_RETVAL(vi->session, ret,
      "cell %" PRIu32 " on page at %s failed timestamp validation", vi->cell_num - 1, vi->tag);
}

/*
 * __verify_dsk_addr_page_del --
 *     Verify a deleted-page address cell's page delete information.
 */
static int
__verify_dsk_addr_page_del(WT_SESSION_IMPL *session, WT_CELL_UNPACK_ADDR *unpack, uint32_t cell_num,
  WT_ADDR *addr, const char *tag)
{
    WT_DECL_RET;
    WT_TIME_AGGREGATE ta_with_delete;
    char time_string[WT_TIME_STRING_SIZE];

    /* The durable timestamp in the page_delete info should not be before its commit timestamp. */
    if (unpack->page_del.durable_timestamp < unpack->page_del.timestamp)
        WT_RET_VRFY(session,
          "fast-delete cell %" PRIu32 " on page at %s has durable timestamp %" PRIu64
          " before its commit timestamp %" PRIu64,
          cell_num - 1, tag, unpack->page_del.durable_timestamp, unpack->page_del.timestamp);

    /*
     * The timestamps in the page_delete information are a global stop time for the entire page.
     * This is not reflected in the aggregate, but is supposed to be reflected in the parent's
     * aggregate. First check that the aggregate is consistent with being deleted at the given time.
     */
    if (unpack->ta.newest_stop_durable_ts > unpack->page_del.durable_timestamp)
        WT_RET_VRFY(session,
          "fast-delete cell %" PRIu32
          " on page at %s has invalid newest durable stop time; should be <= %" PRIu64
          "; time aggregate %s",
          cell_num - 1, tag, unpack->page_del.durable_timestamp,
          __wt_time_aggregate_to_string(&unpack->ta, time_string));
    if (unpack->ta.newest_txn > unpack->page_del.txnid)
        WT_RET_VRFY(session,
          "fast-delete cell %" PRIu32
          " on page at %s has invalid newest transaction; should be <= %" PRIu64
          "; time aggregate %s",
          cell_num - 1, tag, unpack->page_del.txnid,
          __wt_time_aggregate_to_string(&unpack->ta, time_string));
    if (unpack->ta.newest_stop_ts != WT_TS_MAX &&
      unpack->ta.newest_stop_ts > unpack->page_del.timestamp)
        WT_RET_VRFY(session,
          "fast-delete cell %" PRIu32
          " on page at %s has invalid newest stop time; should be <= %" PRIu64
          "; time aggregate %s",
          cell_num - 1, tag, unpack->page_del.timestamp,
          __wt_time_aggregate_to_string(&unpack->ta, time_string));
    if (unpack->ta.newest_stop_txn != WT_TXN_MAX &&
      unpack->ta.newest_stop_txn > unpack->page_del.txnid)
        WT_RET_VRFY(session,
          "fast-delete cell %" PRIu32
          " on page at %s has invalid newest stop transaction; should be <= %" PRIu64
          "; time aggregate %s",
          cell_num - 1, tag, unpack->page_del.txnid,
          __wt_time_aggregate_to_string(&unpack->ta, time_string));

    /*
     * Merge this information into the aggregate and verify the results, against the parent if
     * possible.
     */
    WT_TIME_AGGREGATE_COPY(&ta_with_delete, &unpack->ta);
    ta_with_delete.newest_stop_durable_ts = unpack->page_del.durable_timestamp;
    ta_with_delete.newest_txn = unpack->page_del.txnid;
    ta_with_delete.newest_stop_ts = unpack->page_del.timestamp;
    ta_with_delete.newest_stop_txn = unpack->page_del.txnid;
    ret = __wt_time_aggregate_validate(session, &ta_with_delete, addr != NULL ? &addr->ta : NULL,
      F_ISSET(session, WT_SESSION_QUIET_CORRUPT_FILE));
    if (ret != 0)
        WT_RET_VRFY_RETVAL(session, ret,
          "fast-delete cell %" PRIu32 " on page at %s failed adjusted timestamp validation",
          cell_num - 1, tag);

    /*
     * The other elements of the structure are not stored on disk and are set unconditionally by the
     * unpack code, so just assert about them. Prepared fast-truncates are not allowed to be
     * evicted.
     */
    WT_ASSERT(session, unpack->page_del.prepare_state == 0);
    WT_ASSERT(session, unpack->page_del.committed == true);

    return (0);
}

/*
 * __verify_row_key_order_check --
 *     Check key ordering for row-store pages.
 */
static int
__verify_row_key_order_check(
  WT_ITEM *last, uint32_t last_cell_num, WT_ITEM *current, uint32_t cell_num, WT_VERIFY_INFO *vi)
{
    WT_BTREE *btree;
    WT_DECL_ITEM(tmp1);
    WT_DECL_ITEM(tmp2);
    WT_DECL_RET;
    int cmp;

    btree = S2BT(vi->session);
    WT_RET(__wt_compare(vi->session, btree->collator, last, current, &cmp));
    if (cmp < 0)
        return (0);

    WT_ERR(__wt_scr_alloc(vi->session, 0, &tmp1));
    WT_ERR(__wt_scr_alloc(vi->session, 0, &tmp2));

    ret = WT_ERROR;
    WT_ERR_VRFY(vi->session, vi->flags,
      "the %" PRIu32 " and %" PRIu32 " keys on page at %s are incorrectly sorted: %s, %s",
      last_cell_num, cell_num, vi->tag,
      __wt_buf_set_printable_format(
        vi->session, last->data, last->size, btree->key_format, false, tmp1),
      __wt_buf_set_printable_format(
        vi->session, current->data, current->size, btree->key_format, false, tmp2));

err:
    __wt_scr_free(vi->session, &tmp1);
    __wt_scr_free(vi->session, &tmp2);
    return (ret);
}

/*
 * __verify_dsk_row_int --
 *     Walk a WT_PAGE_ROW_INT disk page and verify it.
 */
static int
__verify_dsk_row_int(WT_VERIFY_INFO *vi)
{
    WT_BM *bm;
    WT_BTREE *btree;
    WT_CELL *cell;
    WT_CELL_UNPACK_ADDR *unpack, _unpack;
    WT_DECL_ITEM(current);
    WT_DECL_ITEM(last);
    WT_DECL_ITEM(tmp);
    WT_DECL_RET;
    enum { FIRST, WAS_KEY, WAS_VALUE } last_cell_type;
    uint32_t cell_num, cell_type, i, key_cnt;
    uint8_t *end;

    btree = S2BT(vi->session);
    bm = btree->bm;
    unpack = &_unpack;

    WT_ERR(__wt_scr_alloc(vi->session, 0, &current));
    WT_ERR(__wt_scr_alloc(vi->session, 0, &last));

    end = (uint8_t *)vi->dsk + vi->dsk->mem_size;

    last_cell_type = FIRST;
    cell_num = 0;
    key_cnt = 0;
    WT_CELL_FOREACH_VRFY (vi->session, vi->dsk, cell, unpack, i) {
        ++cell_num;

        /* Carefully unpack the cell. */
        ret = __wt_cell_unpack_safe(vi->session, vi->dsk, cell, unpack, NULL, end);
        if (ret != 0) {
            (void)__err_cell_corrupt(ret, vi);
            goto err;
        }

        /* Check the raw and collapsed cell types. */
        WT_ERR(__err_cell_type(unpack->raw, vi));
        WT_ERR(__err_cell_type(unpack->type, vi));
        cell_type = unpack->type;

        /* Internal row-store cells should not have prefix compression or recno/rle fields. */
        if (unpack->prefix != 0)
            WT_ERR_VRFY(vi->session, vi->flags,
              "the %" PRIu32 " cell on page at %s has a non-zero prefix", cell_num, vi->tag);
        if (unpack->v != 0)
            WT_ERR_VRFY(vi->session, vi->flags,
              "the %" PRIu32 " cell on page at %s has a non-zero rle/recno field", cell_num,
              vi->tag);

        /*
         * Check ordering relationships between the WT_CELL entries. For row-store internal pages,
         * check for:
         *	- two values in a row,
         *	- two keys in a row,
         *	- a value as the first cell on a page.
         */
        switch (cell_type) {
        case WT_CELL_KEY:
        case WT_CELL_KEY_OVFL:
            ++key_cnt;
            switch (last_cell_type) {
            case FIRST:
            case WAS_VALUE:
                break;
            case WAS_KEY:
                WT_ERR_VRFY(vi->session, vi->flags,
                  "cell %" PRIu32 " on page at %s is the first of two adjacent keys", cell_num - 1,
                  vi->tag);
            }
            last_cell_type = WAS_KEY;
            break;
        case WT_CELL_ADDR_DEL:
        case WT_CELL_ADDR_INT:
        case WT_CELL_ADDR_LEAF:
        case WT_CELL_ADDR_LEAF_NO:
            switch (last_cell_type) {
            case FIRST:
                WT_ERR_VRFY(vi->session, vi->flags, "page at %s begins with a value", vi->tag);
            case WAS_KEY:
                break;
            case WAS_VALUE:
                WT_ERR_VRFY(vi->session, vi->flags,
                  "cell %" PRIu32 " on page at %s is the first of two adjacent values",
                  cell_num - 1, vi->tag);
            }
            last_cell_type = WAS_VALUE;
            break;
        }

        /* Check the validity window. */
        switch (cell_type) {
        case WT_CELL_ADDR_DEL:
        case WT_CELL_ADDR_INT:
        case WT_CELL_ADDR_LEAF:
        case WT_CELL_ADDR_LEAF_NO:
            WT_ERR(__verify_dsk_addr_validity(unpack, vi));
            break;
        }

        /* Check if any referenced item has an invalid address. */
        switch (cell_type) {
        case WT_CELL_ADDR_DEL:
        case WT_CELL_ADDR_INT:
        case WT_CELL_ADDR_LEAF:
        case WT_CELL_ADDR_LEAF_NO:
        case WT_CELL_KEY_OVFL:
            if ((ret = bm->addr_invalid(bm, vi->session, unpack->data, unpack->size)) == EINVAL)
                (void)__err_cell_corrupt_or_eof(ret, vi);
            WT_ERR(ret);
            break;
        }

        /* Check that any fast-delete info is consistent with the validity window. */
        if (cell_type == WT_CELL_ADDR_DEL && F_ISSET(vi->dsk, WT_PAGE_FT_UPDATE))
            WT_ERR(
              __verify_dsk_addr_page_del(vi->session, unpack, cell_num, vi->page_addr, vi->tag));

        /*
         * Remaining checks are for key order. If this cell isn't a key, we're done, move to the
         * next cell. If this cell is an overflow item, instantiate the key and compare it with the
         * last key.
         */
        switch (cell_type) {
        case WT_CELL_KEY:
            /* Get the cell's data/length and make sure we have enough buffer space. */
            WT_ERR(__wt_buf_init(vi->session, current, unpack->size));

            /* Copy the data into place. */
            memcpy((uint8_t *)current->mem, unpack->data, unpack->size);
            current->size = unpack->size;
            break;
        case WT_CELL_KEY_OVFL:
            WT_ERR(__wt_dsk_cell_data_ref_addr(vi->session, vi->dsk->type, unpack, current));
            break;
        default:
            /* Not a key -- continue with the next cell. */
            continue;
        }

        /*
         * Compare the current key against the last key.
         *
         * Be careful about the 0th key on internal pages: we only store the first byte and custom
         * collators may not be able to handle truncated keys.
         */
        if (cell_num > 3)
            WT_ERR(__verify_row_key_order_check(last, cell_num - 2, current, cell_num, vi));

        /* Swap the buffers. */
        tmp = last;
        last = current;
        current = tmp;
    }
    WT_ERR(__verify_dsk_memsize(cell, vi));

    /*
     * On row-store internal pages, the key count should be equal to half the number of physical
     * entries.
     */
    if (key_cnt * 2 != vi->dsk->u.entries)
        WT_ERR_VRFY(vi->session, vi->flags,
          "%s page at %s has a key count of %" PRIu32 " and a physical entry count of %" PRIu32,
          __wt_page_type_string(vi->dsk->type), vi->tag, key_cnt, vi->dsk->u.entries);

    if (0) {
err:
        if (ret == 0)
            ret = WT_ERROR;
    }
    __wt_scr_free(vi->session, &current);
    __wt_scr_free(vi->session, &last);
    return (ret);
}

/*
 * __verify_dsk_row_leaf --
 *     Walk a WT_PAGE_ROW_LEAF disk page and verify it.
 */
static int
__verify_dsk_row_leaf(WT_VERIFY_INFO *vi)
{
    WT_BM *bm;
    WT_BTREE *btree;
    WT_CELL *cell;
    WT_CELL_UNPACK_KV *unpack, _unpack;
    WT_DECL_ITEM(current);
    WT_DECL_ITEM(last_ovfl);
    WT_DECL_ITEM(last_pfx);
    WT_DECL_RET;
    WT_ITEM *last;
    enum { FIRST, WAS_KEY, WAS_VALUE } last_cell_type;
    size_t prefix;
    uint32_t cell_num, cell_type, i, key_cnt, last_cell_num;
    uint8_t *end;

    btree = S2BT(vi->session);
    bm = btree->bm;
    unpack = &_unpack;

    WT_ERR(__wt_scr_alloc(vi->session, 0, &current));
    WT_ERR(__wt_scr_alloc(vi->session, 0, &last_pfx));
    WT_ERR(__wt_scr_alloc(vi->session, 0, &last_ovfl));
    last = last_ovfl;

    end = (uint8_t *)vi->dsk + vi->dsk->mem_size;

    last_cell_type = FIRST;
    cell_num = last_cell_num = 0;
    key_cnt = 0;
    WT_CELL_FOREACH_VRFY (vi->session, vi->dsk, cell, unpack, i) {
        ++cell_num;

        /* Carefully unpack the cell. */
        ret = __wt_cell_unpack_safe(vi->session, vi->dsk, cell, NULL, unpack, end);
        if (ret != 0) {
            (void)__err_cell_corrupt(ret, vi);
            goto err;
        }

        /* Check the raw and collapsed cell types. */
        WT_ERR(__err_cell_type(unpack->raw, vi));
        WT_ERR(__err_cell_type(unpack->type, vi));
        cell_type = unpack->type;

        /* Leaf row-store cells should not have recno/rle fields. */
        if (unpack->v != 0)
            WT_ERR_VRFY(vi->session, vi->flags,
              "the %" PRIu32 " cell on page at %s has a non-zero rle/recno field", cell_num,
              vi->tag);

        /*
         * Check ordering relationships between the WT_CELL entries. For row-store leaf pages, check
         * for:
         *	- two values in a row,
         *	- a value as the first cell on a page.
         */
        switch (cell_type) {
        case WT_CELL_KEY:
        case WT_CELL_KEY_OVFL:
            ++key_cnt;
            last_cell_type = WAS_KEY;
            break;
        case WT_CELL_VALUE:
        case WT_CELL_VALUE_OVFL:
            switch (last_cell_type) {
            case FIRST:
                WT_ERR_VRFY(vi->session, vi->flags, "page at %s begins with a value", vi->tag);
            case WAS_KEY:
                break;
            case WAS_VALUE:
                WT_ERR_VRFY(vi->session, vi->flags,
                  "cell %" PRIu32 " on page at %s is the first of two adjacent values",
                  cell_num - 1, vi->tag);
            }
            last_cell_type = WAS_VALUE;
            break;
        }

        /* Check the validity window. */
        switch (cell_type) {
        case WT_CELL_VALUE:
        case WT_CELL_VALUE_OVFL:
            WT_ERR(__verify_dsk_value_validity(unpack, vi));
            break;
        }

        /* Check if any referenced item has an invalid address. */
        switch (cell_type) {
        case WT_CELL_KEY_OVFL:
        case WT_CELL_VALUE_OVFL:
            if ((ret = bm->addr_invalid(bm, vi->session, unpack->data, unpack->size)) == EINVAL)
                (void)__err_cell_corrupt_or_eof(ret, vi);
            WT_ERR(ret);
            break;
        }

        /*
         * Remaining checks are for key order and prefix compression. If this cell isn't a key,
         * we're done, move to the next cell. If this cell is an overflow item, instantiate the key
         * and compare it with the last key. Otherwise, we have to deal with prefix compression.
         */
        switch (cell_type) {
        case WT_CELL_KEY:
            break;
        case WT_CELL_KEY_OVFL:
            WT_ERR(__wt_dsk_cell_data_ref_kv(vi->session, vi->dsk->type, unpack, current));
            goto key_compare;
        default:
            /* Not a key -- continue with the next cell. */
            continue;
        }

        /*
         * Prefix compression checks.
         *
         * Confirm the first non-overflow key on a page has a zero prefix compression count.
         */
        prefix = unpack->prefix;
        if (last_pfx->size == 0 && prefix != 0)
            WT_ERR_VRFY(vi->session, vi->flags,
              "the %" PRIu32
              " key on page at %s is the first non-overflow key on the page and has a non-zero "
              "prefix compression value",
              cell_num, vi->tag);

        /* Confirm the prefix compression count is possible. */
        if (cell_num > 1 && prefix > last->size)
            WT_ERR_VRFY(vi->session, vi->flags,
              "key %" PRIu32 " on page at %s has a prefix compression count of %" WT_SIZET_FMT
              ", larger than the length of the previous key, %" WT_SIZET_FMT,
              cell_num, vi->tag, prefix, last->size);

        /*
         * Get the cell's data/length and make sure we have enough buffer space.
         */
        WT_ERR(__wt_buf_init(vi->session, current, prefix + unpack->size));

        /* Copy the prefix then the data into place. */
        if (prefix != 0)
            memcpy(current->mem, last->data, prefix);
        memcpy((uint8_t *)current->mem + prefix, unpack->data, unpack->size);
        current->size = prefix + unpack->size;

key_compare:
        /*
         * Compare the current key against the last key.
         */
        if (cell_num > 1)
            WT_ERR(__verify_row_key_order_check(last, last_cell_num, current, cell_num, vi));
        last_cell_num = cell_num;

        /*
         * Swap the buffers: last always references the last key entry, last_pfx and last_ovfl
         * reference the last prefix-compressed and last overflow key entries. Current gets pointed
         * to the buffer we're not using this time around, which is where the next key goes.
         */
        last = current;
        if (cell_type == WT_CELL_KEY) {
            current = last_pfx;
            last_pfx = last;
        } else {
            current = last_ovfl;
            last_ovfl = last;
        }
        WT_ASSERT(vi->session, last != current);
    }
    WT_ERR(__verify_dsk_memsize(cell, vi));

    /*
     * On standard row-store leaf pages there's no check to make, there may be more keys than values
     * as zero-length values aren't physically stored on the page. On row-store leaf pages, where
     * the "no empty values" flag is set, the key count should be equal to half the number of
     * physical entries. On row-store leaf pages where the "all empty values" flag is set, the key
     * count should be equal to the number of physical entries.
     */
    if (F_ISSET(vi->dsk, WT_PAGE_EMPTY_V_ALL) && key_cnt != vi->dsk->u.entries)
        WT_ERR_VRFY(vi->session, vi->flags,
          "%s page at %s with the 'all empty values' flag set has a key count of %" PRIu32
          " and a physical entry count of %" PRIu32,
          __wt_page_type_string(vi->dsk->type), vi->tag, key_cnt, vi->dsk->u.entries);
    if (F_ISSET(vi->dsk, WT_PAGE_EMPTY_V_NONE) && key_cnt * 2 != vi->dsk->u.entries)
        WT_ERR_VRFY(vi->session, vi->flags,
          "%s page at %s with the 'no empty values' flag set has a key count of %" PRIu32
          " and a physical entry count of %" PRIu32,
          __wt_page_type_string(vi->dsk->type), vi->tag, key_cnt, vi->dsk->u.entries);

    if (0) {
err:
        if (ret == 0)
            ret = WT_ERROR;
    }
    __wt_scr_free(vi->session, &current);
    __wt_scr_free(vi->session, &last_pfx);
    __wt_scr_free(vi->session, &last_ovfl);
    return (ret);
}

/*
 * __verify_dsk_memsize --
 *     Verify the last cell on the page matches the page's memory size.
 */
static int
__verify_dsk_memsize(WT_CELL *cell, WT_VERIFY_INFO *vi)
{
    size_t len;

    /*
     * We use the fact that cells exactly fill a page to detect the case of a row-store leaf page
     * where the last cell is a key (that is, there's no subsequent value cell). Check for any page
     * type containing cells.
     */
    len = WT_PTRDIFF((uint8_t *)vi->dsk + vi->dsk->mem_size, cell);
    if (len == 0)
        return (0);
    WT_RET_VRFY(vi->session,
      "%s page at %s has %" WT_SIZET_FMT " unexpected bytes of data after the last cell",
      __wt_page_type_string(vi->dsk->type), vi->tag, len);
}

/*
 * __verify_dsk_chunk --
 *     Verify a Chunk O' Data on a Btree page.
 */
static int
__verify_dsk_chunk(WT_VERIFY_INFO *vi)
{
    WT_BTREE *btree;
    uint32_t datalen;
    uint8_t *end, *p;

    btree = S2BT(vi->session);
    end = (uint8_t *)vi->dsk + vi->dsk->mem_size;
    datalen = vi->dsk->u.datalen;

    /*
     * Fixed-length column-store and overflow pages are simple chunks of data-> Verify the data
     * doesn't overflow the end of the page.
     */
    p = WT_PAGE_HEADER_BYTE(btree, vi->dsk);
    if (p + datalen > end)
        WT_RET_VRFY(vi->session, "data on page at %s extends past the end of the page", vi->tag);

    /* Any bytes after the data chunk should be nul bytes. */
    for (p += datalen; p < end; ++p)
        if (*p != '\0')
            WT_RET_VRFY(vi->session, "%s page at %s has non-zero trailing bytes",
              __wt_page_type_string(vi->dsk->type), vi->tag);

    return (0);
}

/*
 * __err_cell_corrupt --
 *     Generic corrupted cell, we couldn't read it.
 */
static int
__err_cell_corrupt(int retval, WT_VERIFY_INFO *vi)
{
    WT_RET_VRFY_RETVAL(vi->session, retval, "item %" PRIu32 " on page at %s is a corrupted cell",
      vi->cell_num, vi->tag);
}

/*
 * __err_cell_corrupt_or_eof --
 *     Generic corrupted cell or item references non-existent file pages error.
 */
static int
__err_cell_corrupt_or_eof(int retval, WT_VERIFY_INFO *vi)
{
    WT_RET_VRFY_RETVAL(vi->session, retval,
      "item %" PRIu32 " on page at %s is a corrupted cell or references non-existent file pages",
      vi->cell_num, vi->tag);
}

/*
 * __wti_cell_type_check --
 *     Check the cell type against the page type.
 */
bool
__wti_cell_type_check(uint8_t cell_type, uint8_t dsk_type)
{
    switch (cell_type) {
    case WT_CELL_ADDR_DEL:
    case WT_CELL_ADDR_INT:
    case WT_CELL_ADDR_LEAF:
    case WT_CELL_ADDR_LEAF_NO:
        if (dsk_type == WT_PAGE_ROW_INT)
            return (true);
        break;
    case WT_CELL_DEL:
        break;
    case WT_CELL_KEY_SHORT:
    case WT_CELL_KEY:
    case WT_CELL_KEY_OVFL:
        if (dsk_type == WT_PAGE_ROW_INT || dsk_type == WT_PAGE_ROW_LEAF)
            return (true);
        break;
    case WT_CELL_KEY_PFX:
    case WT_CELL_KEY_SHORT_PFX:
        if (dsk_type == WT_PAGE_ROW_LEAF)
            return (true);
        break;
    case WT_CELL_KEY_OVFL_RM:
    case WT_CELL_VALUE_OVFL_RM:
        /*
         * Removed overflow cells are in-memory only, it's an error to ever see one on a disk page.
         */
        break;
    case WT_CELL_VALUE:
    case WT_CELL_VALUE_COPY:
    case WT_CELL_VALUE_OVFL:
    case WT_CELL_VALUE_SHORT:
        if (dsk_type == WT_PAGE_ROW_LEAF)
            return (true);
        break;
    }
    return (false);
}

/*
 * __err_cell_type --
 *     Generic illegal cell type for a particular page type error.
 */
static int
__err_cell_type(uint8_t cell_type, WT_VERIFY_INFO *vi)
{
    if (!__wti_cell_type_check(cell_type, vi->dsk->type))
        WT_RET_VRFY(vi->session,
          "illegal cell and page type combination: cell %" PRIu32
          " on page at %s is a %s cell on a %s page",
          vi->cell_num, vi->tag, __wti_cell_type_string(cell_type),
          __wt_page_type_string(vi->dsk->type));
    return (0);
}
