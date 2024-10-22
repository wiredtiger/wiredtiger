/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_block_disagg_corrupt --
 *     Report a block has been corrupted, external API.
 */
int
__wt_block_disagg_corrupt(
  WT_BM *bm, WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size)
{
    WT_DECL_ITEM(tmp);
    WT_DECL_RET;
    WT_PAGE_BLOCK_META block_meta;
    uint64_t checkpoint_id, page_id, reconciliation_id;
    uint32_t checksum, size;

    /* Read the block. */
    WT_RET(__wt_scr_alloc(session, 0, &tmp));
    WT_ERR(__wt_block_disagg_read(bm, session, tmp, &block_meta, addr, addr_size));

    /* Crack the cookie, dump the block. */
    WT_ERR(__wt_block_disagg_addr_unpack(
      &addr, addr_size, &page_id, &checkpoint_id, &reconciliation_id, &size, &checksum));
    WT_ERR(__wt_bm_corrupt_dump(session, tmp, 0, (wt_off_t)page_id, size, checksum));

err:
    __wt_scr_free(session, &tmp);
    return (ret);
}

/*
 * __block_disagg_read_multiple --
 *     Read a page referenced by a page id, checkpoint id, reconciliation id triple into multiple
 *     buffers.
 */
static int
__block_disagg_read_multiple(WT_SESSION_IMPL *session, WT_BLOCK_DISAGG *block_disagg,
  WT_PAGE_BLOCK_META *block_meta, uint64_t page_id, uint64_t checkpoint_id,
  uint64_t reconciliation_id, uint32_t size, uint32_t checksum, WT_ITEM *results_array,
  u_int *results_count)
{
    WT_BLOCK_DISAGG_HEADER *blk, swap;
    WT_DECL_RET;
    WT_ITEM *current;
    WT_PAGE_LOG_GET_ARGS get_args;

    WT_UNUSED(reconciliation_id);

    /*
     * Disaggregated storage only supports up to 32 items. We shouldn't ask for more.
     */
    WT_ASSERT(session, *results_count <= 32);

    WT_CLEAR(get_args);
    if (block_meta != NULL)
        WT_CLEAR(*block_meta);

    __wt_verbose(session, WT_VERB_READ, "off %" PRIuMAX ", size %" PRIu32 ", checksum %" PRIu32,
      (uintmax_t)page_id, size, checksum);

    WT_STAT_CONN_INCR(session, disagg_block_get);
    WT_STAT_CONN_INCR(session, block_read);
    WT_STAT_CONN_INCRV(session, block_byte_read, size);

    /*
     * Output buffers do not need to be preallocated, the PALI interface does that.
     */
    WT_ERR(block_disagg->plhandle->plh_get(block_disagg->plhandle, &session->iface, page_id,
      checkpoint_id, &get_args, results_array, results_count));

    /*
     * XXX We'll need to handle deltas here.
     */
    WT_ASSERT(session, *results_count == 1);

    current = &results_array[0];

    if (block_meta != NULL) {
        /* Set the other metadata returned by the Page Service. */
        block_meta->page_id = page_id;
        block_meta->checkpoint_id = checkpoint_id;
        /* XXX: verify the reconcilation id from the header */
        block_meta->reconciliation_id = reconciliation_id;
        block_meta->backlink_checkpoint_id = get_args.backlink_checkpoint_id;
        block_meta->base_checkpoint_id = get_args.base_checkpoint_id;
        block_meta->disagg_lsn = get_args.lsn;
        block_meta->disagg_lsn_frontier = get_args.lsn_frontier;
        block_meta->delta_count = get_args.delta_count;
    }

    /*
     * We incrementally read through the structure before doing a checksum, do little- to big-endian
     * handling early on, and then select from the original or swapped structure as needed.
     */
    blk = WT_BLOCK_HEADER_REF(current->data);
    __wt_block_disagg_header_byteswap_copy(blk, &swap);
    if (swap.checksum == checksum) {
        blk->checksum = 0;
        if (__wt_checksum_match(current->data,
              F_ISSET(&swap, WT_BLOCK_DATA_CKSUM) ? size : WT_BLOCK_COMPRESS_SKIP, checksum)) {
            /*
             * Swap the page-header as needed; this doesn't belong here, but it's the best place to
             * catch all callers.
             */
            __wt_page_header_byteswap((void *)current->data);
            goto done;
        }

        if (!F_ISSET(session, WT_SESSION_QUIET_CORRUPT_FILE))
            __wt_errx(session,
              "%s: read checksum error for %" PRIu32
              "B block at "
              "offset %" PRIuMAX
              ": calculated block checksum "
              " doesn't match expected checksum",
              block_disagg->name, size, (uintmax_t)page_id);
    } else if (!F_ISSET(session, WT_SESSION_QUIET_CORRUPT_FILE))
        __wt_errx(session,
          "%s: read checksum error for %" PRIu32
          "B block at "
          "offset %" PRIuMAX
          ": block header checksum "
          "of %" PRIu32
          " doesn't match expected checksum "
          "of %" PRIu32,
          block_disagg->name, size, (uintmax_t)page_id, swap.checksum, checksum);

    if (!F_ISSET(session, WT_SESSION_QUIET_CORRUPT_FILE))
        WT_IGNORE_RET(__wt_bm_corrupt_dump(session, current, 0, (wt_off_t)page_id, size, checksum));

    /* Panic if a checksum fails during an ordinary read. */
    F_SET(S2C(session), WT_CONN_DATA_CORRUPTION);
    if (F_ISSET(session, WT_SESSION_QUIET_CORRUPT_FILE))
        WT_ERR(WT_ERROR);
    WT_ERR_PANIC(session, WT_ERROR, "%s: fatal read error", block_disagg->name);
err:
done:
    return (ret);
}

/*
 * __wt_block_disagg_read --
 *     A basic read of a single block is not supported in disaggregated storage.
 */
int
__wt_block_disagg_read(WT_BM *bm, WT_SESSION_IMPL *session, WT_ITEM *buf,
  WT_PAGE_BLOCK_META *block_meta, const uint8_t *addr, size_t addr_size)
{
    WT_UNUSED(bm);
    WT_UNUSED(session);
    WT_UNUSED(buf);
    WT_UNUSED(block_meta);
    WT_UNUSED(addr);
    WT_UNUSED(addr_size);

    return (ENOTSUP);
}

/*
 * __wt_block_disagg_read_multiple --
 *     Map or read address cookie referenced page and deltas into an array of buffers, with memory
 *     managed by a memory buffer.
 */
int
__wt_block_disagg_read_multiple(WT_BM *bm, WT_SESSION_IMPL *session, WT_PAGE_BLOCK_META *block_meta,
  const uint8_t *addr, size_t addr_size, WT_ITEM *buffer_array, u_int *buffer_count)
{
    WT_BLOCK_DISAGG *block_disagg;
    uint64_t checkpoint_id, page_id, reconciliation_id;
    uint32_t checksum, size;

    block_disagg = (WT_BLOCK_DISAGG *)bm->block;

    /* Crack the cookie. */
    WT_RET(__wt_block_disagg_addr_unpack(
      &addr, addr_size, &page_id, &checkpoint_id, &reconciliation_id, &size, &checksum));

    /* Read the block. */
    WT_RET(__block_disagg_read_multiple(session, block_disagg, block_meta, page_id, checkpoint_id,
      reconciliation_id, size, checksum, buffer_array, buffer_count));
    return (0);
}
