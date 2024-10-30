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
 * __block_disagg_read_checksum_err --
 *     Print a checksum or reconciliation id mismatch in a standard way.
 */
static void
__block_disagg_read_checksum_err(WT_SESSION_IMPL *session, const char *name, uint32_t size,
  uint64_t page_id, uint64_t checkpoint_id, uint32_t checksum, uint32_t expected_checksum,
  uint64_t rec_id, uint64_t expected_rec_id, const char *context_msg)
{
    __wt_errx(session,
      "%s: read checksum error for %" PRIu32
      "B block at "
      "page %" PRIuMAX ", ckpt %" PRIuMAX ": %s of %" PRIu32 " (%" PRIu64
      ") doesn't match expected checksum of %" PRIu32 " (%" PRIu64 ")",
      name, size, page_id, checkpoint_id, context_msg, checksum, rec_id, expected_checksum,
      expected_rec_id);
}

/*
 * __block_disagg_read_multiple --
 *     Read a full page along with its deltas, into multiple buffers. The page is referenced by a
 *     page id, checkpoint id pair.
 */
static int
__block_disagg_read_multiple(WT_SESSION_IMPL *session, WT_BLOCK_DISAGG *block_disagg,
  WT_PAGE_BLOCK_META *block_meta, uint64_t page_id, uint64_t checkpoint_id,
  uint64_t reconciliation_id, uint32_t size, uint32_t checksum, WT_ITEM *results_array,
  uint32_t *results_count)
{
    WT_BLOCK_DISAGG_HEADER *blk, swap;
    WT_DECL_RET;
    WT_ITEM *current;
    WT_PAGE_LOG_GET_ARGS get_args;
    uint32_t retry;
    int32_t result;
    uint8_t expected_magic;
    bool is_delta;

    retry = 0;

    /*
     * Disaggregated storage only supports up to a fixed number of items. We shouldn't ask for more.
     */
    WT_ASSERT(session, *results_count <= WT_DELTA_LIMIT);

    WT_CLEAR(get_args);
    if (block_meta != NULL)
        WT_CLEAR(*block_meta);

    __wt_verbose(session, WT_VERB_READ, "off %" PRIuMAX ", size %" PRIu32 ", checksum %" PRIu32,
      (uintmax_t)page_id, size, checksum);

    WT_STAT_CONN_INCR(session, disagg_block_get);
    WT_STAT_CONN_INCR(session, block_read);
    WT_STAT_CONN_INCRV(session, block_byte_read, size);

    if (0) {
reread:
        /*
         * Retry a read again. This code may go away once we establish a way to ask for a particular
         * delta.
         */
        __wt_sleep(0, 100 + retry * 100);
        memset(results_array, 0, *results_count * sizeof(results_array[0]));
        ++retry;
    }
    /*
     * Output buffers do not need to be preallocated, the PALI interface does that.
     */
    WT_ERR(block_disagg->plhandle->plh_get(block_disagg->plhandle, &session->iface, page_id,
      checkpoint_id, &get_args, results_array, results_count));

    WT_ASSERT(session, *results_count <= WT_DELTA_LIMIT);

    if (*results_count == 0) {
        /*
         * The page was not found for this page id. This would normally be an error, as we will
         * never ask for a page that we haven't previously written. However, if it hasn't
         * materialized yet in the page service, this can happen, so retry with a delay.
         *
         * This code may go away once we establish a way to ask for a particular delta, and the PALI
         * interface will be obligated to wait until it appears.
         */
        if (retry < 100)
            goto reread;
        return (WT_NOTFOUND);
    }

    /* Set the other metadata returned by the Page Service. */
    block_meta->page_id = page_id;
    block_meta->checkpoint_id = checkpoint_id;
    block_meta->reconciliation_id = reconciliation_id;
    block_meta->backlink_checkpoint_id = get_args.backlink_checkpoint_id;
    block_meta->base_checkpoint_id = get_args.base_checkpoint_id;
    block_meta->disagg_lsn = get_args.lsn;
    block_meta->delta_count = get_args.delta_count;
    block_meta->checksum = checksum;

err:
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
