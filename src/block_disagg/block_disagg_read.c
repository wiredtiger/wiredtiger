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
    uint32_t orig_count, retry;
    int32_t result, last;
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

    __wt_verbose(session, WT_VERB_READ,
      "page_id %" PRIu64 ", checkpoint_id %" PRIu64 ", reconciliation_id %" PRIu64 ", size %" PRIu32
      ", checksum %" PRIu32,
      page_id, checkpoint_id, reconciliation_id, size, checksum);

    WT_STAT_CONN_INCR(session, disagg_block_get);
    WT_STAT_CONN_INCR(session, block_read);
    WT_STAT_CONN_INCRV(session, block_byte_read, size);

    orig_count = *results_count;

    if (0) {
reread:
        /*
         * Retry a read again. This code may go away once we establish a way to ask for a particular
         * delta.
         */
        __wt_verbose_notice(session, WT_VERB_READ,
          "retry #%" PRIu32 " for page_id %" PRIu64 ", checkpoint_id %" PRIu64
          ", reconciliation_id %" PRIu64 ", size %" PRIu32 ", checksum %" PRIu32,
          retry, page_id, checkpoint_id, reconciliation_id, size, checksum);
        __wt_sleep(0, 10000 + retry * 5000);
        memset(results_array, 0, *results_count * sizeof(results_array[0]));
        *results_count = orig_count;
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

    last = (int32_t)(*results_count - 1);

    /*
     * Walk through all the results from most recent delta backwards to the base page. This makes it
     * easier to do checks.
     */
    for (result = last; result >= 0; result--) {
        current = &results_array[result];
        WT_ASSERT(session, current->size < UINT32_MAX);
        size = (uint32_t)current->size;
        is_delta = (result != 0);

        /*
         * Do little- to big-endian handling early on.
         */
        if (is_delta)
            blk = WT_BLOCK_HEADER_REF_FOR_DELTAS(current->data);
        else
            blk = WT_BLOCK_HEADER_REF(current->data);
        __wt_block_disagg_header_byteswap_copy(blk, &swap);

        /*
         * Make a quick check of the checksum on the final delta, it should match the cookie. If it
         * doesn't and the reconciliation id does not match what is expected, retry with a delay.
         *
         * This code may go away once we establish a way to ask for a particular delta.
         */
        if (result == last && swap.checksum != checksum &&
          swap.reconciliation_id < reconciliation_id && retry < 100)
            goto reread;

        if (swap.checksum == checksum) {
            blk->checksum = 0;
            if (__wt_checksum_match(current->data,
                  F_ISSET(&swap, WT_BLOCK_DATA_CKSUM) ? size : WT_BLOCK_COMPRESS_SKIP, checksum) &&
              swap.reconciliation_id == reconciliation_id) {

                expected_magic =
                  (is_delta ? WT_BLOCK_DISAGG_MAGIC_DELTA : WT_BLOCK_DISAGG_MAGIC_BASE);
                if (swap.magic != expected_magic) {
                    __wt_errx(session,
                      "%s: magic error for %" PRIu32
                      "B block at "
                      "page %" PRIuMAX " ckpt %" PRIu64 ", magic %" PRIu8
                      ": doesn't match expected magic of %" PRIu8,
                      block_disagg->name, size, page_id, checkpoint_id, swap.magic, expected_magic);
                    goto corrupt;
                }

                if (swap.compatible_version > WT_BLOCK_DISAGG_COMPATIBLE_VERSION) {
                    __wt_errx(session,
                      "%s: compatible version error for %" PRIu32
                      "B block at "
                      "page %" PRIuMAX " ckpt %" PRIu64 ", version %" PRIu8
                      ": is greater than compatible version of %" PRIu8,
                      block_disagg->name, size, page_id, checkpoint_id, swap.compatible_version,
                      WT_BLOCK_DISAGG_COMPATIBLE_VERSION);
                    goto corrupt;
                }

                /*
                 * Swap the page-header as needed; this doesn't belong here, but it's the best place
                 * to catch all callers.
                 */
                if (is_delta)
                    __wt_delta_header_byteswap((void *)current->data);
                else
                    __wt_page_header_byteswap((void *)current->data);
                checksum = swap.previous_checksum;

                if (result == last && block_meta != NULL) {
                    /* Set the other metadata returned by the Page Service. */
                    block_meta->page_id = page_id;
                    block_meta->checkpoint_id = checkpoint_id;
                    block_meta->reconciliation_id = reconciliation_id;
                    block_meta->backlink_checkpoint_id = get_args.backlink_checkpoint_id;
                    block_meta->base_checkpoint_id = get_args.base_checkpoint_id;
                    block_meta->disagg_lsn = get_args.lsn;
                    block_meta->delta_count = get_args.delta_count;
                    block_meta->checksum = checksum;
                }
                continue;
            }

            if (!F_ISSET(session, WT_SESSION_QUIET_CORRUPT_FILE))
                __block_disagg_read_checksum_err(session, block_disagg->name, size, page_id,
                  checkpoint_id, swap.checksum, checksum, swap.reconciliation_id, reconciliation_id,
                  "calculated block checksum");
        } else if (!F_ISSET(session, WT_SESSION_QUIET_CORRUPT_FILE))
            __block_disagg_read_checksum_err(session, block_disagg->name, size, page_id,
              checkpoint_id, swap.checksum, checksum, swap.reconciliation_id, reconciliation_id,
              "block header checksum");

corrupt:
        if (!F_ISSET(session, WT_SESSION_QUIET_CORRUPT_FILE))
            WT_IGNORE_RET(
              __wt_bm_corrupt_dump(session, current, 0, (wt_off_t)page_id, size, checksum));

        /* Panic if a checksum fails during an ordinary read. */
        F_SET(S2C(session), WT_CONN_DATA_CORRUPTION);
        if (F_ISSET(session, WT_SESSION_QUIET_CORRUPT_FILE))
            WT_ERR(WT_ERROR);
        WT_ERR_PANIC(session, WT_ERROR, "%s: fatal read error", block_disagg->name);
    }
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
