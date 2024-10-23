/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __bmd_addr_invalid --
 *     Return an error code if an address cookie is invalid.
 */
static int
__bmd_addr_invalid(WT_BM *bm, WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size)
{
    WT_UNUSED(bm);
    WT_UNUSED(session);

    return (__wt_block_disagg_addr_invalid(addr, addr_size));
}

/*
 * __bmd_block_header --
 *     Return the size of the block header.
 */
static u_int
__bmd_block_header(WT_BM *bm)
{
    WT_UNUSED(bm);

    return ((u_int)WT_BLOCK_DISAGG_HEADER_SIZE);
}

/*
 * __bmd_close --
 *     Close a file.
 */
static int
__bmd_close(WT_BM *bm, WT_SESSION_IMPL *session)
{
    WT_DECL_RET;

    if (bm == NULL) /* Safety check */
        return (0);

    ret = __wt_block_disagg_close(session, (WT_BLOCK_DISAGG *)bm->block);

    __wt_overwrite_and_free(session, bm);
    return (ret);
}

/*
 * __bmd_free --
 *     Free a block of space to the underlying file.
 */
static int
__bmd_free(WT_BM *bm, WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size)
{
    /*
     * Nothing to do for now - this should notify the space manager that the page is no longer
     * required, but that isn't necessary to see something working.
     */
    WT_UNUSED(bm);
    WT_UNUSED(session);
    WT_UNUSED(addr);
    WT_UNUSED(addr_size);
    return (0);
}

/*
 * __bmd_stat --
 *     Block-manager statistics.
 */
static int
__bmd_stat(WT_BM *bm, WT_SESSION_IMPL *session, WT_DSRC_STATS *stats)
{
    __wt_block_disagg_stat(session, (WT_BLOCK_DISAGG *)bm->block, stats);
    return (0);
}

/*
 * __bmd_write --
 *     Write a buffer into a block, returning the block's address cookie.
 */
static int
__bmd_write(WT_BM *bm, WT_SESSION_IMPL *session, WT_ITEM *buf, WT_PAGE_BLOCK_META *block_meta,
  uint8_t *addr, size_t *addr_sizep, bool data_checksum, bool checkpoint_io)
{
    __wt_capacity_throttle(
      session, buf->size, checkpoint_io ? WT_THROTTLE_CKPT : WT_THROTTLE_EVICT);
    return (__wt_block_disagg_write(
      session, bm->block, buf, block_meta, addr, addr_sizep, data_checksum, checkpoint_io));
}

/*
 * __bmd_write_size --
 *     Return the buffer size required to write a block.
 */
static int
__bmd_write_size(WT_BM *bm, WT_SESSION_IMPL *session, size_t *sizep)
{
    WT_UNUSED(bm);
    WT_UNUSED(session);

    return (__wt_block_disagg_write_size(sizep));
}

/*
 * __bmd_method_set --
 *     Set up the legal methods.
 */
static void
__bmd_method_set(WT_BM *bm, bool readonly)
{
    WT_UNUSED(readonly);

    bm->addr_invalid = __bmd_addr_invalid;
    bm->addr_string = __wt_block_disagg_addr_string;
    bm->block_header = __bmd_block_header;
    bm->checkpoint = __wt_block_disagg_checkpoint;
    bm->checkpoint_load = __wt_block_disagg_checkpoint_load;
    bm->checkpoint_resolve = __wt_block_disagg_checkpoint_resolve;
    bm->checkpoint_start = __wt_block_disagg_checkpoint_start;
    bm->checkpoint_unload = __wt_block_disagg_checkpoint_unload;
    bm->close = __bmd_close;
    bm->compact_end = __wt_bmp_compact_end;
    bm->compact_page_skip = __wt_bmp_compact_page_skip;
    bm->compact_skip = __wt_bmp_compact_skip;
    bm->compact_start = __wt_bmp_compact_start;
    bm->corrupt = __wt_block_disagg_corrupt;
    bm->free = __bmd_free;
    bm->is_mapped = __wt_bmp_is_mapped;
    bm->map_discard = __wt_bmp_map_discard;
    bm->read = __wt_block_disagg_read;
    bm->read_multiple = __wt_block_disagg_read_multiple;
    bm->salvage_end = __wt_bmp_salvage_end;
    bm->salvage_next = __wt_bmp_salvage_next;
    bm->salvage_start = __wt_bmp_salvage_start;
    bm->salvage_valid = __wt_bmp_salvage_valid;
    bm->size = __wt_block_disagg_manager_size;
    bm->stat = __bmd_stat;
    bm->sync = __wt_bmp_sync;
    bm->verify_addr = __wt_bmp_verify_addr;
    bm->verify_end = __wt_bmp_verify_end;
    bm->verify_start = __wt_bmp_verify_start;
    bm->write = __bmd_write;
    bm->write_size = __bmd_write_size;
}

/*
 * __wt_block_disagg_manager_owns_object --
 *     Check whether the object being opened should be managed by this block manager.
 */
bool
__wt_block_disagg_manager_owns_object(WT_SESSION_IMPL *session, const char *uri)
{
    /*
     * It's a janky check that should be made better, but assume any handle with a page log belongs
     * to this object-based block manager for now.
     */
    if (session->dhandle == NULL || S2BT(session) == NULL)
        return (false);
    if (WT_PREFIX_MATCH(uri, "file:") && (S2BT(session)->page_log != NULL))
        return (true);
    return (false);
}

/*
 * __wt_block_disagg_manager_open --
 *     Open a file.
 */
int
__wt_block_disagg_manager_open(WT_SESSION_IMPL *session, const char *uri, const char *cfg[],
  bool forced_salvage, bool readonly, WT_BM **bmp)
{
    WT_BM *bm;
    WT_DECL_RET;

    *bmp = NULL;

    WT_RET(__wt_calloc_one(session, &bm));
    bm->is_remote = true;

    __bmd_method_set(bm, false);

    uri += strlen("file:");

    WT_ERR(__wt_block_disagg_open(session, uri, cfg, forced_salvage, readonly, &bm->block));

    *bmp = bm;
    return (0);

err:
    WT_TRET(bm->close(bm, session));
    return (ret);
}
