/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __bmi_addr_invalid --
 *     Return an error code if an address cookie is invalid.
 */
static int
__bmi_addr_invalid(WT_BM *bm, WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size)
{
    WT_UNUSED(bm);

    return (__wti_block_inmem_addr_invalid(session, addr, addr_size));
}

/*
 * __bmi_block_header_init --
 *     Initialize the block header of a disk image laid out for writing.
 */
static void
__bmi_block_header_init(WT_BM *bm, WT_SESSION_IMPL *session, void *dsk)
{
    __wti_block_inmem_block_header_init(bm, session, dsk);
}

/*
 * __bmi_block_header_read --
 *     Return the size of the block header when reading an existing header.
 */
static u_int
__bmi_block_header_read(WT_BM *bm, WT_SESSION_IMPL *session, const void *dsk)
{
    return (__wti_block_inmem_block_header_read(bm, session, dsk));
}

/*
 * __bmi_block_header_write --
 *     Return the size of the block header when writing a new header.
 */
static u_int
__bmi_block_header_write(WT_BM *bm, WT_SESSION_IMPL *session)
{
    return (__wti_block_inmem_block_header_write(bm, session));
}

/*
 * __bmi_can_truncate --
 *     Return whether there's free space at the end of the file.
 */
static bool
__bmi_can_truncate(WT_BM *bm, WT_SESSION_IMPL *session)
{
    return (__wti_block_inmem_can_truncate(bm, session));
}

/*
 * __bmi_close --
 *     Close a file.
 */
static int
__bmi_close(WT_BM *bm, WT_SESSION_IMPL *session)
{
    WT_DECL_RET;

    if (bm == NULL)
        return (0);

    ret = __wti_block_inmem_close(session, (WT_BLOCK_INMEM *)bm->block);

    __wt_overwrite_and_free(session, bm);
    return (ret);
}

/*
 * __bmi_encrypt_skip_size --
 *     Return the skip size for encryption.
 */
static size_t
__bmi_encrypt_skip_size(WT_BM *bm, WT_SESSION_IMPL *session, const void *dsk)
{
    WT_UNUSED(bm);
    WT_UNUSED(session);
    WT_UNUSED(dsk);

    return (WT_BLOCK_HEADER_BYTE_SIZE);
}

/*
 * __bmi_stat --
 *     Block-manager statistics.
 */
static int
__bmi_stat(WT_BM *bm, WT_SESSION_IMPL *session, WT_DSRC_STATS *stats)
{
    return (__wti_block_inmem_stat(session, (WT_BLOCK_INMEM *)bm->block, stats));
}

/*
 * __bmi_write_size --
 *     Return the buffer size required to write a block.
 */
static int
__bmi_write_size(WT_BM *bm, WT_SESSION_IMPL *session, size_t *sizep)
{
    WT_UNUSED(bm);

    return (__wti_block_inmem_write_size(session, sizep));
}

/*
 * __bmi_method_set --
 *     Set up the legal methods.
 */
static void
__bmi_method_set(WT_BM *bm, bool readonly)
{
    WT_UNUSED(readonly);

    bm->addr_invalid = __bmi_addr_invalid;
    bm->addr_string = __wti_block_inmem_addr_string;
    bm->block_header_init = __bmi_block_header_init;
    bm->block_header_read_size = __bmi_block_header_read;
    bm->block_header_write_size = __bmi_block_header_write;
    bm->can_truncate = __bmi_can_truncate;
    bm->checkpoint = __wti_block_inmem_checkpoint;
    bm->checkpoint_last = __wti_block_inmem_checkpoint_last;
    bm->checkpoint_load = __wti_block_inmem_checkpoint_load;
    bm->checkpoint_resolve = __wti_block_inmem_checkpoint_resolve;
    bm->checkpoint_start = __wti_block_inmem_checkpoint_start;
    bm->checkpoint_unload = __wti_block_inmem_checkpoint_unload;
    bm->close = __bmi_close;
    bm->compact_end = __wti_block_inmem_checkpoint_start;
    bm->compact_page_rewrite = __wti_block_inmem_compact_page_rewrite;
    bm->compact_page_skip = __wti_block_inmem_compact_page_skip;
    bm->compact_progress = __wti_block_inmem_compact_progress;
    bm->compact_skip = __wti_block_inmem_compact_skip;
    bm->compact_start = __wti_block_inmem_checkpoint_start;
    bm->corrupt = __wti_block_inmem_corrupt;
    bm->encrypt_skip = __bmi_encrypt_skip_size;
    bm->free = __wti_block_inmem_free;
    bm->is_mapped = __wti_block_inmem_is_mapped;
    bm->map_discard = __wti_block_inmem_map_discard;
    bm->read = __wti_block_inmem_read;
    bm->salvage_end = __wti_block_inmem_checkpoint_start;
    bm->salvage_next = __wti_block_inmem_salvage_next;
    bm->salvage_start = __wti_block_inmem_checkpoint_start;
    bm->salvage_valid = __wti_block_inmem_salvage_valid;
    bm->size = __wti_block_inmem_manager_size;
    bm->stat = __bmi_stat;
    bm->sync = __wti_block_inmem_sync;
    bm->verify_addr = __wti_block_inmem_verify_addr;
    bm->verify_end = __wti_block_inmem_verify_end;
    bm->verify_start = __wti_block_inmem_verify_start;
    bm->write = __wti_block_inmem_write;
    bm->write_size = __bmi_write_size;
}

/*
 * __wt_block_inmem_manager_owns_object --
 *     Check whether the object being opened should be managed by this block manager.
 */
bool
__wt_block_inmem_manager_owns_object(WT_SESSION_IMPL *session, const char *uri, const char *cfg[])
{
    WT_CONFIG_ITEM cval;

    if (!WT_PREFIX_MATCH(uri, "file:"))
        return (false);

    /*
     * Connection-wide in_memory uses the default block manager with the in-memory file system;
     * only per-file in_memory=true selects this backend.
     */
    if (F_ISSET(S2C(session), WT_CONN_IN_MEMORY))
        return (false);

    return (__wt_config_gets(session, cfg, "in_memory", &cval) == 0 && cval.val != 0);
}

/*
 * __wt_block_inmem_manager_open --
 *     Open a file.
 */
int
__wt_block_inmem_manager_open(WT_SESSION_IMPL *session, const char *uri, const char *cfg[],
  bool forced_salvage, bool readonly, WT_BM **bmp)
{
    WT_BM *bm;
    WT_DECL_RET;

    *bmp = NULL;

    WT_RET(__wt_calloc_one(session, &bm));
    bm->is_live = true;

    __bmi_method_set(bm, readonly);

    uri += strlen("file:");

    WT_ERR(__wti_block_inmem_open(
      session, uri, cfg, forced_salvage, readonly, &bm->block));

    WT_ASSERT_ALWAYS(session, ((WT_BLOCK_INMEM *)bm->block)->tableid == S2BT(session)->id,
      "block manager bound to a block handle for a different table");

    *bmp = bm;
    return (0);

err:
    WT_TRET(bm->close(bm, session));
    return (ret);
}
