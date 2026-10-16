/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

static int __block_inmem_destroy(WT_SESSION_IMPL *, WT_BLOCK_INMEM *);

/*
 * __bmi_notsup --
 *     Block-manager entry points that do not apply to per-file in_memory objects.
 */
static int
__bmi_notsup(WT_BM *bm, WT_SESSION_IMPL *session)
{
    WT_UNUSED(bm);

    WT_RET_MSG(session, ENOTSUP, "in-memory file: operation not supported");
}

/*
 * __bmi_noop --
 *     Hooks that callers may invoke; succeed without doing work.
 */
static int
__bmi_noop(WT_BM *bm, WT_SESSION_IMPL *session)
{
    WT_UNUSED(bm);
    WT_UNUSED(session);

    return (0);
}

/*
 * __block_inmem_destroy --
 *     Destroy a block handle.
 */
static int
__block_inmem_destroy(WT_SESSION_IMPL *session, WT_BLOCK_INMEM *block_inmem)
{
    WT_CONNECTION_IMPL *conn;
    uint64_t bucket, hash;

    conn = S2C(session);
    hash = __wt_hash_city64(block_inmem->name, strlen(block_inmem->name));
    bucket = hash & (conn->hash_size - 1);
    WT_CONN_BLOCK_REMOVE(conn, (WT_BLOCK *)block_inmem, bucket);

    __wt_free(session, block_inmem->name);
    __wt_overwrite_and_free(session, block_inmem);

    return (0);
}

/*
 * __bmi_open_block --
 *     Open a block handle.
 */
static int
__bmi_open_block(WT_SESSION_IMPL *session, const char *filename, WT_BLOCK **blockp)
{
    WT_BLOCK *block;
    WT_BLOCK_INMEM *block_inmem;
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    uint64_t bucket, hash, tableid;

    *blockp = NULL;
    block_inmem = NULL;

    WT_ASSERT(session, filename != NULL);

    __wt_verbose(session, WT_VERB_BLOCK, "open: %s (table_id: %" PRIu64 ")", filename,
      (uint64_t)S2BT(session)->id);

    conn = S2C(session);
    tableid = S2BT(session)->id;
    hash = __wt_hash_city64(filename, strlen(filename));
    bucket = hash & (conn->hash_size - 1);
    __wt_spin_lock(session, &conn->block_lock);
    TAILQ_FOREACH (block, &conn->blockhash[bucket], hashq) {
        if (strcmp(filename, block->name) == 0 && ((WT_BLOCK_INMEM *)block)->tableid == tableid) {
            ++block->ref;
            *blockp = block;
            __wt_spin_unlock(session, &conn->block_lock);
            return (0);
        }
    }

    WT_ERR(__wt_calloc_one(session, &block_inmem));
    block_inmem->ref = 1;
    WT_CONN_BLOCK_INSERT(conn, (WT_BLOCK *)block_inmem, bucket);

    WT_ERR(__wt_strdup(session, filename, &block_inmem->name));
    block_inmem->tableid = tableid;

    *blockp = (WT_BLOCK *)block_inmem;
    __wt_spin_unlock(session, &conn->block_lock);
    return (0);

err:
    if (block_inmem != NULL)
        WT_TRET(__block_inmem_destroy(session, block_inmem));
    __wt_spin_unlock(session, &conn->block_lock);
    return (ret);
}

/*
 * __bmi_close --
 *     Close a file.
 */
static int
__bmi_close(WT_BM *bm, WT_SESSION_IMPL *session)
{
    WT_BLOCK_INMEM *block_inmem;
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;

    if (bm == NULL)
        return (0);

    block_inmem = (WT_BLOCK_INMEM *)bm->block;
    if (block_inmem == NULL) {
        __wt_overwrite_and_free(session, bm);
        return (0);
    }

    conn = S2C(session);

    __wt_verbose(session, WT_VERB_BLOCK, "close: %s (table_id: %" PRIu64 ")",
      block_inmem->name == NULL ? "" : block_inmem->name, block_inmem->tableid);

    __wt_spin_lock(session, &conn->block_lock);

    if (block_inmem->ref == 0 || --block_inmem->ref == 0)
        ret = __block_inmem_destroy(session, block_inmem);

    __wt_spin_unlock(session, &conn->block_lock);

    __wt_overwrite_and_free(session, bm);
    return (ret);
}

/*
 * __bmi_addr_invalid --
 *     Return an error code if an address cookie is invalid.
 */
static int
__bmi_addr_invalid(WT_BM *bm, WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size)
{
    WT_UNUSED(bm);

    if (addr == NULL || addr_size == 0)
        return (0);

    WT_RET_MSG(session, EINVAL, "in-memory file: invalid block address cookie");
}

/*
 * __bmi_block_header_init --
 *     Initialize the block header of a disk image laid out for writing.
 */
static void
__bmi_block_header_init(WT_BM *bm, WT_SESSION_IMPL *session, void *dsk)
{
    WT_UNUSED(bm);
    WT_UNUSED(session);

    memset(WT_BLOCK_HEADER_REF(dsk), 0, WT_BLOCK_HEADER_SIZE);
}

/*
 * __bmi_block_header_read --
 *     Return the size of the block header when reading an existing header.
 */
static u_int
__bmi_block_header_read(WT_BM *bm, WT_SESSION_IMPL *session, const void *dsk)
{
    WT_UNUSED(bm);
    WT_UNUSED(session);
    WT_UNUSED(dsk);

    return ((u_int)WT_BLOCK_HEADER_SIZE);
}

/*
 * __bmi_block_header_write --
 *     Return the size of the block header when writing a new header.
 */
static u_int
__bmi_block_header_write(WT_BM *bm, WT_SESSION_IMPL *session)
{
    WT_UNUSED(bm);
    WT_UNUSED(session);

    return ((u_int)WT_BLOCK_HEADER_SIZE);
}

/*
 * __bmi_can_truncate --
 *     Return whether there's free space at the end of the file.
 */
static bool
__bmi_can_truncate(WT_BM *bm, WT_SESSION_IMPL *session)
{
    WT_UNUSED(bm);
    WT_UNUSED(session);

    return (false);
}

/*
 * __bmi_checkpoint_load --
 *     Load a checkpoint.
 */
static int
__bmi_checkpoint_load(WT_BM *bm, WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size,
  uint8_t *root_addr, size_t *root_addr_sizep, bool checkpoint)
{
    WT_UNUSED(root_addr);

    bm->is_live = !checkpoint;
    *root_addr_sizep = 0;

    if (addr != NULL && addr_size != 0)
        WT_RET_MSG(
          session, EINVAL, "in-memory file: cannot load a checkpoint with on-disk block addresses");

    return (0);
}

/*
 * __bmi_checkpoint_resolve --
 *     Resolve a checkpoint.
 */
static int
__bmi_checkpoint_resolve(WT_BM *bm, WT_SESSION_IMPL *session, bool failed)
{
    WT_UNUSED(bm);
    WT_UNUSED(session);
    WT_UNUSED(failed);

    return (0);
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
 * __bmi_sync --
 *     Flush a file to disk.
 */
static int
__bmi_sync(WT_BM *bm, WT_SESSION_IMPL *session, bool block)
{
    WT_UNUSED(block);

    return (__bmi_noop(bm, session));
}

/*
 * __bmi_free --
 *     Free a block of space to the underlying file.
 */
static int
__bmi_free(WT_BM *bm, WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size, bool is_root)
{
    WT_UNUSED(bm);
    WT_UNUSED(session);
    WT_UNUSED(addr);
    WT_UNUSED(addr_size);
    WT_UNUSED(is_root);

    return (0);
}

/*
 * __bmi_is_mapped --
 *     Return if the file is mapped into memory.
 */
static bool
__bmi_is_mapped(WT_BM *bm, WT_SESSION_IMPL *session)
{
    WT_UNUSED(bm);
    WT_UNUSED(session);

    return (false);
}

/*
 * __bmi_size --
 *     Return the size of a live block handle.
 */
static int
__bmi_size(WT_BM *bm, WT_SESSION_IMPL *session, wt_off_t *sizep)
{
    WT_UNUSED(bm);
    WT_UNUSED(session);

    *sizep = 0;
    return (0);
}

/*
 * __bmi_stat --
 *     Block-manager statistics.
 */
static int
__bmi_stat(WT_BM *bm, WT_SESSION_IMPL *session, WT_DSRC_STATS *stats)
{
    WT_UNUSED(bm);
    WT_UNUSED(session);
    WT_UNUSED(stats);

    return (0);
}

/*
 * __bmi_write_size --
 *     Return the buffer size required to write a block.
 */
static int
__bmi_write_size(WT_BM *bm, WT_SESSION_IMPL *session, size_t *sizep)
{
    WT_BTREE *btree;

    WT_UNUSED(bm);

    btree = S2BT(session);
    *sizep = (size_t)WT_ALIGN(*sizep + WT_BLOCK_HEADER_BYTE_SIZE, btree->allocsize);
    if (*sizep > UINT32_MAX - 1024)
        WT_RET_MSG(session, EINVAL, "requested block size is too large");

    return (0);
}

/*
 * __bmi_compact_progress --
 *     Report compaction progress.
 */
static void
__bmi_compact_progress(WT_BM *bm, WT_SESSION_IMPL *session)
{
    WT_UNUSED(bm);
    WT_UNUSED(session);
}

/*
 * __bmi_read --
 *     Read a buffer from a block.
 */
static int
__bmi_read(WT_BM *bm, WT_SESSION_IMPL *session, WT_ITEM *buf, WT_PAGE_BLOCK_META *block_meta,
  const uint8_t *addr, size_t addr_size)
{
    WT_UNUSED(buf);
    WT_UNUSED(block_meta);
    WT_UNUSED(addr);
    WT_UNUSED(addr_size);

    return (__bmi_notsup(bm, session));
}

/*
 * __bmi_write --
 *     Write a buffer into a block.
 */
static int
__bmi_write(WT_BM *bm, WT_SESSION_IMPL *session, WT_ITEM *buf, WT_PAGE_BLOCK_META *block_meta,
  size_t page_image_size, uint8_t *addr, size_t *addr_sizep, bool data_checksum, bool checkpoint_io)
{
    WT_UNUSED(buf);
    WT_UNUSED(block_meta);
    WT_UNUSED(page_image_size);
    WT_UNUSED(addr);
    WT_UNUSED(addr_sizep);
    WT_UNUSED(data_checksum);
    WT_UNUSED(checkpoint_io);

    return (__bmi_notsup(bm, session));
}

/*
 * __bmi_checkpoint --
 *     Write a checkpoint.
 */
static int
__bmi_checkpoint(WT_BM *bm, WT_SESSION_IMPL *session, WT_ITEM *buf, WT_PAGE_BLOCK_META *block_meta,
  WT_CKPT *ckptbase, bool data_checksum)
{
    WT_UNUSED(buf);
    WT_UNUSED(block_meta);
    WT_UNUSED(ckptbase);
    WT_UNUSED(data_checksum);

    return (__bmi_notsup(bm, session));
}

/*
 * __bmi_checkpoint_last --
 *     Return the last checkpoint information.
 */
static int
__bmi_checkpoint_last(WT_BM *bm, WT_SESSION_IMPL *session, char **metadatap,
  char **checkpoint_listp, WT_ITEM *checkpoint)
{
    WT_UNUSED(metadatap);
    WT_UNUSED(checkpoint_listp);
    WT_UNUSED(checkpoint);

    return (__bmi_notsup(bm, session));
}

/*
 * __bmi_addr_string --
 *     Return a printable string representation of an address cookie.
 */
static int
__bmi_addr_string(
  WT_BM *bm, WT_SESSION_IMPL *session, WT_ITEM *buf, const uint8_t *addr, size_t addr_size)
{
    WT_UNUSED(buf);
    WT_UNUSED(addr);
    WT_UNUSED(addr_size);

    return (__bmi_notsup(bm, session));
}

/*
 * __bmi_map_discard --
 *     Discard a mapped segment.
 */
static int
__bmi_map_discard(WT_BM *bm, WT_SESSION_IMPL *session, void *map, size_t len)
{
    WT_UNUSED(map);
    WT_UNUSED(len);

    return (__bmi_notsup(bm, session));
}

/*
 * __bmi_salvage_next --
 *     Return the next block from the file.
 */
static int
__bmi_salvage_next(
  WT_BM *bm, WT_SESSION_IMPL *session, uint8_t *addr, size_t *addr_sizep, bool *eofp)
{
    WT_UNUSED(addr);
    WT_UNUSED(addr_sizep);
    WT_UNUSED(eofp);

    return (__bmi_notsup(bm, session));
}

/*
 * __bmi_salvage_valid --
 *     Inform salvage a block is valid.
 */
static int
__bmi_salvage_valid(
  WT_BM *bm, WT_SESSION_IMPL *session, uint8_t *addr, size_t addr_size, bool valid)
{
    WT_UNUSED(addr);
    WT_UNUSED(addr_size);
    WT_UNUSED(valid);

    return (__bmi_notsup(bm, session));
}

/*
 * __bmi_verify_end --
 *     End a block manager verify.
 */
static int
__bmi_verify_end(WT_BM *bm, WT_SESSION_IMPL *session, bool verify_success)
{
    WT_UNUSED(verify_success);

    return (__bmi_notsup(bm, session));
}

/*
 * __bmi_verify_start --
 *     Start a block manager verify.
 */
static int
__bmi_verify_start(WT_BM *bm, WT_SESSION_IMPL *session, WT_CKPT *ckptbase, const char *cfg[])
{
    WT_UNUSED(ckptbase);
    WT_UNUSED(cfg);

    return (__bmi_notsup(bm, session));
}

/*
 * __bmi_verify_addr --
 *     Verify an address.
 */
static int
__bmi_verify_addr(WT_BM *bm, WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size)
{
    WT_UNUSED(addr);
    WT_UNUSED(addr_size);

    return (__bmi_notsup(bm, session));
}

/*
 * __bmi_compact_page_skip --
 *     Return if a page is useful for compaction.
 */
static int
__bmi_compact_page_skip(
  WT_BM *bm, WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size, bool *skipp)
{
    WT_UNUSED(addr);
    WT_UNUSED(addr_size);
    WT_UNUSED(skipp);

    return (__bmi_notsup(bm, session));
}

/*
 * __bmi_compact_skip --
 *     Return if a file can be compacted.
 */
static int
__bmi_compact_skip(WT_BM *bm, WT_SESSION_IMPL *session, bool *skipp)
{
    WT_UNUSED(skipp);

    return (__bmi_notsup(bm, session));
}

/*
 * __bmi_compact_page_rewrite --
 *     Rewrite a page for compaction.
 */
static int
__bmi_compact_page_rewrite(
  WT_BM *bm, WT_SESSION_IMPL *session, uint8_t *addr, size_t *addr_sizep, bool *writtenp)
{
    WT_UNUSED(addr);
    WT_UNUSED(addr_sizep);
    WT_UNUSED(writtenp);

    return (__bmi_notsup(bm, session));
}

/*
 * __bmi_corrupt --
 *     Report a corrupt block.
 */
static int
__bmi_corrupt(WT_BM *bm, WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size)
{
    WT_UNUSED(addr);
    WT_UNUSED(addr_size);

    return (__bmi_notsup(bm, session));
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
    bm->addr_string = __bmi_addr_string;
    bm->block_header_init = __bmi_block_header_init;
    bm->block_header_read_size = __bmi_block_header_read;
    bm->block_header_write_size = __bmi_block_header_write;
    bm->can_truncate = __bmi_can_truncate;
    bm->checkpoint = __bmi_checkpoint;
    bm->checkpoint_last = __bmi_checkpoint_last;
    bm->checkpoint_load = __bmi_checkpoint_load;
    bm->checkpoint_resolve = __bmi_checkpoint_resolve;
    bm->checkpoint_start = __bmi_noop;
    bm->checkpoint_unload = __bmi_noop;
    bm->close = __bmi_close;
    bm->compact_end = __bmi_noop;
    bm->compact_page_rewrite = __bmi_compact_page_rewrite;
    bm->compact_page_skip = __bmi_compact_page_skip;
    bm->compact_progress = __bmi_compact_progress;
    bm->compact_skip = __bmi_compact_skip;
    bm->compact_start = __bmi_noop;
    bm->corrupt = __bmi_corrupt;
    bm->encrypt_skip = __bmi_encrypt_skip_size;
    bm->free = __bmi_free;
    bm->is_mapped = __bmi_is_mapped;
    bm->map_discard = __bmi_map_discard;
    bm->read = __bmi_read;
    bm->salvage_end = __bmi_noop;
    bm->salvage_next = __bmi_salvage_next;
    bm->salvage_start = __bmi_noop;
    bm->salvage_valid = __bmi_salvage_valid;
    bm->size = __bmi_size;
    bm->stat = __bmi_stat;
    bm->sync = __bmi_sync;
    bm->verify_addr = __bmi_verify_addr;
    bm->verify_end = __bmi_verify_end;
    bm->verify_start = __bmi_verify_start;
    bm->write = __bmi_write;
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
     * Connection-wide in_memory uses the default block manager with the in-memory file system; only
     * per-file in_memory=true selects this block manager.
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

    WT_UNUSED(cfg);
    WT_UNUSED(forced_salvage);
    WT_UNUSED(readonly);

    *bmp = NULL;

    WT_RET(__wt_calloc_one(session, &bm));
    bm->is_live = true;

    __bmi_method_set(bm, readonly);

    uri += strlen("file:");

    WT_ERR(__bmi_open_block(session, uri, &bm->block));

    WT_ASSERT_ALWAYS(session, ((WT_BLOCK_INMEM *)bm->block)->tableid == S2BT(session)->id,
      "block manager bound to a block handle for a different table");

    *bmp = bm;
    return (0);

err:
    WT_TRET(bm->close(bm, session));
    return (ret);
}
