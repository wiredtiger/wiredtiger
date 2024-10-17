/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __bmd_checkpoint_pack_raw --
 *     This function needs to do two things: Create a recovery point in the object store underlying
 *     this table and create an address cookie that is saved to the metadata (and used to find the
 *     checkpoint again).
 */
static int
__bmd_checkpoint_pack_raw(WT_BLOCK_DISAGG *block_disagg, WT_SESSION_IMPL *session,
  WT_ITEM *root_image, WT_PAGE_BLOCK_META *block_meta, WT_CKPT *ckpt)
{
    uint64_t disagg_id;
    uint32_t checksum, size;
    uint8_t *endp;

    WT_ASSERT(session, block_meta != NULL);
    WT_ASSERT(session, block_meta->page_id != WT_BLOCK_INVALID_PAGE_ID);

    /*
     * !!!
     * Our caller wants the final checkpoint size. Setting the size here violates layering,
     * but the alternative is a call for the btree layer to crack the checkpoint cookie into
     * its components, and that's a fair amount of work.
     */
    /* ckpt->size = __wt_atomic_loadv64(&block_pantry->next_pantry_id); */
    ckpt->size = block_meta->page_id; /* XXX What should be the checkpoint size? Do we need it? */

    /* Copy the checkpoint information into the checkpoint. */
    WT_RET(__wt_buf_init(session, &ckpt->raw, WT_BLOCK_CHECKPOINT_BUFFER));
    endp = ckpt->raw.mem;

    /* Write the root page out, and get back the address information for that page
     * which will be written into the block manager checkpoint cookie.
     */
    WT_RET(__wt_block_disagg_write_internal(
      session, block_disagg, root_image, block_meta, &disagg_id, &size, &checksum, true, true));

    WT_RET(__wt_block_disagg_ckpt_pack(block_disagg, &endp, disagg_id, size, checksum));
    ckpt->raw.size = WT_PTRDIFF(endp, ckpt->raw.mem);

    return (0);
}

/*
 * __wt_block_disagg_checkpoint --
 *     This function needs to do three things: Create a recovery point in the object store
 *     underlying this table and create an address cookie that is saved to the metadata (and used to
 *     find the checkpoint again) and save the content of the binary data added as a root page that
 *     can be retrieved to start finding content for the tree.
 */
int
__wt_block_disagg_checkpoint(WT_BM *bm, WT_SESSION_IMPL *session, WT_ITEM *root_image,
  WT_PAGE_BLOCK_META *block_meta, WT_CKPT *ckptbase, bool data_checksum)
{
    WT_BLOCK_DISAGG *block_disagg;
    WT_CKPT *ckpt;

    WT_UNUSED(data_checksum);

    block_disagg = (WT_BLOCK_DISAGG *)bm->block;

    /*
     * Generate a checkpoint cookie used to find the checkpoint again (and distinguish it from a
     * fake checkpoint).
     */
    WT_CKPT_FOREACH (ckptbase, ckpt)
        if (F_ISSET(ckpt, WT_CKPT_ADD)) {
            /* __wt_bmp_write_page(block_disagg, buf, root_addr); */
            WT_RET(__bmd_checkpoint_pack_raw(block_disagg, session, root_image, block_meta, ckpt));
        }

    return (0);
}

/*
 * __wt_block_disagg_checkpoint_resolve --
 *     Resolve the checkpoint.
 */
int
__wt_block_disagg_checkpoint_resolve(WT_BM *bm, WT_SESSION_IMPL *session, bool failed)
{
    WT_BLOCK_DISAGG *block_disagg;
    WT_CONFIG_ITEM cval;
    WT_CURSOR *md_cursor;
    WT_DECL_ITEM(buf);
    WT_DECL_RET;
    size_t len;
    char *entry, *tablename;
    const char *md_value;

    block_disagg = (WT_BLOCK_DISAGG *)bm->block;

    if (failed)
        return (0);

    /* Get a metadata cursor pointing to this table */
    WT_RET(__wt_metadata_cursor(session, &md_cursor));
    /* TODO less hacky way to get URI */
    len = strlen("file:") + strlen(block_disagg->name) + 1;
    WT_RET(__wt_calloc_def(session, len, &tablename));
    WT_ERR(__wt_snprintf(tablename, len, "file:%s", block_disagg->name));
    md_cursor->set_key(md_cursor, tablename);
    WT_ERR(md_cursor->search(md_cursor));

    /* Get the config we want to print to the metadata file */
    WT_ERR(md_cursor->get_value(md_cursor, &md_value));
    WT_ERR(__wt_config_getones(session, md_value, "checkpoint", &cval));

    len += cval.len + 2; /* +2 for the separator and the newline */
    WT_ERR(__wt_calloc_def(session, len, &entry));
    WT_ERR(__wt_snprintf(entry, len, "%s\n%.*s\n", tablename, (int)cval.len, cval.str));
    /* fprintf(stderr, "[%s] writing metadata %s\n", S2C(session)->home, entry); */

    /* TODO: Add checkpoint ID. */
    WT_ERR(__wt_scr_alloc(session, len, &buf));
    memcpy(buf->mem, entry, len);
    buf->size = len - 1;
    WT_ERR(__wt_disagg_put_meta(session, 0, 0, buf));

err:
    __wt_scr_free(session, &buf);
    __wt_free(session, tablename);
    __wt_free(session, entry); /* TODO may not have been allocated */

    return (0);
}

/*
 * __wt_block_disagg_checkpoint_load --
 *     Load a checkpoint. This involves (1) cracking the checkpoint cookie open (2) loading the root
 *     page from the object store, (3) re-packing the root page's address cookie into root_addr.
 */
int
__wt_block_disagg_checkpoint_load(WT_BM *bm, WT_SESSION_IMPL *session, const uint8_t *addr,
  size_t addr_size, uint8_t *root_addr, size_t *root_addr_sizep, bool checkpoint)
{
    WT_BLOCK_DISAGG *block_disagg;
    unsigned i;
    uint64_t root_id;
    uint32_t root_size, root_checksum;
    uint8_t *endp;

    WT_UNUSED(session);
    WT_UNUSED(addr_size);
    WT_UNUSED(checkpoint);

    block_disagg = (WT_BLOCK_DISAGG *)bm->block;

    *root_addr_sizep = 0;

    if (addr == NULL || addr_size == 0)
        return (0);

    WT_RET(__wt_block_disagg_ckpt_unpack(block_disagg, addr, &root_id, &root_size, &root_checksum));

    /*
     * Read root page address.
     */
    endp = root_addr;
    WT_RET(__wt_block_disagg_addr_pack(&endp, root_id, root_size, root_checksum));
    *root_addr_sizep = WT_PTRDIFF(endp, root_addr);

    fprintf(stderr, "[%s] __wt_block_disagg_checkpoint_load(): 0x", S2C(session)->home);
    for (i = 0; i < *root_addr_sizep; i++) {
        fprintf(stderr, "%02x", root_addr[i]);
    }
    fprintf(stderr, "\n");

    return (0);
}
