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
    ckpt->size = block_meta->page_id; /* XXX What should be the checkpoint size? Do we need it? */

    /*
     * Write the root page out, and get back the address information for that page which will be
     * written into the block manager checkpoint cookie.
     *
     * TODO: we need to check with the page service team if we need to write an empty root page.
     */
    if (root_image == NULL) {
        ckpt->raw.data = NULL;
        ckpt->raw.size = 0;
    } else {
        /* Copy the checkpoint information into the checkpoint. */
        WT_RET(__wt_buf_init(session, &ckpt->raw, WT_BLOCK_CHECKPOINT_BUFFER));
        endp = ckpt->raw.mem;
        WT_RET(__wt_block_disagg_write_internal(
          session, block_disagg, root_image, block_meta, &size, &checksum, true, true));
        WT_RET(__wt_block_disagg_ckpt_pack(block_disagg, &endp, block_meta->page_id,
          block_meta->checkpoint_id, block_meta->reconciliation_id, size, checksum));
        ckpt->raw.size = WT_PTRDIFF(endp, ckpt->raw.mem);
    }

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
 * __block_disagg_update_shared_metadata --
 *     Update the shared metadata.
 */
static int
__block_disagg_update_shared_metadata(
  WT_BM *bm, WT_SESSION_IMPL *session, const char *key, const char *value)
{
    WT_CURSOR *cursor;
    WT_DECL_RET;
    const char *cfg[] = {WT_CONFIG_BASE(session, WT_SESSION_open_cursor), "overwrite", NULL};

    WT_UNUSED(bm);

    cursor = NULL;

    WT_ERR(__wt_open_cursor(session, WT_DISAGG_METADATA_URI, NULL, cfg, &cursor));
    cursor->set_key(cursor, key);
    cursor->set_value(cursor, value);
    WT_ERR(cursor->insert(cursor));

err:
    if (cursor != NULL)
        WT_TRET(cursor->close(cursor));
    return (ret);
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
    WT_CONNECTION_IMPL *conn;
    WT_CURSOR *md_cursor;
    WT_DECL_ITEM(buf);
    WT_DECL_RET;
    size_t len;
    uint64_t checkpoint_id;
    char *entry, *md_key;
    const char *md_value;

    block_disagg = (WT_BLOCK_DISAGG *)bm->block;
    conn = S2C(session);

    buf = NULL;
    entry = NULL;
    md_cursor = NULL;
    md_key = NULL;

    if (failed)
        return (0);

    /* Get the checkpoint ID. */
    WT_ACQUIRE_READ(checkpoint_id, conn->disaggregated_storage.global_checkpoint_id);

    /* Allocate a buffer for metadata keys (plus extra space to fit the longer keys below). */
    len = strlen("file:") + strlen(block_disagg->name) + 16;
    WT_ERR(__wt_calloc_def(session, len, &md_key));

    /* Get a metadata cursor pointing to this table */
    WT_ERR(__wt_metadata_cursor(session, &md_cursor));
    WT_ERR(__wt_snprintf(md_key, len, "file:%s", block_disagg->name));
    md_cursor->set_key(md_cursor, md_key);
    WT_ERR(md_cursor->search(md_cursor));
    WT_ERR(md_cursor->get_value(md_cursor, &md_value));

    /*
     * Store the metadata of regular shared tables in the shared metadata table. Store the metadata
     * of the shared metadata table in the system-level metadata (similar to the turtle file).
     */
    if (strcmp(block_disagg->name, WT_DISAGG_METADATA_FILE) == 0) {
        /* Get the config we want to print to the metadata file */
        WT_ERR(__wt_config_getones(session, md_value, "checkpoint", &cval));

        len = cval.len + 1; /* +1 for the last byte */
        WT_ERR(__wt_calloc_def(session, len, &entry));
        WT_ERR(__wt_snprintf(entry, len, "%.*s", (int)cval.len, cval.str));

        WT_ERR(__wt_scr_alloc(session, len, &buf));
        memcpy(buf->mem, entry, len);
        buf->size = len - 1;
        WT_ERR(__wt_disagg_put_meta(session, WT_DISAGG_METADATA_MAIN_PAGE_ID, checkpoint_id, buf));
    } else {
        /* Keep all metadata for regular tables. */
        WT_SAVE_DHANDLE(
          session, ret = __block_disagg_update_shared_metadata(bm, session, md_key, md_value));
        WT_ERR(ret);

        /* Check if we need to include any other metadata keys. */
        if (WT_SUFFIX_MATCH(block_disagg->name, ".wt")) {
            /* TODO: Less hacky way of finding related metadata. */

            WT_ERR(__wt_snprintf(md_key, len, "colgroup:%s", block_disagg->name));
            md_key[strlen(md_key) - 3] = '\0'; /* Remove the .wt suffix */
            md_cursor->set_key(md_cursor, md_key);
            WT_ERR(md_cursor->search(md_cursor));
            WT_ERR_NOTFOUND_OK(md_cursor->get_value(md_cursor, &md_value), true);
            if (ret == 0) {
                WT_SAVE_DHANDLE(session,
                  ret = __block_disagg_update_shared_metadata(bm, session, md_key, md_value));
                WT_ERR(ret);
            }

            WT_ERR(__wt_snprintf(md_key, len, "table:%s", block_disagg->name));
            md_key[strlen(md_key) - 3] = '\0'; /* Remove the .wt suffix */
            md_cursor->set_key(md_cursor, md_key);
            WT_ERR(md_cursor->search(md_cursor));
            WT_ERR_NOTFOUND_OK(md_cursor->get_value(md_cursor, &md_value), true);
            if (ret == 0) {
                WT_SAVE_DHANDLE(session,
                  ret = __block_disagg_update_shared_metadata(bm, session, md_key, md_value));
                WT_ERR(ret);
            }

            ret = 0; /* In case this is still set to WT_NOTFOUND from the previous step. */
        }

        /* Check if we need to include any other metadata keys for oligarch tables. */
        if (WT_SUFFIX_MATCH(block_disagg->name, ".wt_stable")) {
            /* TODO: Less hacky way of finding related metadata. */

            WT_ERR(__wt_snprintf(md_key, len, "oligarch:%s", block_disagg->name));
            md_key[strlen(md_key) - 10] = '\0'; /* Remove the .wt_stable suffix */
            md_cursor->set_key(md_cursor, md_key);
            WT_ERR_NOTFOUND_OK(md_cursor->search(md_cursor), true);
            if (ret == 0)
                WT_ERR_NOTFOUND_OK(md_cursor->get_value(md_cursor, &md_value), true);
            if (ret == 0) {
                WT_SAVE_DHANDLE(session,
                  ret = __block_disagg_update_shared_metadata(bm, session, md_key, md_value));
                WT_ERR(ret);
            }

            ret = 0; /* In case this is still set to WT_NOTFOUND from the previous step. */
        }
    }

err:
    __wt_scr_free(session, &buf);
    __wt_free(session, md_key);
    __wt_free(session, entry); /* TODO may not have been allocated */
    if (md_cursor != NULL)
        WT_TRET(__wt_metadata_cursor_release(session, &md_cursor));

    return (ret);
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
    uint64_t checkpoint_id, reconciliation_id, root_id;
    uint32_t root_size, root_checksum;
    uint8_t *endp;

    WT_UNUSED(session);
    WT_UNUSED(addr_size);
    WT_UNUSED(checkpoint);

    block_disagg = (WT_BLOCK_DISAGG *)bm->block;

    *root_addr_sizep = 0;

    if (addr == NULL || addr_size == 0)
        return (0);

    WT_RET(__wt_block_disagg_ckpt_unpack(block_disagg, addr, addr_size, &root_id, &checkpoint_id,
      &reconciliation_id, &root_size, &root_checksum));

    /*
     * Read root page address.
     */
    endp = root_addr;
    WT_RET(__wt_block_disagg_addr_pack(
      &endp, root_id, checkpoint_id, reconciliation_id, root_size, root_checksum));
    *root_addr_sizep = WT_PTRDIFF(endp, root_addr);

    fprintf(stderr, "[%s] __wt_block_disagg_checkpoint_load(): 0x", S2C(session)->home);
    for (i = 0; i < *root_addr_sizep; i++) {
        fprintf(stderr, "%02x", root_addr[i]);
    }
    fprintf(stderr, "\n");

    return (0);
}
