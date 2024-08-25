/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __bmp_checkpoint_pack_raw --
 *     This function needs to do two things: Create a recovery point in the object store underlying
 *     this table and create an address cookie that is saved to the metadata (and used to find the
 *     checkpoint again).
 */
static int
__bmp_checkpoint_pack_raw(
  WT_BLOCK_PANTRY *block_pantry, WT_SESSION_IMPL *session, WT_ITEM *root_image, WT_CKPT *ckpt)
{
    uint64_t pantry_id;
    uint32_t checksum, size;
    uint8_t *endp;

    /*
     * !!!
     * Our caller wants the final checkpoint size. Setting the size here violates layering,
     * but the alternative is a call for the btree layer to crack the checkpoint cookie into
     * its components, and that's a fair amount of work.
     */
    ckpt->size = __wt_atomic_loadv64(&block_pantry->next_pantry_id);

    /* Copy the checkpoint information into the checkpoint. */
    WT_RET(__wt_buf_init(session, &ckpt->raw, WT_BLOCK_CHECKPOINT_BUFFER));
    endp = ckpt->raw.mem;

    /* Write the root page out, and get back the address information for that page
     * which will be written into the block manager checkpoint cookie.
     */
    WT_RET(__wt_block_pantry_write_internal(
      session, block_pantry, root_image, &pantry_id, &size, &checksum, true, true));

    WT_RET(__wt_block_pantry_ckpt_pack(block_pantry, &endp, pantry_id, size, checksum));
    ckpt->raw.size = WT_PTRDIFF(endp, ckpt->raw.mem);

    return (0);
}

/*
 * __wt_bmp_checkpoint --
 *     This function needs to do three things: Create a recovery point in the object store
 *     underlying this table and create an address cookie that is saved to the metadata (and used to
 *     find the checkpoint again) and save the content of the binary data added as a root page that
 *     can be retrieved to start finding content for the tree.
 */
int
__wt_bmp_checkpoint(
  WT_BM *bm, WT_SESSION_IMPL *session, WT_ITEM *root_image, WT_CKPT *ckptbase, bool data_checksum)
{
    WT_BLOCK_PANTRY *block_pantry;
    WT_CKPT *ckpt;
    WT_FILE_HANDLE *handle;

    WT_UNUSED(data_checksum);

    block_pantry = (WT_BLOCK_PANTRY *)bm->block;
    handle = block_pantry->fh->handle;

    /*
     * Generate a checkpoint cookie used to find the checkpoint again (and distinguish it from a
     * fake checkpoint).
     */
    WT_CKPT_FOREACH (ckptbase, ckpt)
        if (F_ISSET(ckpt, WT_CKPT_ADD)) {
            /* __wt_bmp_write_page(block_pantry, buf, root_addr); */
            WT_RET(__bmp_checkpoint_pack_raw(block_pantry, session, root_image, ckpt));
        }

    WT_RET(handle->fh_obj_checkpoint(handle, &session->iface, root_image));

    return (0);
}

/*
 * __wt_bmp_checkpoint_load --
 *     Load a checkpoint. This involves (1) cracking the checkpoint cookie open (2) loading the root
 *     page from the object store, (3) re-packing the root page's address cookie into root_addr.
 */
int
__wt_bmp_checkpoint_load(WT_BM *bm, WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size,
  uint8_t *root_addr, size_t *root_addr_sizep, bool checkpoint)
{
    WT_BLOCK_PANTRY *block_pantry;
    WT_FILE_HANDLE *handle;
    unsigned i;
    uint64_t root_id;
    uint32_t root_size, root_checksum;
    uint8_t *endp;

    WT_UNUSED(session);
    WT_UNUSED(addr_size);
    WT_UNUSED(checkpoint);

    block_pantry = (WT_BLOCK_PANTRY *)bm->block;
    handle = block_pantry->fh->handle;

    *root_addr_sizep = 0;

    if (addr == NULL || addr_size == 0)
        return (0);

    WT_RET(__wt_block_pantry_ckpt_unpack(block_pantry, addr, &root_id, &root_size, &root_checksum));

    /* TODO I think we don't need this because the caller should call btree_open with the cookie we
     * put back into root_addr */
    /* WT_RET(__wt_block_pantry_read_internal(session, block_pantry, root_id, root_sz, root_image,
     * root_checksum)); */

    WT_RET(handle->fh_obj_checkpoint_load(handle, &session->iface, NULL, 0));

    /*
     * Pretend there is a root page for this checkpoint - at the moment we don't actually read from
     * a checkpoint when using the block pantry.
     */
    endp = root_addr;
    WT_RET(__wt_block_pantry_addr_pack(&endp, root_id, root_size, root_checksum));
    *root_addr_sizep = WT_PTRDIFF(endp, root_addr);

    fprintf(stderr, "__wt_bmp_checkpoint_load(%s): 0x", block_pantry->fh->handle->name);
    for (i = 0; i < *root_addr_sizep; i++) {
        fprintf(stderr, "%02x", root_addr[i]);
    }
    fprintf(stderr, "\n");

    return (0);
}
