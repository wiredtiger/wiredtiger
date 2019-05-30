/*-
 * Copyright (c) 2014-2019 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_import --
 *	Import a WiredTiger file into the database.
 */
int
__wt_import(WT_SESSION_IMPL *session, const char *uri, const char *source)
{
	WT_BM *bm;
	WT_CKPT *ckpt, *ckptbase;
	WT_CONFIG_ITEM v;
	WT_DECL_ITEM(a);
	WT_DECL_ITEM(b);
	WT_DECL_ITEM(checkpoint);
	WT_DECL_RET;
	WT_KEYED_ENCRYPTOR *kencryptor;
	const char *filecfg[] = {
	   WT_CONFIG_BASE(session, file_meta), NULL, NULL, NULL };
	char *fileconf, *metadata;

	ckptbase = NULL;
	fileconf = metadata = NULL;

	WT_ERR(__wt_scr_alloc(session, 0, &a));
	WT_ERR(__wt_scr_alloc(session, 0, &b));
	WT_ERR(__wt_scr_alloc(session, 0, &checkpoint));

	/*
	 * Open the file, request block manager checkpoint information.
	 * We don't know the allocation size, but 512B allows us to read
	 * the descriptor block and that's all we care about.
	 */
	WT_ERR(__wt_block_manager_open(
	    session, source, filecfg, false, true, 512, &bm));
	ret = bm->checkpoint_last(bm, session, &metadata, checkpoint);
	WT_TRET(bm->close(bm, session));
	WT_ERR(ret);

	/*
	 * The metadata may have been encrypted, in which case it's also
	 * hexadecimal encoded. The checkpoint included a boolean value
	 * set if the metadata was encrypted for easier failure diagnosis.
	 */
	WT_ERR(__wt_config_getones(
	    session, metadata, "block_metadata_encrypted", &v));
	WT_ERR(__wt_btree_config_encryptor(session, filecfg, &kencryptor));
	if ((kencryptor == NULL && v.val != 0) ||
	    (kencryptor != NULL && v.val == 0))
		WT_ERR_MSG(session, EINVAL,
		    "%s: loaded object's encryption configuration doesn't "
		    "match the database's encryption configuration",
		    source);
	WT_ERR(__wt_config_getones(session, metadata, "block_metadata", &v));
	if (kencryptor == NULL) {
		WT_ERR(__wt_buf_set(session, a, v.str, v.len));
		WT_ERR(__wt_buf_grow(session, b, a->size + 1));
		((uint8_t *)a->data)[a->size] = '\0';
	} else {
		WT_ERR(__wt_buf_set(session, a, v.str, v.len));
		WT_ERR(__wt_buf_grow(session, b, a->size));
		WT_ERR(__wt_nhex_to_raw(session, a->data, a->size, b));
		WT_ERR(__wt_decrypt(session, kencryptor->encryptor, 0, b, a));
		WT_ERR(__wt_buf_grow(session, b, a->size + 1));
		((uint8_t *)a->data)[a->size] = '\0';
	}
	filecfg[1] = a->data;
	__wt_verbose(
	    session, WT_VERB_CHECKPOINT, "load metadata: %s", filecfg[1]);

	/*
	 * Build and flatten the complete configuration string, including the
	 * returned metadata and a reference to the source file, then update
	 * the database metadata.
	 */
	WT_ERR(__wt_buf_fmt(session, b, "source=%s", source));
	filecfg[2] = b->data;
	WT_ERR(__wt_config_collapse(session, filecfg, &fileconf));
	__wt_verbose(session,
	    WT_VERB_CHECKPOINT, "load configuration: %s/%s", uri, fileconf);
	WT_ERR(__wt_metadata_insert(session, uri, fileconf));

	/*
	 * We have the checkpoint information from immediately before the final
	 * checkpoint (we just updated the file's metadata), the block manager
	 * returned the corrected final checkpoint, put it all together.
	 *
	 * Get the checkpoint information from the file's metadata as an array
	 * of WT_CKPT structures. We're going to add a new entry for the final
	 * checkpoint at the end, move to that entry.
	 */
	WT_ERR(__wt_meta_ckptlist_get(session, uri, true, &ckptbase));
	WT_CKPT_FOREACH(ckptbase, ckpt)
		if (ckpt->name == NULL)
			break;
	WT_ERR(__wt_buf_set(
	    session, &ckpt->raw, checkpoint->data, checkpoint->size));

	/* Update the file's metadata with the new checkpoint information. */
	WT_ERR(__wt_meta_ckptlist_set(session, uri, ckptbase, NULL));

err:
	__wt_meta_ckptlist_free(session, &ckptbase);

	__wt_free(session, fileconf);
	__wt_free(session, metadata);

	__wt_scr_free(session, &a);
	__wt_scr_free(session, &b);
	__wt_scr_free(session, &checkpoint);

	return (ret);
}
