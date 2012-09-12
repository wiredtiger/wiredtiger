/*-
 * Copyright (c) 2008-2012 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_cell_unpack_copy --
 *	Copy an unpacked cell into a return buffer, processing as needed.
 */
int
__wt_cell_unpack_copy(
    WT_SESSION_IMPL *session, WT_CELL_UNPACK *unpack, WT_ITEM *retb)
{
	WT_BTREE *btree;
	void *huffman;

	btree = session->btree;

	/* Get the cell's data. */
	switch (unpack->type) {
	case WT_CELL_KEY:
	case WT_CELL_VALUE:
		WT_RET(__wt_buf_set(session, retb, unpack->data, unpack->size));
		break;
	case WT_CELL_KEY_OVFL:
	case WT_CELL_VALUE_OVFL:
		WT_RET(__wt_ovfl_in(session, retb, unpack->data, unpack->size));
		break;
	WT_ILLEGAL_VALUE(session);
	}

	/* Select a Huffman encoding function. */
	switch (unpack->type) {
	case WT_CELL_KEY:
	case WT_CELL_KEY_OVFL:
		if ((huffman = btree->huffman_key) == NULL)
			return (0);
		break;
	case WT_CELL_VALUE:
	case WT_CELL_VALUE_OVFL:
	default:
		if ((huffman = btree->huffman_value) == NULL)
			return (0);
		break;
	}

	return (__wt_huffman_decode(
	    session, huffman, retb->data, retb->size, retb));
}
