/*-
 * Copyright (c) 2008-2012 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

static int __err_cell_corrupted(WT_SESSION_IMPL *, uint32_t, const char *);
static int __err_cell_type(
	WT_SESSION_IMPL *, uint32_t, const char *, uint8_t, uint8_t);
static int __err_eof(WT_SESSION_IMPL *, uint32_t, const char *);
static int __verify_dsk_chunk(
	WT_SESSION_IMPL *, const char *, WT_PAGE_HEADER *, uint32_t);
static int __verify_dsk_col_fix(
	WT_SESSION_IMPL *, const char *, WT_PAGE_HEADER *);
static int __verify_dsk_col_int(
	WT_SESSION_IMPL *, const char *, WT_PAGE_HEADER *);
static int __verify_dsk_col_var(
	WT_SESSION_IMPL *, const char *, WT_PAGE_HEADER *);
static int __verify_dsk_row(
	WT_SESSION_IMPL *, const char *, WT_PAGE_HEADER *);

#define	WT_ERR_VRFY(session, ...) do {					\
	if (!(F_ISSET(session, WT_SESSION_SALVAGE_QUIET_ERR)))		\
		__wt_errx(session, __VA_ARGS__);			\
	goto err;							\
} while (0)

#define	WT_RET_VRFY(session, ...) do {					\
	if (!(F_ISSET(session, WT_SESSION_SALVAGE_QUIET_ERR)))		\
		__wt_errx(session, __VA_ARGS__);			\
	return (WT_ERROR);						\
} while (0)

/*
 * __wt_verify_dsk --
 *	Verify a single Btree page as read from disk.
 */
int
__wt_verify_dsk(WT_SESSION_IMPL *session, const char *addr, WT_ITEM *buf)
{
	WT_PAGE_HEADER *dsk;
	uint32_t size;
	uint8_t *p;
	u_int i;

	dsk = buf->mem;
	size = buf->size;

	/* Check the page type. */
	switch (dsk->type) {
	case WT_PAGE_BLOCK_MANAGER:
	case WT_PAGE_COL_FIX:
	case WT_PAGE_COL_INT:
	case WT_PAGE_COL_VAR:
	case WT_PAGE_OVFL:
	case WT_PAGE_ROW_INT:
	case WT_PAGE_ROW_LEAF:
		break;
	case WT_PAGE_INVALID:
	default:
		WT_RET_VRFY(session,
		    "page at %s has an invalid type of %" PRIu32,
		    addr, dsk->type);
	}

	/* Check the page record number. */
	switch (dsk->type) {
	case WT_PAGE_COL_FIX:
	case WT_PAGE_COL_INT:
	case WT_PAGE_COL_VAR:
		if (dsk->recno != 0)
			break;
		WT_RET_VRFY(session,
		    "%s page at %s has a record number of zero",
		    __wt_page_type_string(dsk->type), addr);
	case WT_PAGE_BLOCK_MANAGER:
	case WT_PAGE_OVFL:
	case WT_PAGE_ROW_INT:
	case WT_PAGE_ROW_LEAF:
		if (dsk->recno == 0)
			break;
		WT_RET_VRFY(session,
		    "%s page at %s has a non-zero record number",
		    __wt_page_type_string(dsk->type), addr);
	}

	/* Check the in-memory size. */
	if (dsk->size != size)
		WT_RET_VRFY(session,
		    "%s page at %s has an incorrect size (%" PRIu32 " != %"
		    PRIu32 ")",
		    __wt_page_type_string(dsk->type), addr, dsk->size, size);

	/* Unused bytes */
	for (p = dsk->unused, i = sizeof(dsk->unused); i > 0; --i)
		if (*p != '\0')
			WT_RET_VRFY(session,
			    "page at %s has non-zero unused page header bytes",
			    addr);

	/* Verify the items on the page. */
	switch (dsk->type) {
	case WT_PAGE_COL_INT:
		return (__verify_dsk_col_int(session, addr, dsk));
	case WT_PAGE_COL_FIX:
		return (__verify_dsk_col_fix(session, addr, dsk));
	case WT_PAGE_COL_VAR:
		return (__verify_dsk_col_var(session, addr, dsk));
	case WT_PAGE_ROW_INT:
	case WT_PAGE_ROW_LEAF:
		return (__verify_dsk_row(session, addr, dsk));
	case WT_PAGE_BLOCK_MANAGER:
	case WT_PAGE_OVFL:
		return (__verify_dsk_chunk(session, addr, dsk, dsk->u.datalen));
	WT_ILLEGAL_VALUE(session);
	}
	/* NOTREACHED */
}

/*
 * __verify_dsk_row --
 *	Walk a WT_PAGE_ROW_INT or WT_PAGE_ROW_LEAF disk page and verify it.
 */
static int
__verify_dsk_row(
    WT_SESSION_IMPL *session, const char *addr, WT_PAGE_HEADER *dsk)
{
	WT_BTREE *btree;
	WT_CELL *cell;
	WT_CELL_UNPACK *unpack, _unpack;
	WT_DECL_ITEM(current);
	WT_DECL_ITEM(last_ovfl);
	WT_DECL_ITEM(last_pfx);
	WT_DECL_RET;
	WT_ITEM *last;
	enum { FIRST, WAS_KEY, WAS_VALUE } last_cell_type;
	void *huffman;
	uint32_t cell_num, cell_type, i, prefix;
	uint8_t *end;
	int cmp;

	btree = session->btree;
	huffman = btree->huffman_key;
	unpack = &_unpack;

	WT_ERR(__wt_scr_alloc(session, 0, &current));
	WT_ERR(__wt_scr_alloc(session, 0, &last_pfx));
	WT_ERR(__wt_scr_alloc(session, 0, &last_ovfl));
	last = last_ovfl;

	end = (uint8_t *)dsk + dsk->size;

	last_cell_type = FIRST;
	cell_num = 0;
	WT_CELL_FOREACH(btree, dsk, cell, unpack, i) {
		++cell_num;

		/* Carefully unpack the cell. */
		if (__wt_cell_unpack_safe(cell, unpack, end) != 0) {
			ret = __err_cell_corrupted(session, cell_num, addr);
			goto err;
		}

		/* Check the raw and collapsed cell types. */
		WT_ERR(__err_cell_type(
		    session, cell_num, addr, unpack->raw, dsk->type));
		WT_ERR(__err_cell_type(
		    session, cell_num, addr, unpack->type, dsk->type));
		cell_type = unpack->type;

		/*
		 * Check ordering relationships between the WT_CELL entries.
		 * For row-store internal pages, check for:
		 *	two values in a row,
		 *	two keys in a row,
		 *	a value as the first cell on a page.
		 * For row-store leaf pages, check for:
		 *	two values in a row,
		 *	a value as the first cell on a page.
		 */
		switch (cell_type) {
		case WT_CELL_KEY:
		case WT_CELL_KEY_OVFL:
			switch (last_cell_type) {
			case FIRST:
			case WAS_VALUE:
				break;
			case WAS_KEY:
				if (dsk->type == WT_PAGE_ROW_LEAF)
					break;
				WT_ERR_VRFY(session,
				    "cell %" PRIu32 " on page at %s is the "
				    "first of two adjacent keys",
				    cell_num - 1, addr);
			}
			last_cell_type = WAS_KEY;
			break;
		case WT_CELL_ADDR:
		case WT_CELL_VALUE:
		case WT_CELL_VALUE_OVFL:
			switch (last_cell_type) {
			case FIRST:
				WT_ERR_VRFY(session,
				    "page at %s begins with a value", addr);
			case WAS_KEY:
				break;
			case WAS_VALUE:
				WT_ERR_VRFY(session,
				    "cell %" PRIu32 " on page at %s is the "
				    "first of two adjacent values",
				    cell_num - 1, addr);
			}
			last_cell_type = WAS_VALUE;
			break;
		}

		/* Check if any referenced item is entirely in the file. */
		switch (cell_type) {
		case WT_CELL_ADDR:
		case WT_CELL_KEY_OVFL:
		case WT_CELL_VALUE_OVFL:
			if (!__wt_bm_addr_valid(
			    session, unpack->data, unpack->size))
				goto eof;
			break;
		}

		/*
		 * Remaining checks are for key order and prefix compression.
		 * If this cell isn't a key, we're done, move to the next cell.
		 * If this cell is an overflow item, instantiate the key and
		 * compare it with the last key.   Otherwise, we have to deal
		 * with prefix compression.
		 */
		switch (cell_type) {
		case WT_CELL_KEY:
			break;
		case WT_CELL_KEY_OVFL:
			WT_ERR(__wt_cell_unpack_copy(session, unpack, current));
			goto key_compare;
		default:
			/* Not a key -- continue with the next cell. */
			continue;
		}

		/*
		 * Prefix compression checks.
		 *
		 * Confirm the first non-overflow key on a page has a zero
		 * prefix compression count.
		 */
		prefix = unpack->prefix;
		if (last_pfx->size == 0 && prefix != 0)
			WT_ERR_VRFY(session,
			    "the %" PRIu32 " key on page at %s is the first "
			    "non-overflow key on the page and has a non-zero "
			    "prefix compression value",
			    cell_num, addr);

		/* Confirm the prefix compression count is possible. */
		if (cell_num > 1 && prefix > last->size)
			WT_ERR_VRFY(session,
			    "key %" PRIu32 " on page at %s has a prefix "
			    "compression count of %" PRIu32
			    ", larger than the length of the previous key, %"
			    PRIu32,
			    cell_num, addr, prefix, last->size);

		/*
		 * If Huffman decoding required, use the heavy-weight call to
		 * __wt_cell_unpack_copy() to build the key, up to the prefix.
		 * Else, we can do it faster internally because we don't have
		 * to shuffle memory around as much.
		 */
		if (huffman == NULL) {
			/*
			 * Get the cell's data/length and make sure we have
			 * enough buffer space.
			 */
			WT_ERR(__wt_buf_grow(
			    session, current, prefix + unpack->size));

			/* Copy the prefix then the data into place. */
			if (prefix != 0)
				memcpy((void *)
				    current->data, last->data, prefix);
			memcpy((uint8_t *)
			    current->data + prefix, unpack->data, unpack->size);
			current->size = prefix + unpack->size;
		} else {
			WT_ERR(__wt_cell_unpack_copy(session, unpack, current));

			/*
			 * If there's a prefix, make sure there's enough buffer
			 * space, then shift the decoded data past the prefix
			 * and copy the prefix into place.
			 */
			if (prefix != 0) {
				WT_ERR(__wt_buf_grow(
				    session, current, prefix + current->size));
				memmove((uint8_t *)current->data +
				    prefix, current->data, current->size);
				memcpy(
				    (void *)current->data, last->data, prefix);
				current->size += prefix;
			}
		}

key_compare:	/*
		 * Compare the current key against the last key.
		 *
		 * Be careful about the 0th key on internal pages: we only store
		 * the first byte and custom collators may not be able to handle
		 * truncated keys.
		 */
		if ((dsk->type == WT_PAGE_ROW_INT && cell_num > 3) ||
		    (dsk->type != WT_PAGE_ROW_INT && cell_num > 1)) {
			WT_ERR(
			    WT_BTREE_CMP(session, btree, last, current, cmp));
			if (cmp >= 0)
				WT_ERR_VRFY(session,
				    "the %" PRIu32 " and %" PRIu32 " keys on "
				    "page at %s are incorrectly sorted",
				    cell_num - 2, cell_num, addr);
		}

		/*
		 * Swap the buffers: last always references the last key entry,
		 * last_pfx and last_ovfl reference the last prefix-compressed
		 * and last overflow key entries.  Current gets pointed to the
		 * buffer we're not using this time around, which is where the
		 * next key goes.
		 */
		last = current;
		if (cell_type == WT_CELL_KEY) {
			current = last_pfx;
			last_pfx = last;
		} else {
			current = last_ovfl;
			last_ovfl = last;
		}
		WT_ASSERT(session, last != current);
	}

	if (0) {
eof:		ret = __err_eof(session, cell_num, addr);
	}

	if (0) {
err:		if (ret == 0)
			ret = WT_ERROR;
	}
	__wt_scr_free(&current);
	__wt_scr_free(&last_pfx);
	__wt_scr_free(&last_ovfl);
	return (ret);
}

/*
 * __verify_dsk_col_int --
 *	Walk a WT_PAGE_COL_INT disk page and verify it.
 */
static int
__verify_dsk_col_int(
    WT_SESSION_IMPL *session, const char *addr, WT_PAGE_HEADER *dsk)
{
	WT_BTREE *btree;
	WT_CELL *cell;
	WT_CELL_UNPACK *unpack, _unpack;
	uint32_t cell_num, i;
	uint8_t *end;

	btree = session->btree;
	unpack = &_unpack;
	end = (uint8_t *)dsk + dsk->size;

	cell_num = 0;
	WT_CELL_FOREACH(btree, dsk, cell, unpack, i) {
		++cell_num;

		/* Carefully unpack the cell. */
		if (__wt_cell_unpack_safe(cell, unpack, end) != 0)
			return (__err_cell_corrupted(session, cell_num, addr));

		/* Check the raw and collapsed cell types. */
		WT_RET (__err_cell_type(
		    session, cell_num, addr, unpack->raw, dsk->type));
		WT_RET (__err_cell_type(
		    session, cell_num, addr, unpack->type, dsk->type));

		/* Check if any referenced item is entirely in the file. */
		if (!__wt_bm_addr_valid(session, unpack->data, unpack->size))
			return (__err_eof(session, cell_num, addr));
	}

	return (0);
}

/*
 * __verify_dsk_col_fix --
 *	Walk a WT_PAGE_COL_FIX disk page and verify it.
 */
static int
__verify_dsk_col_fix(
    WT_SESSION_IMPL *session, const char *addr, WT_PAGE_HEADER *dsk)
{
	WT_BTREE *btree;
	uint32_t datalen;

	btree = session->btree;

	datalen = __bitstr_size(btree->bitcnt * dsk->u.entries);
	return (__verify_dsk_chunk(session, addr, dsk, datalen));
}

/*
 * __verify_dsk_col_var --
 *	Walk a WT_PAGE_COL_VAR disk page and verify it.
 */
static int
__verify_dsk_col_var(
    WT_SESSION_IMPL *session, const char *addr, WT_PAGE_HEADER *dsk)
{
	WT_BTREE *btree;
	WT_CELL *cell;
	WT_CELL_UNPACK *unpack, _unpack;
	uint32_t cell_num, cell_type, i, last_size;
	int last_deleted;
	const uint8_t *last_data;
	uint8_t *end;

	btree = session->btree;
	unpack = &_unpack;
	end = (uint8_t *)dsk + dsk->size;

	last_data = NULL;
	last_size = 0;
	last_deleted = 0;

	cell_num = 0;
	WT_CELL_FOREACH(btree, dsk, cell, unpack, i) {
		++cell_num;

		/* Carefully unpack the cell. */
		if (__wt_cell_unpack_safe(cell, unpack, end) != 0)
			return (__err_cell_corrupted(session, cell_num, addr));

		/* Check the raw and collapsed cell types. */
		WT_RET (__err_cell_type(
		    session, cell_num, addr, unpack->raw, dsk->type));
		WT_RET (__err_cell_type(
		    session, cell_num, addr, unpack->type, dsk->type));
		cell_type = unpack->type;

		/* Check if any referenced item is entirely in the file.
		 */
		if (cell_type == WT_CELL_VALUE_OVFL &&
		    !__wt_bm_addr_valid(session, unpack->data, unpack->size))
			return (__err_eof(session, cell_num, addr));

		/*
		 * Compare the last two items and see if reconciliation missed
		 * a chance for RLE encoding.  We don't have to care about data
		 * encoding or anything else, a byte comparison is enough.
		 */
		if (last_deleted == 1) {
			if (cell_type == WT_CELL_DEL)
				goto match_err;
		} else
			if (cell_type == WT_CELL_VALUE &&
			    last_data != NULL &&
			    last_size == unpack->size &&
			    memcmp(last_data, unpack->data, last_size) == 0)
match_err:			WT_RET_VRFY(session,
				    "data entries %" PRIu32 " and %" PRIu32
				    " on page at %s are identical and should "
				    "have been run-length encoded",
				    cell_num - 1, cell_num, addr);

		switch (cell_type) {
		case WT_CELL_DEL:
			last_deleted = 1;
			last_data = NULL;
			break;
		case WT_CELL_VALUE_OVFL:
			last_deleted = 0;
			last_data = NULL;
			break;
		case WT_CELL_VALUE:
			last_deleted = 0;
			last_data = unpack->data;
			last_size = unpack->size;
			break;
		}
	}

	return (0);
}

/*
 * __verify_dsk_chunk --
 *	Verify a Chunk O' Data on a Btree page.
 */
static int
__verify_dsk_chunk(WT_SESSION_IMPL *session,
    const char *addr, WT_PAGE_HEADER *dsk, uint32_t datalen)
{
	WT_BTREE *btree;
	uint8_t *p, *end;

	btree = session->btree;
	end = (uint8_t *)dsk + dsk->size;

	/*
	 * Fixed-length column-store and overflow pages are simple chunks of
	 * data.
	 */
	if (datalen == 0)
		WT_RET_VRFY(session,
		    "%s page at %s has no data",
		    __wt_page_type_string(dsk->type), addr);

	/* Verify the data doesn't overflow the end of the page. */
	p = WT_PAGE_HEADER_BYTE(btree, dsk);
	if (p + datalen > end)
		WT_RET_VRFY(session,
		    "data on page at %s extends past the end of the page",
		    addr);

	/* Any bytes after the data chunk should be nul bytes. */
	for (p += datalen; p < end; ++p)
		if (*p != '\0')
			WT_RET_VRFY(session,
			    "%s page at %s has non-zero trailing bytes",
			    __wt_page_type_string(dsk->type), addr);

	return (0);
}

/*
 * __err_cell_corrupted --
 *	Generic corrupted cell, we couldn't read it.
 */
static int
__err_cell_corrupted(
    WT_SESSION_IMPL *session, uint32_t entry_num, const char *addr)
{
	WT_RET_VRFY(session,
	    "item %" PRIu32 " on page at %s is a corrupted cell",
	    entry_num, addr);
}

/*
 * __err_cell_type --
 *	Generic illegal cell type for a particular page type error.
 */
static int
__err_cell_type(WT_SESSION_IMPL *session,
    uint32_t entry_num, const char *addr, uint8_t cell_type, uint8_t dsk_type)
{
	switch (cell_type) {
	case WT_CELL_ADDR:
	case WT_CELL_ADDR_DEL:
	case WT_CELL_ADDR_LNO:
		if (dsk_type == WT_PAGE_COL_INT ||
		    dsk_type == WT_PAGE_ROW_INT)
			return (0);
		break;
	case WT_CELL_DEL:
		if (dsk_type == WT_PAGE_COL_VAR)
			return (0);
		break;
	case WT_CELL_KEY:
	case WT_CELL_KEY_OVFL:
	case WT_CELL_KEY_SHORT:
		if (dsk_type == WT_PAGE_ROW_INT ||
		    dsk_type == WT_PAGE_ROW_LEAF)
			return (0);
		break;
	case WT_CELL_VALUE:
	case WT_CELL_VALUE_COPY:
	case WT_CELL_VALUE_OVFL:
	case WT_CELL_VALUE_SHORT:
		if (dsk_type == WT_PAGE_COL_VAR ||
		    dsk_type == WT_PAGE_ROW_LEAF)
			return (0);
		break;
	case WT_CELL_VALUE_OVFL_RM:
		/*
		 * The overflow-value deleted cell is in-memory only, it's an
		 * error to ever see it on a disk page.
		 */
		break;
	default:
		break;
	}

	WT_RET_VRFY(session,
	    "illegal cell and page type combination: cell %" PRIu32
	    " on page at %s is a %s cell on a %s page",
	    entry_num, addr,
	    __wt_cell_type_string(cell_type), __wt_page_type_string(dsk_type));
}

/*
 * __err_eof --
 *	Generic item references non-existent file pages error.
 */
static int
__err_eof(WT_SESSION_IMPL *session, uint32_t entry_num, const char *addr)
{
	WT_RET_VRFY(session,
	    "off-page item %" PRIu32
	    " on page at %s references non-existent file pages",
	    entry_num, addr);
}
