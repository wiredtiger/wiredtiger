/*-
 * Copyright (c) 2014-2019 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * WT_CELL --
 *	Variable-length cell type.
 *
 * Pages containing variable-length keys or values data (the WT_PAGE_ROW_INT,
 * WT_PAGE_ROW_LEAF, WT_PAGE_COL_INT and WT_PAGE_COL_VAR page types), have
 * cells after the page header.
 *
 * There are 4 basic cell types: keys and data (each of which has an overflow
 * form), deleted cells and off-page references.  The cell is usually followed
 * by additional data, varying by type: keys are followed by a chunk of data,
 * data is followed by optional timestamps and a chunk of data, overflow and
 * off-page cells are followed by optional timestamps and an address cookie.
 *
 * Deleted cells are place-holders for column-store files, where entries cannot
 * be removed in order to preserve the record count.
 *
 * Here's the cell use by page type:
 *
 * WT_PAGE_ROW_INT (row-store internal page):
 *	Keys and offpage-reference pairs (a WT_CELL_KEY or WT_CELL_KEY_OVFL
 * cell followed by a WT_CELL_ADDR_XXX cell).
 *
 * WT_PAGE_ROW_LEAF (row-store leaf page):
 *	Keys with optional data cells (a WT_CELL_KEY or WT_CELL_KEY_OVFL cell,
 *	normally followed by a WT_CELL_{VALUE,VALUE_COPY,VALUE_OVFL} cell).
 *
 *	WT_PAGE_ROW_LEAF pages optionally prefix-compress keys, using a single
 *	byte count immediately following the cell.
 *
 * WT_PAGE_COL_INT (Column-store internal page):
 *	Off-page references (a WT_CELL_ADDR_XXX cell).
 *
 * WT_PAGE_COL_VAR (Column-store leaf page storing variable-length cells):
 *	Data cells (a WT_CELL_{VALUE,VALUE_COPY,VALUE_OVFL} cell), or deleted
 * cells (a WT_CELL_DEL cell).
 *
 * Each cell starts with a descriptor byte:
 *
 * Bits 1 and 2 are reserved for "short" key and value cells (that is, a cell
 * carrying data less than 64B, where we can store the data length in the cell
 * descriptor byte):
 *	0x00	Not a short key/data cell
 *	0x01	Short key cell
 *	0x10	Short key cell, with a following prefix-compression byte
 *	0x11	Short value cell
 * In the "short" variants, the other 6 bits of the descriptor byte are the
 * data length.
 *
 * Bit 3 marks an 8B packed, uint64_t value following the cell description byte.
 * (A run-length counter or a record number for variable-length column store.)
 *
 * Bit 4 marks a value with associated timestamps (globally visible values don't
 * require timestamps).
 *
 * Bits 5-8 are cell "types".
 */
#define	WT_CELL_KEY_SHORT	0x01		/* Short key */
#define	WT_CELL_KEY_SHORT_PFX	0x02		/* Short key with prefix byte */
#define	WT_CELL_VALUE_SHORT	0x03		/* Short data */
#define	WT_CELL_SHORT_TYPE(v)	((v) & 0x03U)

#define	WT_CELL_SHORT_MAX	63		/* Maximum short key/value */
#define	WT_CELL_SHORT_SHIFT	2		/* Shift for short key/value */

#define	WT_CELL_64V		0x04		/* Associated value */
#define	WT_CELL_TIMESTAMPS	0x08		/* Associated timestamps */

/*
 * WT_CELL_ADDR_INT is an internal block location, WT_CELL_ADDR_LEAF is a leaf
 * block location, and WT_CELL_ADDR_LEAF_NO is a leaf block location where the
 * page has no overflow items.  (The goal is to speed up truncation as we don't
 * have to read pages without overflow items in order to delete them.  Note,
 * WT_CELL_ADDR_LEAF_NO is not guaranteed to be set on every page without
 * overflow items, the only guarantee is that if set, the page has no overflow
 * items.)
 *
 * WT_CELL_VALUE_COPY is a reference to a previous cell on the page, supporting
 * value dictionaries: if the two values are the same, we only store them once
 * and have any second and subsequent uses reference the original.
 */
#define	WT_CELL_ADDR_DEL	 (0)		/* Address: deleted */
#define	WT_CELL_ADDR_INT	 (1 << 4)	/* Address: internal  */
#define	WT_CELL_ADDR_LEAF	 (2 << 4)	/* Address: leaf */
#define	WT_CELL_ADDR_LEAF_NO	 (3 << 4)	/* Address: leaf no overflow */
#define	WT_CELL_DEL		 (4 << 4)	/* Deleted value */
#define	WT_CELL_KEY		 (5 << 4)	/* Key */
#define	WT_CELL_KEY_OVFL	 (6 << 4)	/* Overflow key */
#define	WT_CELL_KEY_OVFL_RM	(12 << 4)	/* Overflow key (removed) */
#define	WT_CELL_KEY_PFX		 (7 << 4)	/* Key with prefix byte */
#define	WT_CELL_VALUE		 (8 << 4)	/* Value */
#define	WT_CELL_VALUE_COPY	 (9 << 4)	/* Value copy */
#define	WT_CELL_VALUE_OVFL	(10 << 4)	/* Overflow value */
#define	WT_CELL_VALUE_OVFL_RM	(11 << 4)	/* Overflow value (removed) */

#define	WT_CELL_TYPE_MASK	(0x0fU << 4)	/* Maximum 16 cell types */
#define	WT_CELL_TYPE(v)		((v) & WT_CELL_TYPE_MASK)

/*
 * When unable to create a short key or value (and where it wasn't an associated
 * RLE or timestamps that prevented creating a short value), the data must be at
 * least 64B, else we'd have used a short cell. When packing/unpacking the size,
 * decrement/increment the size, in the hopes that a smaller size will pack into
 * a single byte instead of two.
 */
#define	WT_CELL_SIZE_ADJUST	(WT_CELL_SHORT_MAX + 1)

/*
 * WT_CELL --
 *	Variable-length, on-page cell header.
 */
struct __wt_cell {
	/*
	 * Maximum of 34 bytes:
	 * 1: cell descriptor byte
	 * 1: prefix compression count
	 * 9: start timestamp		(uint64_t encoding, max 9 bytes)
	 * 9: stop timestamp		(uint64_t encoding, max 9 bytes)
	 * 9: associated 64-bit value	(uint64_t encoding, max 9 bytes)
	 * 5: data length		(uint32_t encoding, max 5 bytes)
	 *
	 * This calculation is pessimistic: the prefix compression count and
	 * 64V value overlap, the 64V value and data length are optional, and
	 * timestamps only appear in values.
	 */
	uint8_t __chunk[1 + 1 +
	    3 * WT_INTPACK64_MAXSIZE + WT_INTPACK32_MAXSIZE];
};

/*
 * WT_CELL_UNPACK --
 *	Unpacked cell.
 */
struct __wt_cell_unpack {
	WT_CELL *cell;			/* Cell's disk image address */

	uint64_t v;			/* RLE count or recno */

					/* Start/stop timestamps for a value */
	wt_timestamp_t start_ts, stop_ts;
					/* Aggregated timestamp information */
	wt_timestamp_t oldest_start_ts, newest_start_ts, newest_stop_ts;

	/*
	 * !!!
	 * The size and __len fields are reasonably type size_t; don't change
	 * the type, performance drops significantly if they're type size_t.
	 */
	const void *data;		/* Data */
	uint32_t    size;		/* Data size */

	uint32_t __len;			/* Cell + data length (usually) */

	uint8_t prefix;			/* Cell prefix length */

	uint8_t raw;			/* Raw cell type (include "shorts") */
	uint8_t type;			/* Cell type */

	uint8_t ovfl;			/* boolean: cell is an overflow */
};

/*
 * __wt_timestamp_value_check --
 *	Check an start/stop timestamp pair for sanity.
 */
static inline void
__wt_timestamp_value_check(
    WT_SESSION_IMPL *session, wt_timestamp_t start_ts, wt_timestamp_t stop_ts)
{
	WT_UNUSED(start_ts);
	WT_UNUSED(stop_ts);

	WT_ASSERT(session, stop_ts != WT_TS_NONE);
	WT_ASSERT(session, start_ts <= stop_ts);
}

/*
 * __cell_pack_timestamp_value --
 *	Pack a start, stop timestamp pair for a value.
 */
static inline void
__cell_pack_timestamp_value(WT_SESSION_IMPL *session,
    uint8_t **pp, wt_timestamp_t start_ts, wt_timestamp_t stop_ts)
{
	__wt_timestamp_value_check(session, start_ts, stop_ts);

	/*
	 * TIMESTAMP-FIXME
	 * Values (presumably) have associated transaction IDs, but we haven't
	 * yet decided how to handle them.
	 *
	 * Historic versions and globally visible values don't have associated
	 * timestamps, else set a flag bit and store the packed timestamp pair.
	 */
	if (!__wt_process.page_version_ts ||
	    (start_ts == WT_TS_NONE && stop_ts == WT_TS_MAX))
		++*pp;
	else {
		**pp |= WT_CELL_TIMESTAMPS;
		++*pp;

		/* Store differences, not absolutes. */
		(void)__wt_vpack_uint(pp, 0, start_ts);
		(void)__wt_vpack_uint(pp, 0, stop_ts - start_ts);
	}
}

/*
 * __wt_timestamp_addr_check --
 *	Check an address timestamp for sanity.
 */
static inline void
__wt_timestamp_addr_check(WT_SESSION_IMPL *session,
    wt_timestamp_t oldest_start_ts,
    wt_timestamp_t newest_start_ts, wt_timestamp_t newest_stop_ts)
{
	WT_UNUSED(oldest_start_ts);
	WT_UNUSED(newest_start_ts);
	WT_UNUSED(newest_stop_ts);

	WT_ASSERT(session, newest_stop_ts != WT_TS_NONE);
	WT_ASSERT(session, oldest_start_ts <= newest_start_ts);
	WT_ASSERT(session, newest_start_ts <= newest_stop_ts);
}

/*
 * __cell_pack_timestamp_addr --
 *	Pack a oldest_start, newest_start, newest_stop timestamp triplet for an
 * address.
 */
static inline void
__cell_pack_timestamp_addr(WT_SESSION_IMPL *session,
    uint8_t **pp, wt_timestamp_t oldest_start_ts,
    wt_timestamp_t newest_start_ts, wt_timestamp_t newest_stop_ts)
{
	__wt_timestamp_addr_check(session,
	    oldest_start_ts, newest_start_ts, newest_stop_ts);

	++*pp;
	if (__wt_process.page_version_ts) {
		/* Store differences, not absolutes. */
		(void)__wt_vpack_uint(pp, 0, oldest_start_ts);
		(void)__wt_vpack_uint(pp, 0, newest_start_ts - oldest_start_ts);
		(void)__wt_vpack_uint(pp, 0, newest_stop_ts - newest_start_ts);
	}
}

/*
 * __wt_cell_pack_addr --
 *	Pack an address cell.
 */
static inline size_t
__wt_cell_pack_addr(WT_SESSION_IMPL *session,
    WT_CELL *cell, u_int cell_type, uint64_t recno,
    wt_timestamp_t oldest_start_ts,
    wt_timestamp_t newest_start_ts, wt_timestamp_t newest_stop_ts, size_t size)
{
	uint8_t *p;

	/* Start building a cell: the descriptor byte starts zero. */
	p = cell->__chunk;
	*p = '\0';

	__cell_pack_timestamp_addr(session,
	    &p, oldest_start_ts, newest_start_ts, newest_stop_ts);

	if (recno == WT_RECNO_OOB)
		cell->__chunk[0] = (uint8_t)cell_type;	/* Type */
	else {
		cell->__chunk[0] = (uint8_t)(cell_type | WT_CELL_64V);
		(void)__wt_vpack_uint(&p, 0, recno);	/* Record number */
	}
	(void)__wt_vpack_uint(&p, 0, (uint64_t)size);	/* Length */
	return (WT_PTRDIFF(p, cell));
}

/*
 * __wt_cell_pack_value --
 *	Set a value item's WT_CELL contents.
 */
static inline size_t
__wt_cell_pack_value(WT_SESSION_IMPL *session, WT_CELL *cell,
    wt_timestamp_t start_ts, wt_timestamp_t stop_ts, uint64_t rle, size_t size)
{
	uint8_t byte, *p;
	bool ts;

	/* Start building a cell: the descriptor byte starts zero. */
	p = cell->__chunk;
	*p = '\0';

	__cell_pack_timestamp_value(session, &p, start_ts, stop_ts);

	/*
	 * Short data cells without timestamps or run-length encoding have 6
	 * bits of data length in the descriptor byte.
	 */
	ts = (cell->__chunk[0] & WT_CELL_TIMESTAMPS) != 0;
	if (!ts && rle < 2 && size <= WT_CELL_SHORT_MAX) {
		byte = (uint8_t)size;			/* Type + length */
		cell->__chunk[0] = (uint8_t)
		    ((byte << WT_CELL_SHORT_SHIFT) | WT_CELL_VALUE_SHORT);
	} else {
		/*
		 * If the size was what prevented us from using a short cell,
		 * it's larger than the adjustment size. Decrement/increment
		 * it when packing/unpacking so it takes up less room.
		 */
		if (!ts && rle < 2) {
			size -= WT_CELL_SIZE_ADJUST;
			cell->__chunk[0] |= WT_CELL_VALUE;	/* Type */
		} else {
			cell->__chunk[0] |= WT_CELL_VALUE | WT_CELL_64V;
			(void)__wt_vpack_uint(&p, 0, rle);	/* RLE */
		}
		(void)__wt_vpack_uint(&p, 0, (uint64_t)size);	/* Length */
	}
	return (WT_PTRDIFF(p, cell));
}

/*
 * __wt_cell_pack_value_match --
 *	Return if two value items would have identical WT_CELLs (except for
 * timestamps and any RLE).
 */
static inline int
__wt_cell_pack_value_match(WT_CELL *page_cell,
    WT_CELL *val_cell, const uint8_t *val_data, bool *matchp)
{
	uint64_t alen, blen, v;
	const uint8_t *a, *b;
	bool rle, ts;

	*matchp = false;			/* Default to no-match */

	/*
	 * This is a special-purpose function used by reconciliation to support
	 * dictionary lookups.  We're passed an on-page cell and a created cell
	 * plus a chunk of data we're about to write on the page, and we return
	 * if they would match on the page. Ignore timestamps and column-store
	 * RLE because the copied cell will have its own.
	 */
	a = (uint8_t *)page_cell;
	b = (uint8_t *)val_cell;

	if (WT_CELL_SHORT_TYPE(a[0]) == WT_CELL_VALUE_SHORT) {
		alen = a[0] >> WT_CELL_SHORT_SHIFT;
		++a;
	} else if (WT_CELL_TYPE(a[0]) == WT_CELL_VALUE) {
		rle = (a[0] & WT_CELL_64V) != 0;
		ts = (a[0] & WT_CELL_TIMESTAMPS) != 0;
		++a;
		if (ts) {
			WT_RET(__wt_vunpack_uint(&a, 0, &v));	/* Skip TS */
			WT_RET(__wt_vunpack_uint(&a, 0, &v));
		}
		if (rle)					/* Skip RLE */
			WT_RET(__wt_vunpack_uint(&a, 0, &v));
		WT_RET(__wt_vunpack_uint(&a, 0, &alen));	/* Length */
	} else
		return (0);

	if (WT_CELL_SHORT_TYPE(b[0]) == WT_CELL_VALUE_SHORT) {
		blen = b[0] >> WT_CELL_SHORT_SHIFT;
		++b;
	} else if (WT_CELL_TYPE(b[0]) == WT_CELL_VALUE) {
		rle = (b[0] & WT_CELL_64V) != 0;
		ts = (b[0] & WT_CELL_TIMESTAMPS) != 0;
		++b;
		if (ts) {
			WT_RET(__wt_vunpack_uint(&b, 0, &v));	/* Skip TS */
			WT_RET(__wt_vunpack_uint(&b, 0, &v));
		}
		if (rle)					/* Skip RLE */
			WT_RET(__wt_vunpack_uint(&b, 0, &v));
		WT_RET(__wt_vunpack_uint(&b, 0, &blen));	/* Length */
	} else
		return (0);

	if (alen == blen)
		*matchp = memcmp(a, val_data, alen) == 0;
	return (0);
}

/*
 * __wt_cell_pack_copy --
 *	Write a copy value cell.
 */
static inline size_t
__wt_cell_pack_copy(WT_SESSION_IMPL *session, WT_CELL *cell,
    wt_timestamp_t start_ts, wt_timestamp_t stop_ts, uint64_t rle, uint64_t v)
{
	uint8_t *p;

	/* Start building a cell: the descriptor byte starts zero. */
	p = cell->__chunk;
	*p = '\0';

	__cell_pack_timestamp_value(session, &p, start_ts, stop_ts);

	if (rle < 2)
		cell->__chunk[0] |= WT_CELL_VALUE_COPY;	/* Type */
	else {
		cell->__chunk[0] |=			/* Type */
		    WT_CELL_VALUE_COPY | WT_CELL_64V;
		(void)__wt_vpack_uint(&p, 0, rle);	/* RLE */
	}
	(void)__wt_vpack_uint(&p, 0, v);		/* Copy offset */
	return (WT_PTRDIFF(p, cell));
}

/*
 * __wt_cell_pack_del --
 *	Write a deleted value cell.
 */
static inline size_t
__wt_cell_pack_del(WT_SESSION_IMPL *session, WT_CELL *cell,
    wt_timestamp_t start_ts, wt_timestamp_t stop_ts, uint64_t rle)
{
	uint8_t *p;

	/* Start building a cell: the descriptor byte starts zero. */
	p = cell->__chunk;
	*p = '\0';

	__cell_pack_timestamp_value(session, &p, start_ts, stop_ts);

	if (rle < 2)
		cell->__chunk[0] |= WT_CELL_DEL;	/* Type */
	else {
		cell->__chunk[0] |=			/* Type */
		    WT_CELL_DEL | WT_CELL_64V;
		(void)__wt_vpack_uint(&p, 0, rle);	/* RLE */
	}
	return (WT_PTRDIFF(p, cell));
}

/*
 * __wt_cell_pack_int_key --
 *	Set a row-store internal page key's WT_CELL contents.
 */
static inline size_t
__wt_cell_pack_int_key(WT_CELL *cell, size_t size)
{
	uint8_t byte, *p;

	/* Short keys have 6 bits of data length in the descriptor byte. */
	if (size <= WT_CELL_SHORT_MAX) {
		byte = (uint8_t)size;
		cell->__chunk[0] = (uint8_t)
		    ((byte << WT_CELL_SHORT_SHIFT) | WT_CELL_KEY_SHORT);
		return (1);
	}

	cell->__chunk[0] = WT_CELL_KEY;			/* Type */
	p = cell->__chunk + 1;

	/*
	 * If the size prevented us from using a short cell, it's larger than
	 * the adjustment size. Decrement/increment it when packing/unpacking
	 * so it takes up less room.
	 */
	size -= WT_CELL_SIZE_ADJUST;
	(void)__wt_vpack_uint(&p, 0, (uint64_t)size);	/* Length */
	return (WT_PTRDIFF(p, cell));
}

/*
 * __wt_cell_pack_leaf_key --
 *	Set a row-store leaf page key's WT_CELL contents.
 */
static inline size_t
__wt_cell_pack_leaf_key(WT_CELL *cell, uint8_t prefix, size_t size)
{
	uint8_t byte, *p;

	/* Short keys have 6 bits of data length in the descriptor byte. */
	if (size <= WT_CELL_SHORT_MAX) {
		if (prefix == 0) {
			byte = (uint8_t)size;		/* Type + length */
			cell->__chunk[0] = (uint8_t)
			    ((byte << WT_CELL_SHORT_SHIFT) | WT_CELL_KEY_SHORT);
			return (1);
		}
		byte = (uint8_t)size;			/* Type + length */
		cell->__chunk[0] = (uint8_t)
		    ((byte << WT_CELL_SHORT_SHIFT) | WT_CELL_KEY_SHORT_PFX);
		cell->__chunk[1] = prefix;		/* Prefix */
		return (2);
	}

	if (prefix == 0) {
		cell->__chunk[0] = WT_CELL_KEY;		/* Type */
		p = cell->__chunk + 1;
	} else {
		cell->__chunk[0] = WT_CELL_KEY_PFX;	/* Type */
		cell->__chunk[1] = prefix;		/* Prefix */
		p = cell->__chunk + 2;
	}

	/*
	 * If the size prevented us from using a short cell, it's larger than
	 * the adjustment size. Decrement/increment it when packing/unpacking
	 * so it takes up less room.
	 */
	size -= WT_CELL_SIZE_ADJUST;
	(void)__wt_vpack_uint(&p, 0, (uint64_t)size);	/* Length */
	return (WT_PTRDIFF(p, cell));
}

/*
 * __wt_cell_pack_ovfl --
 *	Pack an overflow cell.
 */
static inline size_t
__wt_cell_pack_ovfl(WT_SESSION_IMPL *session, WT_CELL *cell, uint8_t type,
    wt_timestamp_t start_ts, wt_timestamp_t stop_ts, uint64_t rle, size_t size)
{
	uint8_t *p;

	/* Start building a cell: the descriptor byte starts zero. */
	p = cell->__chunk;
	*p = '\0';

	switch (type) {
	case WT_CELL_KEY_OVFL:
	case WT_CELL_KEY_OVFL_RM:
		++p;
		break;
	case WT_CELL_VALUE_OVFL:
	case WT_CELL_VALUE_OVFL_RM:
		__cell_pack_timestamp_value(session, &p, start_ts, stop_ts);
		break;
	}

	if (rle < 2)
		cell->__chunk[0] |= type;		/* Type */
	else {
		cell->__chunk[0] |= type | WT_CELL_64V;	/* Type */
		(void)__wt_vpack_uint(&p, 0, rle);	/* RLE */
	}
	(void)__wt_vpack_uint(&p, 0, (uint64_t)size);	/* Length */
	return (WT_PTRDIFF(p, cell));
}

/*
 * __wt_cell_rle --
 *	Return the cell's RLE value.
 */
static inline uint64_t
__wt_cell_rle(WT_CELL_UNPACK *unpack)
{
	/*
	 * Any item with only 1 occurrence is stored with an RLE of 0, that is,
	 * without any RLE at all.  This code is a single place to handle that
	 * correction, for simplicity.
	 */
	return (unpack->v < 2 ? 1 : unpack->v);
}

/*
 * __wt_cell_total_len --
 *	Return the cell's total length, including data.
 */
static inline size_t
__wt_cell_total_len(WT_CELL_UNPACK *unpack)
{
	/*
	 * The length field is specially named because it's dangerous to use it:
	 * it represents the length of the current cell (normally used for the
	 * loop that walks through cells on the page), but occasionally we want
	 * to copy a cell directly from the page, and what we need is the cell's
	 * total length. The problem is dictionary-copy cells, because in that
	 * case, the __len field is the length of the current cell, not the cell
	 * for which we're returning data.  To use the __len field, you must be
	 * sure you're not looking at a copy cell.
	 */
	return (unpack->__len);
}

/*
 * __wt_cell_type --
 *	Return the cell's type (collapsing special types).
 */
static inline u_int
__wt_cell_type(WT_CELL *cell)
{
	u_int type;

	switch (WT_CELL_SHORT_TYPE(cell->__chunk[0])) {
	case WT_CELL_KEY_SHORT:
	case WT_CELL_KEY_SHORT_PFX:
		return (WT_CELL_KEY);
	case WT_CELL_VALUE_SHORT:
		return (WT_CELL_VALUE);
	}

	switch (type = WT_CELL_TYPE(cell->__chunk[0])) {
	case WT_CELL_KEY_PFX:
		return (WT_CELL_KEY);
	case WT_CELL_KEY_OVFL_RM:
		return (WT_CELL_KEY_OVFL);
	case WT_CELL_VALUE_OVFL_RM:
		return (WT_CELL_VALUE_OVFL);
	}
	return (type);
}

/*
 * __wt_cell_type_raw --
 *	Return the cell's type.
 */
static inline u_int
__wt_cell_type_raw(WT_CELL *cell)
{
	return (WT_CELL_SHORT_TYPE(cell->__chunk[0]) == 0 ?
	    WT_CELL_TYPE(cell->__chunk[0]) :
	    WT_CELL_SHORT_TYPE(cell->__chunk[0]));
}

/*
 * __wt_cell_type_reset --
 *	Reset the cell's type.
 */
static inline void
__wt_cell_type_reset(
    WT_SESSION_IMPL *session, WT_CELL *cell, u_int old_type, u_int new_type)
{
	/*
	 * For all current callers of this function, this should happen once
	 * and only once, assert we're setting what we think we're setting.
	 */
	WT_ASSERT(session, old_type == 0 || old_type == __wt_cell_type(cell));
	WT_UNUSED(old_type);

	cell->__chunk[0] =
	    (cell->__chunk[0] & ~WT_CELL_TYPE_MASK) | WT_CELL_TYPE(new_type);
}

/*
 * __wt_cell_leaf_value_parse --
 *	Return the cell if it's a row-store leaf page value, otherwise return
 * NULL.
 */
static inline WT_CELL *
__wt_cell_leaf_value_parse(WT_PAGE *page, WT_CELL *cell)
{
	/*
	 * This function exists so there's a place for this comment.
	 *
	 * Row-store leaf pages may have a single data cell between each key, or
	 * keys may be adjacent (when the data cell is empty).
	 *
	 * One special case: if the last key on a page is a key without a value,
	 * don't walk off the end of the page: the size of the underlying disk
	 * image is exact, which means the end of the last cell on the page plus
	 * the length of the cell should be the byte immediately after the page
	 * disk image.
	 *
	 * !!!
	 * This line of code is really a call to __wt_off_page, but we know the
	 * cell we're given will either be on the page or past the end of page,
	 * so it's a simpler check.  (I wouldn't bother, but the real problem is
	 * we can't call __wt_off_page directly, it's in btree.i which requires
	 * this file be included first.)
	 */
	if (cell >= (WT_CELL *)((uint8_t *)page->dsk + page->dsk->mem_size))
		return (NULL);

	switch (__wt_cell_type_raw(cell)) {
	case WT_CELL_KEY:
	case WT_CELL_KEY_OVFL:
	case WT_CELL_KEY_OVFL_RM:
	case WT_CELL_KEY_PFX:
	case WT_CELL_KEY_SHORT:
	case WT_CELL_KEY_SHORT_PFX:
		return (NULL);
	default:
		return (cell);
	}
}

/*
 * __wt_cell_unpack_safe --
 *	Unpack a WT_CELL into a structure, with optional boundary checks.
 */
static inline int
__wt_cell_unpack_safe(WT_SESSION_IMPL *session, const WT_PAGE_HEADER *dsk,
    WT_CELL *cell, WT_CELL_UNPACK *unpack, const void *end)
{
	struct {
		uint64_t v;
		wt_timestamp_t start_ts, stop_ts;
		uint32_t len;
	} copy;
	uint64_t v;
	const uint8_t *p;

	copy.v = 0;			/* -Werror=maybe-uninitialized */
	copy.start_ts = WT_TS_NONE;
	copy.stop_ts = WT_TS_MAX;
	copy.len = 0;

	/*
	 * The verification code specifies an end argument, a pointer to 1B past
	 * the end-of-page. In which case, make sure all reads are inside the
	 * page image. If an error occurs, return an error code but don't output
	 * messages, our caller handles that.
	 */
#define	WT_CELL_LEN_CHK(t, len) do {					\
	if (end != NULL &&						\
	    ((uint8_t *)(t) < (uint8_t *)dsk ||				\
	    (((uint8_t *)(t)) + (len)) > (uint8_t *)end))		\
		return (WT_ERROR);	        			\
} while (0)

	/*
	 * NB: when unpacking a WT_CELL_VALUE_COPY cell, unpack.cell is returned
	 * as the original cell, not the copied cell (in other words, data from
	 * the copied cell must be available from unpack after we return, as our
	 * caller has no way to find the copied cell).
	 */
	WT_CELL_LEN_CHK(cell, 0);
	unpack->cell = cell;

restart:
	/*
	 * This path is performance critical for read-only trees, we're parsing
	 * on-page structures. For that reason we don't clear the unpacked cell
	 * structure (although that would be simpler), instead we make sure we
	 * initialize all structure elements either here or in the immediately
	 * following switch. All default timestamps default to durability.
	 */
	unpack->v = 0;
	unpack->start_ts = WT_TS_NONE;
	unpack->stop_ts = WT_TS_MAX;
	unpack->oldest_start_ts = unpack->newest_start_ts = WT_TS_NONE;
	unpack->newest_stop_ts = WT_TS_MAX;
	unpack->raw = (uint8_t)__wt_cell_type_raw(cell);
	unpack->type = (uint8_t)__wt_cell_type(cell);
	unpack->ovfl = 0;

	/*
	 * Handle cells with neither RLE counts, timestamps or a data length:
	 * short key/data cells have 6 bits of data length in the descriptor
	 * byte and nothing else.
	 */
	switch (unpack->raw) {
	case WT_CELL_KEY_SHORT_PFX:
		WT_CELL_LEN_CHK(cell, 1);		/* skip prefix */
		unpack->prefix = cell->__chunk[1];
		unpack->data = cell->__chunk + 2;
		unpack->size = cell->__chunk[0] >> WT_CELL_SHORT_SHIFT;
		unpack->__len = 2 + unpack->size;
		goto done;
	case WT_CELL_KEY_SHORT:
	case WT_CELL_VALUE_SHORT:
		unpack->prefix = 0;
		unpack->data = cell->__chunk + 1;
		unpack->size = cell->__chunk[0] >> WT_CELL_SHORT_SHIFT;
		unpack->__len = 1 + unpack->size;
		goto done;
	}

	unpack->prefix = 0;
	unpack->data = NULL;
	unpack->size = 0;
	unpack->__len = 0;

	p = (uint8_t *)cell + 1;			/* skip cell */

	/*
	 * Check for a prefix byte that optionally follows the cell descriptor
	 * byte in keys on row-store leaf pages.
	 */
	if (unpack->raw == WT_CELL_KEY_PFX) {
		unpack->prefix = *p++;			/* skip prefix */
		WT_CELL_LEN_CHK(p, 0);
	}

	/* Check for timestamps. */
	switch (unpack->raw) {
	case WT_CELL_ADDR_DEL:
	case WT_CELL_ADDR_INT:
	case WT_CELL_ADDR_LEAF:
	case WT_CELL_ADDR_LEAF_NO:
		if (!__wt_process.page_version_ts)
			break;

		WT_RET(__wt_vunpack_uint(&p, end == NULL ? 0 :
		    WT_PTRDIFF(end, p), &unpack->oldest_start_ts));
		WT_RET(__wt_vunpack_uint(&p, end == NULL ? 0 :
		    WT_PTRDIFF(end, p), &unpack->newest_start_ts));
		unpack->newest_start_ts += unpack->oldest_start_ts;
		WT_RET(__wt_vunpack_uint(&p, end == NULL ? 0 :
		    WT_PTRDIFF(end, p), &unpack->newest_stop_ts));
		unpack->newest_stop_ts += unpack->newest_start_ts;

		__wt_timestamp_addr_check(session,
		    unpack->oldest_start_ts,
		    unpack->newest_start_ts, unpack->newest_stop_ts);
		break;
	case WT_CELL_DEL:
	case WT_CELL_VALUE:
	case WT_CELL_VALUE_COPY:
	case WT_CELL_VALUE_OVFL:
	case WT_CELL_VALUE_OVFL_RM:
		if ((cell->__chunk[0] & WT_CELL_TIMESTAMPS) == 0)
			break;

		WT_RET(__wt_vunpack_uint(&p, end == NULL ?
		    0 : WT_PTRDIFF(end, p), &unpack->start_ts));
		WT_RET(__wt_vunpack_uint(&p, end == NULL ?
		    0 : WT_PTRDIFF(end, p), &unpack->stop_ts));
		unpack->stop_ts += unpack->start_ts;

		__wt_timestamp_value_check(
		    session, unpack->start_ts, unpack->stop_ts);
		break;
	}

	/*
	 * Check for an RLE count or record number that optionally follows the
	 * cell descriptor byte on column-store variable-length pages.
	 */
	if (cell->__chunk[0] & WT_CELL_64V)		/* skip value */
		WT_RET(__wt_vunpack_uint(
		    &p, end == NULL ? 0 : WT_PTRDIFF(end, p), &unpack->v));

	/*
	 * Handle special actions for a few different cell types and set the
	 * data length (deleted cells are fixed-size without length bytes,
	 * almost everything else has data length bytes).
	 */
	switch (unpack->raw) {
	case WT_CELL_VALUE_COPY:
		/*
		 * The cell is followed by an offset to a cell written earlier
		 * in the page.  Save/restore the length and RLE of this cell,
		 * we need the length to step through the set of cells on the
		 * page and this RLE is probably different from the RLE of the
		 * earlier cell.
		 */
		WT_RET(__wt_vunpack_uint(
		    &p, end == NULL ? 0 : WT_PTRDIFF(end, p), &v));
		copy.v = unpack->v;
		copy.start_ts = unpack->start_ts;
		copy.stop_ts = unpack->stop_ts;
		copy.len = WT_PTRDIFF32(p, cell);
		cell = (WT_CELL *)((uint8_t *)cell - v);
		goto restart;

	case WT_CELL_KEY_OVFL:
	case WT_CELL_KEY_OVFL_RM:
	case WT_CELL_VALUE_OVFL:
	case WT_CELL_VALUE_OVFL_RM:
		/*
		 * Set overflow flag.
		 */
		unpack->ovfl = 1;
		/* FALLTHROUGH */

	case WT_CELL_ADDR_DEL:
	case WT_CELL_ADDR_INT:
	case WT_CELL_ADDR_LEAF:
	case WT_CELL_ADDR_LEAF_NO:
	case WT_CELL_KEY:
	case WT_CELL_KEY_PFX:
	case WT_CELL_VALUE:
		/*
		 * The cell is followed by a 4B data length and a chunk of
		 * data.
		 */
		WT_RET(__wt_vunpack_uint(
		    &p, end == NULL ? 0 : WT_PTRDIFF(end, p), &v));

		/*
		 * If the size was what prevented us from using a short cell,
		 * it's larger than the adjustment size. Decrement/increment
		 * it when packing/unpacking so it takes up less room.
		 */
		if (unpack->raw == WT_CELL_KEY ||
		    unpack->raw == WT_CELL_KEY_PFX ||
		    (unpack->raw == WT_CELL_VALUE &&
		    unpack->v == 0 &&
		    (cell->__chunk[0] & WT_CELL_TIMESTAMPS) == 0))
			v += WT_CELL_SIZE_ADJUST;

		unpack->data = p;
		unpack->size = (uint32_t)v;
		unpack->__len = WT_PTRDIFF32(p, cell) + unpack->size;
		break;

	case WT_CELL_DEL:
		unpack->__len = WT_PTRDIFF32(p, cell);
		break;
	default:
		return (WT_ERROR);		/* Unknown cell type. */
	}

	/*
	 * Check the original cell against the full cell length (this is a
	 * diagnostic as well, we may be copying the cell from the page and
	 * we need the right length).
	 */
done:	WT_CELL_LEN_CHK(cell, unpack->__len);
	if (copy.len != 0) {
		unpack->raw = WT_CELL_VALUE_COPY;
		unpack->v = copy.v;
		unpack->start_ts = copy.start_ts;
		unpack->stop_ts = copy.stop_ts;
		unpack->__len = copy.len;
	}

	return (0);
}

/*
 * __wt_cell_unpack_dsk --
 *	Unpack a WT_CELL into a structure.
 */
static inline void
__wt_cell_unpack_dsk(WT_SESSION_IMPL *session,
    const WT_PAGE_HEADER *dsk, WT_CELL *cell, WT_CELL_UNPACK *unpack)
{
	/*
	 * Row-store doesn't store zero-length values on pages, but this allows
	 * us to pretend.
	 */
	if (cell == NULL) {
		unpack->cell = NULL;
		unpack->v = 0;
		/*
		 * If there aren't any timestamps (which is what it will take
		 * to get to a zero-length item), the value must be stable.
		 */
		unpack->start_ts = WT_TS_NONE;
		unpack->stop_ts = WT_TS_MAX;
		/*
		 * Uninitialized timestamps, they aren't valid for value items,
		 * hopefully the compiler will notice if they are actually used
		 * somewhere.
		 *
		unpack->oldest_start_ts
		unpack->newest_start_ts
		unpack->newest_stop_ts
		 */
		unpack->data = "";
		unpack->size = 0;
		unpack->__len = 0;
		unpack->prefix = 0;
		unpack->raw = unpack->type = WT_CELL_VALUE;
		unpack->ovfl = 0;
		return;
	}

	(void)__wt_cell_unpack_safe(session, dsk, cell, unpack, NULL);
}

/*
 * __wt_cell_unpack --
 *	Unpack a WT_CELL into a structure.
 */
static inline void
__wt_cell_unpack(WT_SESSION_IMPL *session,
    WT_PAGE *page, WT_CELL *cell, WT_CELL_UNPACK *unpack)
{
	__wt_cell_unpack_dsk(session, page->dsk, cell, unpack);
}

/*
 * __cell_data_ref --
 *	Set a buffer to reference the data from an unpacked cell.
 */
static inline int
__cell_data_ref(WT_SESSION_IMPL *session,
    WT_PAGE *page, int page_type, WT_CELL_UNPACK *unpack, WT_ITEM *store)
{
	WT_BTREE *btree;
	bool decoded;
	void *huffman;

	btree = S2BT(session);

	/* Reference the cell's data, optionally decode it. */
	switch (unpack->type) {
	case WT_CELL_KEY:
		store->data = unpack->data;
		store->size = unpack->size;
		if (page_type == WT_PAGE_ROW_INT)
			return (0);

		huffman = btree->huffman_key;
		break;
	case WT_CELL_VALUE:
		store->data = unpack->data;
		store->size = unpack->size;
		huffman = btree->huffman_value;
		break;
	case WT_CELL_KEY_OVFL:
		WT_RET(__wt_ovfl_read(session, page, unpack, store, &decoded));
		if (page_type == WT_PAGE_ROW_INT || decoded)
			return (0);

		huffman = btree->huffman_key;
		break;
	case WT_CELL_VALUE_OVFL:
		WT_RET(__wt_ovfl_read(session, page, unpack, store, &decoded));
		if (decoded)
			return (0);
		huffman = btree->huffman_value;
		break;
	WT_ILLEGAL_VALUE(session, unpack->type);
	}

	return (huffman == NULL || store->size == 0 ? 0 :
	    __wt_huffman_decode(
	    session, huffman, store->data, store->size, store));
}

/*
 * __wt_dsk_cell_data_ref --
 *	Set a buffer to reference the data from an unpacked cell.
 *
 * There are two versions because of WT_CELL_VALUE_OVFL_RM type cells.  When an
 * overflow item is deleted, its backing blocks are removed; if there are still
 * running transactions that might need to see the overflow item, we cache a
 * copy of the item and reset the item's cell to WT_CELL_VALUE_OVFL_RM.  If we
 * find a WT_CELL_VALUE_OVFL_RM cell when reading an overflow item, we use the
 * page reference to look aside into the cache.  So, calling the "dsk" version
 * of the function declares the cell cannot be of type WT_CELL_VALUE_OVFL_RM,
 * and calling the "page" version means it might be.
 */
static inline int
__wt_dsk_cell_data_ref(WT_SESSION_IMPL *session,
    int page_type, WT_CELL_UNPACK *unpack, WT_ITEM *store)
{
	WT_ASSERT(session,
	    __wt_cell_type_raw(unpack->cell) != WT_CELL_VALUE_OVFL_RM);
	return (__cell_data_ref(session, NULL, page_type, unpack, store));
}

/*
 * __wt_page_cell_data_ref --
 *	Set a buffer to reference the data from an unpacked cell.
 */
static inline int
__wt_page_cell_data_ref(WT_SESSION_IMPL *session,
    WT_PAGE *page, WT_CELL_UNPACK *unpack, WT_ITEM *store)
{
	return (__cell_data_ref(session, page, page->type, unpack, store));
}

/*
 * WT_CELL_FOREACH --
 *	Walk the cells on a page.
 */
#define	WT_CELL_FOREACH_BEGIN(session, btree, dsk, unpack, skip_ts) do {\
	uint32_t __i;							\
	uint8_t *__cell;						\
	for (__cell = WT_PAGE_HEADER_BYTE(btree, dsk),			\
	    __i = (dsk)->u.entries;					\
	    __i > 0; __cell += (unpack).__len,	--__i) {		\
		__wt_cell_unpack_dsk(					\
		    session, dsk, (WT_CELL *)__cell, &(unpack));	\
		/*							\
		 * Optionally skip unstable page entries after downgrade\
		 * to a release without page timestamps. Check for cells\
		 * with unstable timestamps when we're not writing such	\
		 * cells ourselves.					\
		 */							\
		if ((skip_ts) &&					\
		    (unpack).stop_ts != WT_TS_MAX &&			\
		    !__wt_process.page_version_ts)			\
			continue;
#define	WT_CELL_FOREACH_END						\
	} } while (0)
