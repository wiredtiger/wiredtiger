/*-
 * Copyright (c) 2008-2012 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * WT_CELL --
 *	Variable-length cell type.
 *
 * Pages containing variable-length data (WT_PAGE_ROW_INT, WT_PAGE_ROW_LEAF,
 * and WT_PAGE_COL_VAR page types), have cells after the page header.
 *
 * There are 4 basic cell types: keys and data (each of which has an overflow
 * form), deleted cells and off-page references.  The cell is usually followed
 * by additional data, varying by type: a key or data cell is followed by a set
 * of bytes, an address cookie follows overflow or off-page cells.
 *
 * Deleted cells are place-holders for column-store files, where entries cannot
 * be removed in order to preserve the record count.
 *
 * Here's cell use by page type:
 *
 * WT_PAGE_ROW_INT (row-store internal page):
 *	Keys and offpage-reference pairs (a WT_CELL_KEY or WT_CELL_KEY_OVFL
 * cell followed by a WT_CELL_{ADDR,ADDR_DEL,ADDR_LNO} cell).
 *
 * WT_PAGE_ROW_LEAF (row-store leaf page):
 *	Keys with optional data cells (a WT_CELL_KEY or WT_CELL_KEY_OVFL cell,
 *	optionally followed by a WT_CELL_VALUE or WT_CELL_VALUE_OVFL cell).
 *
 * Both WT_PAGE_ROW_INT and WT_PAGE_ROW_LEAF pages prefix compress keys, using
 * a single byte immediately following the cell.
 *
 * WT_PAGE_COL_INT (Column-store internal page):
 *	Off-page references (a WT_CELL_{ADDR,ADDR_LNO} cell).
 *
 * WT_PAGE_COL_VAR (Column-store leaf page storing variable-length cells):
 *	Data cells (a WT_CELL_VALUE or WT_CELL_VALUE_OVFL cell), and deleted
 * cells (a WT_CELL_DEL cell).
 *
 * Cell descriptor byte:
 *
 * Bits 1 and 2 are reserved for "short" key and data cells.  If bit 1 (but not
 * bit 2) is set, it's a short data item, less than 128 bytes in length, and the
 * other 7 bits are the length.   If bit 2 is set (but not bit 1), it's a short
 * key, less than 64 bytes in length, and the other 6 bits are the length.
 *
 * Bit 3 marks variable-length column store data with an associated run-length
 * counter or a record number: there is a uint64_t value immediately after the
 * cell description byte.
 *
 * Bits 4 and 5 are unused.
 *
 * Bits 6-8 are cell "types", there are 8, we have no unused slots.
 *
 * The 0x03 bit combination (setting both 0x01 and 0x02) is unused, but would
 * require code changes.  We can use bits 4 or 5 as single bits easily; we can
 * use bits 4 and 5 as type bits in a backward compatible way by adding bit 5
 * to the type mask and adding new types that incorporate bit 5.
 */
#define	WT_CELL_VALUE_SHORT	0x001		/* Short data */
#define	WT_CELL_KEY_SHORT	0x002		/* Short key */
#define	WT_CELL_64V		0x004		/* associated recno or RLE */

#define	WT_CELL_UNUSED_BIT4	0x008		/* Unused */
#define	WT_CELL_UNUSED_BIT5	0x010		/* Unused */

/*
 * WT_CELL_ADDR is a block location, WT_CELL_ADDR_LNO is a block location with
 * the additional information that the address is for a leaf page without any
 * overflow items.  The goal is to speed up data truncation since we don't have
 * to read leaf pages without overflow items in order to delete them.
 */
#define	WT_CELL_ADDR		(0 << 5)	/* Block location */
#define	WT_CELL_ADDR_DEL	(1 << 5)	/* Block location (deleted) */
#define	WT_CELL_ADDR_LNO	(2 << 5)	/* Block location (lno) */
#define	WT_CELL_DEL		(3 << 5)	/* Deleted value */
#define	WT_CELL_KEY		(4 << 5)	/* Key */
#define	WT_CELL_KEY_OVFL	(5 << 5)	/* Key overflow */
#define	WT_CELL_VALUE		(6 << 5)	/* Value */
#define	WT_CELL_VALUE_OVFL	(7 << 5)	/* Value overflow */
#define	WT_CELL_TYPE(v)		((v) & 7 << 5)	/* Bits 6-8 */

/*
 * WT_CELL --
 *	Variable-length, on-page cell header.
 */
struct __wt_cell {
	/*
	 * Maximum of 16 bytes:
	 * 1: type + 64V flag (recno/rle)
	 * 1: prefix compression count
	 * 9: optional RLE or recno	(uint64_t encoding, max 9 bytes)
	 * 5: optional data length	(uint32_t encoding, max 5 bytes)
	 *
	 * The prefix compression count and 64V value overlap, this calculation
	 * is pessimistic, but it isn't worth optimizing.
	 */
	uint8_t __chunk[1 + 1 + WT_INTPACK64_MAXSIZE + WT_INTPACK32_MAXSIZE];
};

/*
 * WT_CELL_UNPACK --
 *	Unpacked cell.
 */
struct __wt_cell_unpack {
	uint64_t v;			/* RLE count or recno */

	const void *data;		/* Data */
	uint32_t    size;		/* Data size */

	uint32_t len;			/* Cell + data total length */

	uint8_t  prefix;		/* Cell prefix length */

	uint8_t  raw;			/* Raw cell type (include "shorts") */
	uint8_t  type;			/* Cell type */
	uint8_t	 ovfl;			/* 1/0: cell is an overflow */
};

/*
 * WT_CELL_FOREACH --
 *	Walk the cells on a page.
 */
#define	WT_CELL_FOREACH(btree, dsk, cell, unpack, i)			\
	for ((cell) =							\
	    WT_PAGE_HEADER_BYTE(btree, dsk), (i) = (dsk)->u.entries;	\
	    (i) > 0;							\
	    (cell) = (WT_CELL *)((uint8_t *)(cell) + (unpack)->len), --(i))

/*
 * __wt_cell_pack_addr --
 *	Pack an address cell.
 */
static inline uint32_t
__wt_cell_pack_addr(
    WT_CELL *cell, u_int cell_type, uint64_t recno, uint32_t size)
{
	uint8_t *p;

	p = cell->__chunk + 1;

	if (recno == 0)				/* Cell type */
		cell->__chunk[0] = cell_type;
	else {					/* Cell type + record number */
		cell->__chunk[0] = cell_type | WT_CELL_64V;
		(void)__wt_vpack_uint(&p, 0, recno);
	}
						/* Length */
	(void)__wt_vpack_uint(&p, 0, (uint64_t)size);
	return (WT_PTRDIFF32(p, cell));
}

/*
 * __wt_cell_pack_data --
 *	Set a data item's WT_CELL contents.
 */
static inline uint32_t
__wt_cell_pack_data(WT_CELL *cell, uint64_t rle, uint32_t size)
{
	uint8_t byte, *p;

	/*
	 * Short data cells have 7-bits of length in the descriptor byte and no
	 * length bytes.
	 *
	 * Bit 0 is the WT_CELL_VALUE_SHORT flag; the other 7 bits are the size.
	 */
	if (rle == 0 && size <= 0x7f) {
		byte = (uint8_t)size;
		cell->__chunk[0] = (byte << 1) | WT_CELL_VALUE_SHORT;
		return (1);
	}

	p = cell->__chunk + 1;
	if (rle < 2)				/* Type + RLE */
		cell->__chunk[0] = WT_CELL_VALUE;
	else {
		cell->__chunk[0] = WT_CELL_VALUE | WT_CELL_64V;
		(void)__wt_vpack_uint(&p, 0, rle);
	}
						/* Length */
	(void)__wt_vpack_uint(&p, 0, (uint64_t)size);

	return (WT_PTRDIFF32(p, cell));
}

/*
 * __wt_cell_pack_del --
 *	Write a deleted value cell.
 */
static inline uint32_t
__wt_cell_pack_del(WT_CELL *cell, uint64_t rle)
{
	uint8_t *p;

	p = cell->__chunk + 1;
	if (rle < 2) {				/* Type */
		cell->__chunk[0] = WT_CELL_DEL;
		return (1);
	}
						/* Type + RLE */
	cell->__chunk[0] = WT_CELL_DEL | WT_CELL_64V;
	(void)__wt_vpack_uint(&p, 0, rle);

	return (WT_PTRDIFF32(p, cell));
}

/*
 * __wt_cell_pack_key --
 *	Set a key's WT_CELL contents.
 */
static inline uint32_t
__wt_cell_pack_key(WT_CELL *cell, uint8_t prefix, uint32_t size)
{
	uint8_t byte, *p;

	/*
	 * Short keys have 6-bits of length in the descriptor byte and no length
	 * bytes.
	 *
	 * Bit 0 is 0, bit 1 is the WT_CELL_KEY_SHORT flag; the other 6 bits are
	 * the size.
	 */
	if (size <= 0x3f) {
		byte = (uint8_t)size;
		cell->__chunk[0] = (byte << 2) | WT_CELL_KEY_SHORT;
		cell->__chunk[1] = prefix;
		return (2);
	}

	cell->__chunk[0] = WT_CELL_KEY;		/* Type */
	cell->__chunk[1] = prefix;		/* Prefix */

	p = cell->__chunk + 2;			/* Length */
	(void)__wt_vpack_uint(&p, 0, (uint64_t)size);

	return (WT_PTRDIFF32(p, cell));
}

/*
 * __wt_cell_pack_key_empty --
 *	Write an empty key cell.
 */
static inline void
__wt_cell_pack_key_empty(WT_CELL *cell)
{
	/*
	 * At the end of a row-store leaf page we have to write an empty key to
	 * act as a marker in case the last value on the page is zero-length.
	 * See the caller of this function for details.
	 */
	cell->__chunk[0] = WT_CELL_KEY;
}

/*
 * __wt_cell_pack_ovfl --
 *	Pack an overflow cell.
 */
static inline uint32_t
__wt_cell_pack_ovfl(WT_CELL *cell, uint8_t type, uint64_t rle, uint32_t size)
{
	uint8_t *p;

	p = cell->__chunk + 1;
	if (rle < 2)				/* Type + RLE */
		cell->__chunk[0] = type;
	else {
		cell->__chunk[0] = type | WT_CELL_64V;
		(void)__wt_vpack_uint(&p, 0, rle);
	}
						/* Length */
	(void)__wt_vpack_uint(&p, 0, (uint64_t)size);

	return (WT_PTRDIFF32(p, cell));
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
 * __wt_cell_type --
 *	Return the cell's type (collapsing special types).
 */
static inline u_int
__wt_cell_type(WT_CELL *cell)
{
	u_int type;

	/*
	 * NOTE: WT_CELL_VALUE_SHORT MUST BE CHECKED BEFORE WT_CELL_KEY_SHORT.
	 */
	if (cell->__chunk[0] & WT_CELL_VALUE_SHORT)
		return (WT_CELL_VALUE);
	if (cell->__chunk[0] & WT_CELL_KEY_SHORT)
		return (WT_CELL_KEY);

	type = WT_CELL_TYPE(cell->__chunk[0]);

	if (type == WT_CELL_ADDR_DEL || type == WT_CELL_ADDR_LNO)
		return (WT_CELL_ADDR);

	return (type);
}

/*
 * __wt_cell_unpack_safe --
 *	Unpack a WT_CELL into a structure during verification.
 */
static inline int
__wt_cell_unpack_safe(WT_CELL *cell, WT_CELL_UNPACK *unpack, uint8_t *end)
{
	uint64_t v;
	const uint8_t *p;

	/*
	 * The verification code specifies an end argument, a pointer to 1 past
	 * the end-of-page.  In that case, make sure we don't go past the end
	 * of the page when reading.  If an error occurs, we simply return the
	 * error code, the verification code takes care of complaining (and, in
	 * the case of salvage, it won't complain at all, it's OK to fail).
	 */
#undef	CHK
#define	CHK(p, len) do {						\
	if (end != NULL && (((uint8_t *)p) + (len)) > end)		\
		return (WT_ERROR);					\
} while (0)

	memset(unpack, 0, sizeof(*unpack));

	/*
	 * Check the cell description byte, then get the cell type.
	 *
	 * NOTE: WT_CELL_VALUE_SHORT MUST BE CHECKED BEFORE WT_CELL_KEY_SHORT.
	 */
	CHK(cell, 0);
	if (cell->__chunk[0] & WT_CELL_VALUE_SHORT) {
		unpack->type = WT_CELL_VALUE;
		unpack->raw = WT_CELL_VALUE_SHORT;
	} else if (cell->__chunk[0] & WT_CELL_KEY_SHORT) {
		unpack->type = WT_CELL_KEY;
		unpack->raw = WT_CELL_KEY_SHORT;
	} else {
		unpack->raw = WT_CELL_TYPE(cell->__chunk[0]);
		if (unpack->raw == WT_CELL_ADDR_DEL ||
		    unpack->raw == WT_CELL_ADDR_LNO)
			unpack->type = WT_CELL_ADDR;
		else
			unpack->type = unpack->raw;
	}

	/*
	 * Handle cells with neither an RLE count or data length: short key/data
	 * cells have 6- or 7-bits of data length in the descriptor byte and no
	 * RLE count or length bytes.   Off-page cells have known sizes, and no
	 * RLE count or length bytes.
	 */
	switch (unpack->raw) {
	case WT_CELL_KEY_SHORT:
		/*
		 * Check the prefix byte that follows the cell descriptor byte.
		 */
		CHK(cell, 1);
		unpack->prefix = cell->__chunk[1];

		unpack->data = cell->__chunk + 2;
		unpack->size = cell->__chunk[0] >> 2;
		unpack->len = 2 + unpack->size;
		goto done;
	case WT_CELL_VALUE_SHORT:
		/*
		 * Not reading any more memory, no further checks until the
		 * final check of the complete cell and its associated data.
		 */
		unpack->data = cell->__chunk + 1;
		unpack->size = cell->__chunk[0] >> 1;
		unpack->len = 1 + unpack->size;
		goto done;
	}

	/*
	 * The rest of the cell types optionally have an RLE count and/or a
	 * data length.
	 */
	p = (uint8_t *)cell + 1;			/* skip cell */

	if (unpack->raw == WT_CELL_KEY) {
		/*
		 * Check the prefix byte that follows the cell descriptor byte.
		 */
		++p;					/* skip prefix */
		CHK(p, 0);
		unpack->prefix = cell->__chunk[1];
	}

	if (cell->__chunk[0] & WT_CELL_64V)		/* skip RLE/recno */
		WT_RET(__wt_vunpack_uint(
		    &p, end == NULL ? 0 : (size_t)(end - p), &unpack->v));

	/* Set overflow flags. */
	switch (unpack->raw) {
	case WT_CELL_KEY_OVFL:
	case WT_CELL_VALUE_OVFL:
		unpack->ovfl = 1;
		break;
	}

	/*
	 * Deleted cells have known sizes, and no length bytes; everything else
	 * has data length bytes.
	 */
	switch (unpack->raw) {
	case WT_CELL_ADDR:
	case WT_CELL_ADDR_DEL:
	case WT_CELL_ADDR_LNO:
	case WT_CELL_KEY:
	case WT_CELL_KEY_OVFL:
	case WT_CELL_VALUE:
	case WT_CELL_VALUE_OVFL:
		WT_RET(__wt_vunpack_uint(
		    &p, end == NULL ? 0 : (size_t)(end - p), &v));
		unpack->data = p;
		unpack->size = WT_STORE_SIZE(v);
		unpack->len = WT_PTRDIFF32(p + unpack->size, cell);
		break;
	case WT_CELL_DEL:
		unpack->len = WT_PTRDIFF32(p, cell);
		break;
	default:
		return (WT_ERROR);			/* Unknown cell type. */
	}

	/*
	 * Check the original cell against the full cell length (this is a
	 * diagnostic as well, we may be copying the cell from the page and
	 * we need the right length).
	 */
done:	CHK(cell, unpack->len);
	return (0);
}

/*
 * __wt_cell_unpack --
 *	Unpack a WT_CELL into a structure.
 */
static inline void
__wt_cell_unpack(WT_CELL *cell, WT_CELL_UNPACK *unpack)
{
	(void)__wt_cell_unpack_safe(cell, unpack, NULL);
}
