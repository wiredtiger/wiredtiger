/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * WT_CELL --
 *	Variable-length, on-page cell header.
 */
struct __wt_cell {
	/*
	 * Maximum of 16 bytes:
	 * 1: cell descriptor byte
	 * 1: prefix compression count
	 * 9: associated 64-bit value	(uint64_t encoding, max 9 bytes)
	 * 5: data length		(uint32_t encoding, max 5 bytes)
	 *
	 * This calculation is pessimistic: the prefix compression count and
	 * 64V value overlap, the 64V value and data length are optional.
	 */
	uint8_t __chunk[1 + 1 + WT_INTPACK64_MAXSIZE + WT_INTPACK32_MAXSIZE];
};

/*
 * WT_CELL_UNPACK --
 *	Unpacked cell.
 */
struct __wt_cell_unpack {
	WT_CELL *cell;			/* Cell's disk image address */

	uint64_t v;			/* RLE count or recno */

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

