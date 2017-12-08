/*-
 * Copyright (c) 2014-2017 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

struct __wt_bitmap {
	uint8_t *bitstring;

	uint64_t cnt;		/* The number of bits in use. */
	uint64_t size;		/* The number of bits allocated. */
};
