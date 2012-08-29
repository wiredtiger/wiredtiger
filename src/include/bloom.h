/*-
 * Copyright (c) 2008-2012 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */
/*
 * REFERENCES:
 *      http://www.eecs.harvard.edu/~kirsch/pubs/bbbf/esa06.pdf
 *      http://code.google.com/p/cityhash-c/
 */

struct __wt_bloom {
	const char *uri;
	char *config;
	uint8_t *bitstring;     /* For in memory representation. */
	WT_SESSION_IMPL *session;
	WT_CURSOR *c;

	uint8_t k;		/* The number of hash functions used. */
	uint8_t factor;		/* The number of bits per item inserted. */
	uint64_t m;		/* The number of slots in the bit string. */
	uint64_t n;		/* The number of items to be inserted. */
};
