/*-
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "memcmp.gnu"

/*
 * __wt_memcmp --
 *	Fast memcmp.
 */
static inline int
__wt_memcmp(const void *a, const void *b, size_t len)
{
	return (__memcmp_gnu(a, b, len));
}
