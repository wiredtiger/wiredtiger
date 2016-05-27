/*-
 * Copyright (c) 2014-2016 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"
#include <gperftools/malloc_extension_c.h>

int
__wt_cache_tcmalloc_overhead(WT_SESSION_IMPL *session)
{
	WT_CACHE *cache;
	size_t allocated, heap_size, unmapped;
	u_int overhead_pct;

	if (!MallocExtension_GetNumericProperty(
	    "generic.current_allocated_bytes", &allocated) ||
	    !MallocExtension_GetNumericProperty(
	    "generic.heap_size", &heap_size) ||
	    !MallocExtension_GetNumericProperty(
	    "tcmalloc.pageheap_unmapped_bytes", &unmapped))
		return (0);

	cache = S2C(session)->cache;
	overhead_pct = 100 - (u_int)(100 * allocated / (heap_size - unmapped));

	/* Keep the calculated overhead bounded. */
	overhead_pct = WT_MAX(5, WT_MIN(overhead_pct, 70));

	/* Don't "change" to the same value. */
	if (cache->overhead_pct == overhead_pct)
		return (0);
	cache->overhead_pct = overhead_pct;

	return (__wt_verbose(session, WT_VERB_EVICT,
	    "Set tcmalloc overhead_pct to %u\n", overhead_pct));
}
