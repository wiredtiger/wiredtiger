/*-
 * Copyright (c) 2014-2017 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_modify_pack --
 *	Pack a modify structure into a buffer.
 */
int
__wt_modify_pack(WT_SESSION_IMPL *session,
    WT_ITEM **modifyp, WT_MODIFY *entries, int nentries)
{
	WT_ITEM *modify;
	size_t len, *p;
	int i;
	uint8_t *data;

	/*
	 * Build the in-memory modify value. For now, it's the entries count,
	 * followed by the modify structure offsets written in order, followed
	 * by the data (data at the end to avoid unaligned reads). The format
	 * never ends up on disk so it can be changed at will.
	 */
	len = sizeof(size_t);                           /* nentries */
	for (i = 0; i < nentries; ++i) {
		len += 3 * sizeof(size_t);              /* WT_MODIFY fields */
		len += entries[i].data.size;            /* data */
	}

	WT_RET(__wt_scr_alloc(session, len, &modify));

	data = (uint8_t *)modify->mem +
	    sizeof(size_t) + ((size_t)nentries * 3 * sizeof(size_t));
	p = modify->mem;
	*p++ = (size_t)nentries;
	for (i = 0; i < nentries; ++i) {
		*p++ = entries[i].data.size;
		*p++ = entries[i].offset;
		*p++ = entries[i].size;

		memcpy(data, entries[i].data.data, entries[i].data.size);
		data += entries[i].data.size;
	}
	modify->size = WT_PTRDIFF(data, modify->data);
	*modifyp = modify;
	return (0);
}

/*
 * __modify_apply_one --
 *	Apply a single modify structure change to the buffer.
 */
static int
__modify_apply_one(WT_SESSION_IMPL *session, WT_ITEM *value,
    size_t data_size, size_t offset, size_t size, const uint8_t *data)
{
	uint8_t *p, *t;

	/*
	 * Fast-path the expected case, where we're overwriting a set of bytes
	 * that already exist in the buffer.
	 */
	if (value->size > offset + data_size && data_size == size) {
		memmove((uint8_t *)value->data + offset, data, data_size);
		return (0);
	}

	/*
	 * Grow the buffer to the maximum size we'll need. This is pessimistic
	 * because it ignores replacement bytes, but it's a simpler calculation.
	 */
	WT_RET(__wt_buf_grow(
	    session, value, WT_MAX(value->size, offset) + data_size));

	/*
	 * If appending bytes past the end of the value, initialize gap bytes
	 * and copy the new bytes into place.
	 */
	if (value->size <= offset) {
		if (value->size < offset)
			memset((uint8_t *)value->data +
			    value->size, '\0', offset - value->size);
		memmove((uint8_t *)value->data + offset, data, data_size);
		value->size = offset + data_size;
		return (0);
	}

	/*
	 * Correct the replacement size if it's nonsense, we can't replace more
	 * bytes than remain in the value. (Nonsense sizes are permitted in the
	 * API because we don't want to handle the errors.)
	 */
	if (value->size < offset + size)
		size = value->size - offset;

	if (data_size == size) {			/* Overwrite */
		/* Copy in the new data. */
		memmove((uint8_t *)value->data + offset, data, data_size);

		/*
		 * The new data must overlap the buffer's end (else, we'd use
		 * the fast-path code above). Grow the buffer size to include
		 * the new data.
		 */
		value->size = offset + data_size;
	} else {					/* Shrink or grow */
		/* Shift the current data into its new location. */
		p = (uint8_t *)value->data + offset + size;
		t = (uint8_t *)value->data + offset + data_size;
		memmove(t, p, value->size - (offset + size));

		/* Copy in the new data. */
		memmove((uint8_t *)value->data + offset, data, data_size);

		/* Fix the size. */
		if (data_size > size)
			value->size += (data_size - size);
		else
			value->size -= (size - data_size);
	}

	return (0);
}

/*
 * __wt_modify_apply --
 *	Apply a single set of WT_MODIFY changes to a buffer.
 */
int
__wt_modify_apply(WT_SESSION_IMPL *session, WT_ITEM *value, const void *modify)
{
	const size_t *p;
	int nentries;
	const uint8_t *data;

	/*
	 * Get the number of entries, and set a second pointer to reference the
	 * change data.
	 */
	p = modify;
	nentries = (int)*p++;
	data = (uint8_t *)modify +
	    sizeof(size_t) + ((size_t)nentries * 3 * sizeof(size_t));

	/* Step through the list of entries, applying them in order. */
	for (; nentries-- > 0; data += p[0], p += 3)
		WT_RET(__modify_apply_one(
		    session, value, p[0], p[1], p[2], data));

	return (0);
}
