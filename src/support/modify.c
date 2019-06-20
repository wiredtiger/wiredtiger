/*-
 * Copyright (c) 2014-2019 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

#define	WT_MODIFY_FOREACH_BEGIN(mod, p, nentries) do {			\
	const size_t *__p = p;						\
	const uint8_t *__data =						\
	    (const uint8_t *)(__p + (size_t)nentries * 3);		\
	int __i;							\
	for (__i = 0; __i < nentries; ++__i) {				\
		mod.data.data = __data;					\
		memcpy(&mod.data.size, __p++, sizeof(size_t));		\
		__data += mod.data.size;				\
		memcpy(&mod.offset, __p++, sizeof(size_t));		\
		memcpy(&mod.size, __p++, sizeof(size_t));

#define	WT_MODIFY_FOREACH_REVERSE(mod, p, nentries, datasz) do {	\
	const size_t *__p = p + (size_t)nentries * 3;			\
	const uint8_t *__data = (const uint8_t *)__p + datasz;		\
	int __i;							\
	for (__i = 0; __i < nentries; ++__i) {				\
		memcpy(&mod.size, --__p, sizeof(size_t));		\
		memcpy(&mod.offset, --__p, sizeof(size_t));		\
		memcpy(&mod.data.size, --__p, sizeof(size_t));		\
		mod.data.data = (__data -= mod.data.size);

#define	WT_MODIFY_FOREACH_END						\
	}								\
} while (0)

/*
 * __wt_modify_pack --
 *	Pack a modify structure into a buffer.
 */
int
__wt_modify_pack(WT_CURSOR *cursor,
    WT_ITEM **modifyp, WT_MODIFY *entries, int nentries)
{
	WT_ITEM *modify;
	WT_SESSION_IMPL *session;
	size_t diffsz, len, *p;
	uint8_t *data;
	int i;

	session = (WT_SESSION_IMPL *)cursor->session;

	/*
	 * Build the in-memory modify value. It's the entries count, followed
	 * by the modify structure offsets written in order, followed by the
	 * data (data at the end to minimize unaligned reads/writes).
	 */
	len = sizeof(size_t);                           /* nentries */
	for (i = 0, diffsz = 0; i < nentries; ++i) {
		len += 3 * sizeof(size_t);              /* WT_MODIFY fields */
		len += entries[i].data.size;            /* data */
		diffsz += entries[i].size;		/* bytes touched */
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

	/*
	 * Update statistics. This is the common path called by
	 * WT_CURSOR::modify implementations.
	 */
	WT_STAT_CONN_INCR(session, cursor_modify);
	WT_STAT_DATA_INCR(session, cursor_modify);
	WT_STAT_CONN_INCRV(session, cursor_modify_bytes, cursor->value.size);
	WT_STAT_DATA_INCRV(session, cursor_modify_bytes, cursor->value.size);
	WT_STAT_CONN_INCRV(session, cursor_modify_bytes_touch, diffsz);
	WT_STAT_DATA_INCRV(session, cursor_modify_bytes_touch, diffsz);

	return (0);
}

/*
 * __modify_apply_one --
 *	Apply a single modify structure change to the buffer.
 */
static int
__modify_apply_one(
    WT_SESSION_IMPL *session, WT_ITEM *value, WT_MODIFY *modify, bool sformat)
{
	size_t data_size, len, offset, size;
	const uint8_t *data, *from;
	uint8_t *to;

	data = modify->data.data;
	data_size = modify->data.size;
	offset = modify->offset;
	size = modify->size;

	/*
	* Grow the buffer to the maximum size we'll need. This is pessimistic
	* because it ignores replacement bytes, but it's a simpler calculation.
	*
	* Grow the buffer first. This function is often called using a cursor
	* buffer referencing on-page memory and it's easy to overwrite a page.
	* A side-effect of growing the buffer is to ensure the buffer's value
	* is in buffer-local memory.
	*
	* Because the buffer may reference an overflow item, the data may not
	* start at the start of the buffer's memory and we have to correct for
	* that.
	*/
	len = WT_DATA_IN_ITEM(value) ? WT_PTRDIFF(value->data, value->mem) : 0;
	WT_RET(__wt_buf_grow(session, value,
	    len + WT_MAX(value->size, offset) + data_size + (sformat ? 1 : 0)));
	WT_ASSERT(session, value->data == value->mem);

	/*
	 * Fast-path the common case, where we're overwriting a set of bytes
	 * that already exist in the buffer.
	 */
	if (value->size > offset + data_size && data_size == size) {
		memmove((uint8_t *)value->mem + offset, data, data_size);
		return (0);
	}

	/*
	 * Decrement the size to discard the trailing nul (done after growing
	 * the buffer to ensure it can be restored without further checking).
	 */
	if (sformat)
		--value->size;

	/*
	 * If appending bytes past the end of the value, initialize gap bytes
	 * and copy the new bytes into place.
	 */
	if (value->size <= offset) {
		WT_ASSERT(session,
		    offset + data_size + (sformat ? 1 : 0) <= value->memsize);
		if (value->size < offset)
			memset((uint8_t *)value->mem + value->size,
			    sformat ? ' ' : 0, offset - value->size);
		memmove((uint8_t *)value->mem + offset, data, data_size);
		value->size = offset + data_size;

		/* Restore the trailing nul. */
		if (sformat)
			((char *)value->data)[value->size++] = '\0';
		return (0);
	}

	/*
	 * Correct the replacement size if it's nonsense, we can't replace more
	 * bytes than remain in the value. (Nonsense sizes are permitted in the
	 * API because we don't want to handle the errors.)
	 */
	if (value->size < offset + size)
		size = value->size - offset;

	WT_ASSERT(session, value->size + (data_size - size) +
	    (sformat ? 1 : 0) <= value->memsize);

	if (data_size == size) {			/* Overwrite */
		/* Copy in the new data. */
		memmove((uint8_t *)value->mem + offset, data, data_size);

		/*
		 * The new data must overlap the buffer's end (else, we'd use
		 * the fast-path code above). Set the buffer size to include
		 * the new data.
		 */
		value->size = offset + data_size;
	} else {					/* Shrink or grow */
		/* Move trailing data forward/backward to its new location. */
		from = (const uint8_t *)value->data + (offset + size);
		WT_ASSERT(session, WT_DATA_IN_ITEM(value) &&
		    from + (value->size - (offset + size)) <=
		    (uint8_t *)value->mem + value->memsize);
		to = (uint8_t *)value->mem + (offset + data_size);
		WT_ASSERT(session, WT_DATA_IN_ITEM(value) &&
		    to + (value->size - (offset + size)) <=
		    (uint8_t *)value->mem + value->memsize);
		memmove(to, from, value->size - (offset + size));

		/* Copy in the new data. */
		memmove((uint8_t *)value->mem + offset, data, data_size);

		/*
		 * Correct the size. This works because of how the C standard
		 * defines unsigned arithmetic, and gcc7 complains about more
		 * verbose forms:
		 *
		 *	if (data_size > size)
		 *		value->size += (data_size - size);
		 *	else
		 *		value->size -= (size - data_size);
		 *
		 * because the branches are identical.
		 */
		 value->size += (data_size - size);
	}

	/* Restore the trailing nul. */
	if (sformat)
		((char *)value->data)[value->size++] = '\0';

	return (0);
}

/*
 * __modify_check_fast_path --
 *	Process a set of modifications, check if the fast path is possible.
 */
static bool
__modify_check_fast_path(
    WT_ITEM *value, const size_t *p, int nentries,
    size_t *dataszp, size_t *destszp)
{
	WT_MODIFY current, prev;
	size_t datasz, destoff;
	bool first;

	datasz = destoff = 0;
	WT_CLEAR(current);

	/*
	 * If the modifications are sorted and don't overlap in the old or new
	 * values, we can do a fast application of all the modifications
	 * modifications in a single pass.
	 *
	 * The requirement for ordering is unfortunate, but modifications are
	 * performed in order, and applications specify byte offsets based on
	 * that. In other words, byte offsets are cumulative, modifications
	 * that shrink or grow the data affect subsequent modification's byte
	 * offsets.
	 */
	first = true;
	WT_MODIFY_FOREACH_BEGIN(current, p, nentries) {
		datasz += current.data.size;

		/* Step over the current unmodified block. */
		if (first)
			destoff = current.offset;
		else {
			/* Check that entries are sorted and non-overlapping. */
			if (current.offset < prev.offset + prev.size ||
			    current.offset < prev.offset + prev.data.size)
				return (false);
			destoff += current.offset - (prev.offset + prev.size);
		}

		/*
		 * If the source is past the end of the current value, we have
		 * to deal with padding bytes.  Don't try to fast-path padding
		 * bytes; it's not common and adds branches to the loop
		 * applying the changes.
		 */
		if (current.offset + current.size > value->size)
			return (false);

		/*
		 * If copying this block overlaps with the next one, the fast
		 * path in reverse order will fail.
		 */
		if (current.size != current.data.size &&
		    current.offset + current.size > destoff)
			return (false);

		/* Step over the current modification. */
		destoff += current.data.size;

		prev = current;
		first = false;
	} WT_MODIFY_FOREACH_END;

	/* Step over the final unmodified block. */
	destoff += value->size - (current.offset + current.size);

	*dataszp = datasz;
	*destszp = destoff;
	return (true);
}

/*
 * __modify_apply_no_overlap --
 *	Apply a single set of WT_MODIFY changes to a buffer, where the changes
 *	are in sorted order and none of the changes overlap.
 */
static int
__modify_apply_no_overlap(WT_SESSION_IMPL *session,
    WT_ITEM *value, const size_t *p, int nentries, size_t datasz, size_t destsz)
{
	WT_MODIFY current;
	size_t sz;
	const uint8_t *from;
	uint8_t *to;
	int delta;

	/*
	 * Grow the buffer first. This function is often called using a cursor
	 * buffer referencing on-page memory and it's easy to overwrite a page.
	 * A side-effect of growing the buffer is to ensure the buffer's value
	 * is in buffer-local memory.
	 */
	WT_RET(__wt_buf_grow(session, value, WT_MAX(destsz, value->size)));

	delta = 0;
	WT_MODIFY_FOREACH_BEGIN(current, p, nentries) {
		delta += (int)current.data.size - (int)current.size;
	} WT_MODIFY_FOREACH_END;

	from = (const uint8_t *)value->data + value->size;
	to = (uint8_t *)value->mem + destsz;
	WT_MODIFY_FOREACH_REVERSE(current, p, nentries, datasz) {
		/* Move the current unmodified block into place if necessary. */
		sz = WT_PTRDIFF(to, value->mem) -
		    (current.offset + current.data.size);
		from -= sz;
		to -= sz;
		WT_ASSERT(session, from >= (const uint8_t *)value->data &&
		    to >= (uint8_t *)value->mem);
		WT_ASSERT(session,
		    from + sz <= (const uint8_t *)value->data + value->size);

		if (to != from)
			memmove(to, from, sz);

		from -= current.size;
		to -= current.data.size;
		memmove(to, current.data.data, current.data.size);
	} WT_MODIFY_FOREACH_END;

	value->size = destsz;
	return (0);
}

/*
 * __wt_modify_apply_api --
 *	Apply a single set of WT_MODIFY changes to a buffer, the cursor API
 * interface.
 */
int
__wt_modify_apply_api(WT_CURSOR *cursor, WT_MODIFY *entries, int nentries)
    WT_GCC_FUNC_ATTRIBUTE((visibility("default")))
{
	WT_DECL_ITEM(modify);
	WT_DECL_RET;

	WT_ERR(__wt_modify_pack(cursor, &modify, entries, nentries));
	WT_ERR(__wt_modify_apply(cursor, modify->data));

err:	__wt_scr_free((WT_SESSION_IMPL *)cursor->session, &modify);
	return (ret);
}

/*
 * __wt_modify_apply --
 *	Apply a single set of WT_MODIFY changes to a buffer.
 */
int
__wt_modify_apply(WT_CURSOR *cursor, const void *modify)
{
	WT_ITEM *value;
	WT_MODIFY mod;
	WT_SESSION_IMPL *session;
	size_t datasz, destsz, tmp;
	const size_t *p;
	int nentries;
	bool fast_path, sformat;

	session = (WT_SESSION_IMPL *)cursor->session;
	sformat = cursor->value_format[0] == 'S';
	value = &cursor->value;

	/*
	 * Get the number of modify entries and set a second pointer to
	 * reference the replacement data.
	 */
	p = modify;
	memcpy(&tmp, p++, sizeof(size_t));
	nentries = (int)tmp;

	fast_path = __modify_check_fast_path(
	    value, p, nentries, &datasz, &destsz);

	if (fast_path && !sformat)
		return (__modify_apply_no_overlap(
		    session, value, p, nentries, datasz, destsz));

	WT_MODIFY_FOREACH_BEGIN(mod, p, nentries) {
		WT_RET(__modify_apply_one(session, value, &mod, sformat));
	} WT_MODIFY_FOREACH_END;

	return (0);
}
