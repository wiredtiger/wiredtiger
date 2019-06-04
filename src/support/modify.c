/*-
 * Copyright (c) 2014-2019 MongoDB, Inc.
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
	uint8_t *data;
	int i;

	/*
	 * Build the in-memory modify value. It's the entries count, followed
	 * by the modify structure offsets written in order, followed by the
	 * data (data at the end to minimize unaligned reads/writes).
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
__modify_apply_one(
    WT_SESSION_IMPL *session, WT_CURSOR *cursor, WT_MODIFY *modify)
{
	WT_ITEM *value;
	size_t data_size, len, offset, size;
	const uint8_t *data;
	uint8_t *from, *to;
	bool sformat;

	value = &cursor->value;
	sformat = cursor->value_format[0] == 'S';

	offset = modify->offset;
	size = modify->size;
	data = modify->data.data;
	data_size = modify->data.size;

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

	/*
	 * Fast-path the expected case, where we're overwriting a set of bytes
	 * that already exist in the buffer.
	 */
	if (value->size > offset + data_size && data_size == size) {
		memmove((uint8_t *)value->data + offset, data, data_size);
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
		if (value->size < offset)
			memset((uint8_t *)value->data + value->size,
			    sformat ? ' ' : 0, offset - value->size);
		memmove((uint8_t *)value->data + offset, data, data_size);
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

	if (data_size == size) {			/* Overwrite */
		/* Copy in the new data. */
		memmove((uint8_t *)value->data + offset, data, data_size);

		/*
		 * The new data must overlap the buffer's end (else, we'd use
		 * the fast-path code above). Set the buffer size to include
		 * the new data.
		 */
		value->size = offset + data_size;
	} else {					/* Shrink or grow */
		/* Move trailing data forward/backward to its new location. */
		from = (uint8_t *)value->data + (offset + size);
		WT_ASSERT(session, WT_DATA_IN_ITEM(value) &&
		    from + (value->size - (offset + size)) <=
		    (uint8_t *)value->mem + value->memsize);
		to = (uint8_t *)value->data + (offset + data_size);
		WT_ASSERT(session, WT_DATA_IN_ITEM(value) &&
		    to + (value->size - (offset + size)) <=
		    (uint8_t *)value->mem + value->memsize);
		memmove(to, from, value->size - (offset + size));

		/* Copy in the new data. */
		memmove((uint8_t *)value->data + offset, data, data_size);

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
 * __modify_apply_no_overlap --
 *	Apply a single set of WT_MODIFY changes to a buffer, where the changes
 * are in sorted order and none of the changes overlap.
 */
static int
__modify_apply_no_overlap(WT_SESSION_IMPL *session,
    WT_CURSOR *cursor, WT_MODIFY *entries, int nentries, bool *successp)
{
	WT_DECL_ITEM(tmp);
	WT_DECL_RET;
	WT_ITEM *value;
	size_t n, vsize;
	const uint8_t *from;
	uint8_t *to;
	int i;
	bool sformat;

	*successp = false;

	value = &cursor->value;
	sformat = cursor->value_format[0] == 'S';

	/*
	 * If the modifications are sorted and don't overlap, we need a second
	 * buffer but we can do the modifications in a single pass.
	 *
	 * The requirement for ordering is unfortunate, but modifications are
	 * performed "in order", and applications specify byte offsets based on
	 * that. (In other words, byte offsets are cumulative, modifications
	 * that shrink or grow the data affect subsequent modification's byte
	 * offsets.) Byte offsets become hard if we re-order modifications.
	 */
	for (vsize = value->size, i = 0; i < nentries; ++i) {
		/* Check if the entries are sorted and non-overlapping. */
		if (i + 1 < nentries &&
		    (entries[i + 1].offset <
		    entries[i].offset + entries[i].size ||
		    entries[i + 1].offset <
		    entries[i].offset + entries[i].data.size))
			return (0);

		/*
		 * If either the data size or replacement count extend past the
		 * end of the current value, we have to deal with padding bytes.
		 * Don't try to fast-path padding bytes; it's not common and
		 * adds branches to the loop applying the changes.
		 */
		if (vsize < entries[i].offset + entries[i].size ||
		    vsize < entries[i].offset + entries[i].data.size)
			return (0);
		vsize += (entries[i].data.size - entries[i].size);
	}

	/* Get a scratch buffer than can hold the result. */
	WT_RET(__wt_scr_alloc(session, vsize, &tmp));

	/*
	 * Walk the list of modifications, taking from the original buffer
	 * anywhere there are gaps. Don't bother checking for copies of 0
	 * bytes, assume it's unlikely in order to get rid of the branches.
	 */
	from = value->data;
	to = tmp->mem;
	for (i = 0; i < nentries; ++i) {
		WT_ASSERT(session,
		    entries[i].offset >= WT_PTRDIFF(to, tmp->mem));
		n = entries[i].offset - WT_PTRDIFF(to, tmp->mem);
		WT_ASSERT(session,
		    from + n <= (uint8_t *)value->data + value->size);

		memmove(to, from, n);
		to += n;
		memmove(to, entries[i].data.data, entries[i].data.size);
		to += entries[i].data.size;

		from += n + entries[i].size;
	}

	/* Copy any data remaining in the original buffer. */
	n = value->size - WT_PTRDIFF(from, value->data);
	memmove(to, from, n);
	to += n;
	tmp->size = WT_PTRDIFF(to, tmp->mem);

	/*
	 * Copy the result into the original buffer. This function is often
	 * called using a cursor buffer referencing on-page memory and it's easy
	 * to overwrite a page. A side-effect of setting the buffer is to ensure
	 * the buffer's value is in buffer-local memory.
	 */
	WT_ERR(__wt_buf_set(session, value, tmp->data, tmp->size));

	/* Restore the trailing nul. */
	if (sformat) {
		WT_ERR(__wt_buf_grow(session, value, value->size + 1));
		((char *)value->data)[value->size++] = '\0';
	}
	*successp = true;

err:	__wt_scr_free(session, &tmp);
	return (ret);

}

/*
 * __modify_apply --
 *	Apply a single set of WT_MODIFY changes to a buffer.
 */
static int
__modify_apply(WT_SESSION_IMPL *session,
    WT_CURSOR *cursor, WT_MODIFY *entries, int nentries)
{
	WT_ITEM *value;
	int i;
	bool success;

	value = &cursor->value;

	/*
	 * Grow the buffer first. This function is often called using a cursor
	 * buffer referencing on-page memory and it's easy to overwrite a page.
	 * A side-effect of growing the buffer is to ensure the buffer's value
	 * is in buffer-local memory.
	 */
	WT_RET(__wt_buf_grow(session, value, value->size));

	/*
	 * Try fast path #1.
	 * Do simple replacement in the existing buffer if the modifications are
	 * overwriting fixed-sized fields.
	 *
	 * If either the data size or replacement count extend past the end of
	 * the current value, we have to deal with padding bytes. Don't try to
	 * fast-path padding bytes; it's not common and adds branches to the
	 * loop applying the changes.
	 */
	for (i = 0; i < nentries; ++i) {
		if (entries[i].data.size != entries[i].size ||
		    value->size < entries[i].offset + entries[i].size ||
		    value->size < entries[i].offset + entries[i].data.size)
			break;

		memmove((uint8_t *)value->data + entries[i].offset,
		   entries[i].data.data, entries[i].data.size);
	}

	/* Check for completion, else, skip any entries consumed. */
	if (i == nentries)
		return (0);
	entries += i;
	nentries -= i;

	/* Try fast path #2. */
	WT_RET(__modify_apply_no_overlap(
	    session, cursor, entries, nentries, &success));
	if (success)
		return (0);

	/* Do it the slow way. */
	for (i = 0; i < nentries; ++i)
		WT_RET(__modify_apply_one(session, cursor, entries + i));

	return (0);
}

/*
 * __wt_modify_apply_api --
 *	Apply a single set of WT_MODIFY changes to a buffer, the cursor API
 * interface.
 */
int
__wt_modify_apply_api(WT_SESSION_IMPL *session,
    WT_CURSOR *cursor, WT_MODIFY *entries, int nentries)
    WT_GCC_FUNC_ATTRIBUTE((visibility("default")))
{
	size_t modified;
	int i;

	WT_RET(__modify_apply(session, cursor, entries, nentries));

	/*
	 * This API is used by some external test functions with a NULL
	 * session pointer - they don't expect statistics to be incremented.
	 */
	if (session != NULL) {
		for (modified = 0, i = 0; i < nentries; ++i)
			modified += entries[i].size;
		WT_STAT_CONN_INCR(session, cursor_modify);
		WT_STAT_DATA_INCR(session, cursor_modify);
		WT_STAT_CONN_INCRV(session,
		    cursor_modify_bytes, cursor->value.size);
		WT_STAT_DATA_INCRV(session,
		    cursor_modify_bytes, cursor->value.size);
		WT_STAT_CONN_INCRV(session,
		    cursor_modify_bytes_touch, modified);
		WT_STAT_DATA_INCRV(session,
		    cursor_modify_bytes_touch, modified);
	}

	return (0);
}

/*
 * __wt_modify_apply --
 *	Apply a single set of WT_MODIFY changes to a buffer.
 */
int
__wt_modify_apply(
    WT_SESSION_IMPL *session, WT_CURSOR *cursor, const void *modify)
{
	WT_DECL_RET;
	WT_MODIFY *entries, _entries[30];
	size_t nentries;
	const size_t *p;
	const uint8_t *data;
	u_int i;

	entries = NULL;

	/*
	 * Get the number of modify entries and get an array to hold them. The
	 * 30 structures allocated on the stack was chosen because it's under
	 * 2KB on the stack at 50B per WT_MODIFY structure, and 30 modifications
	 * is a lot of changes for a single buffer.
	 */
	p = modify;
	memcpy(&nentries, p++, sizeof(size_t));
	if (nentries <= WT_ELEMENTS(_entries))
		entries = _entries;
	else
		WT_RET(__wt_malloc(
		    session, nentries * sizeof(WT_MODIFY), &entries));

	/*
	 * Set a second pointer to reference the change data. The modify string
	 * isn't necessarily aligned for size_t access, copy memory to be sure.
	 */
	data = (uint8_t *)modify +
	    sizeof(size_t) + (nentries * 3 * sizeof(size_t));
	for (i = 0; i < nentries; ++i) {
		memcpy(&entries[i].data.size, p++, sizeof(size_t));
		memcpy(&entries[i].offset, p++, sizeof(size_t));
		memcpy(&entries[i].size, p++, sizeof(size_t));
		entries[i].data.data = data;
		data += entries[i].data.size;
	}

	ret = __modify_apply(session, cursor, entries, nentries);

	if (entries != _entries)
		__wt_free(session, entries);

	return (ret);
}
