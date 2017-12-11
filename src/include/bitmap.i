/*-
 * Public Domain 2014-2017 MongoDB, Inc.
 * Public Domain 2008-2014 WiredTiger, Inc.
 *
 * This is free and unencumbered software released into the public domain.
 *
 * Anyone is free to copy, modify, publish, use, compile, sell, or
 * distribute this software, either in source code form or as a compiled
 * binary, for any purpose, commercial or non-commercial, and by any
 * means.
 *
 * In jurisdictions that recognize copyright laws, the author or authors
 * of this software dedicate any and all copyright interest in the
 * software to the public domain. We make this dedication for the benefit
 * of the public at large and to the detriment of our heirs and
 * successors. We intend this dedication to be an overt act of
 * relinquishment in perpetuity of all present and future rights to this
 * software under copyright law.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#define	__bitmap_size(bm)	__bitstr_size(bm->cnt)
#define	__bitmap_alloced(bm)	__bitstr_size(bm->size)

#define	__min_bitmap_size(a, b)	\
	(((a)->cnt < (b)->cnt) ? __bitstr_size(a->cnt) : __bitstr_size(b->cnt))

/*
 * Allocate extra bytes so we don't need to realloc as often.
 */
#define	WT_BITMAP_EXTRA				10

/*
 * __wt_bitmap_ensure --
 *	Ensure bitmap has room for a number of bits.
 */
static inline int
__wt_bitmap_ensure(WT_SESSION_IMPL *session, WT_BITMAP *bm, uint64_t nbits)
{
	uint64_t newbits;

	if (bm->cnt < nbits) {
		if (bm->size < nbits) {
			newbits = (__bitstr_size(nbits) + WT_BITMAP_EXTRA) * 8;
			WT_RET(__bit_realloc(session, bm->size, newbits,
			    &bm->bitstring));
			bm->size = newbits;
		}
		bm->cnt = nbits;
	}
	return (0);
}

/*
 * __wt_bitmap_alloc_bit --
 *	Find the first unset bit, set it and return the bit number.
 *	The bitmap is grown if needed.
 */
static inline int
__wt_bitmap_alloc_bit(WT_SESSION_IMPL *session, WT_BITMAP *bm,
    uint64_t *descriptorp)
{
	uint64_t avail;

	if (bm->bitstring == NULL) {
		WT_RET(__bit_alloc(session, 1, &bm->bitstring));
		bm->cnt = bm->size = 1;
	}
	if (__bit_ffc(bm->bitstring, bm->cnt, &avail) != 0) {
		avail = bm->cnt;
		WT_RET(__wt_bitmap_ensure(session, bm, bm->cnt + 1));
	}
	__bit_set(bm->bitstring, avail);
	*descriptorp = avail;
	return (0);
}

/*
 * __wt_bitmap_set_quick --
 *	Set a bit in the bitmap, which is assumed to be big enough.
 */
static inline void
__wt_bitmap_set_quick(WT_SESSION_IMPL *session, WT_BITMAP *bm, uint64_t bit)
{
	WT_ASSERT(session, bit < bm->cnt);
	__bit_set(bm->bitstring, bit);
}

/*
 * __wt_bitmap_set --
 *	Set a bit in the bitmap, growing it if needed.
 */
static inline int
__wt_bitmap_set(WT_SESSION_IMPL *session, WT_BITMAP *bm, uint64_t bit)
{
	WT_RET(__wt_bitmap_ensure(session, bm, bit + 1));
	__bit_set(bm->bitstring, bit);
	return (0);
}

/*
 * __wt_bitmap_clear --
 *	Clear a bit in the bitmap.
 */
static inline void
__wt_bitmap_clear(WT_SESSION_IMPL *session, WT_BITMAP *bm, uint64_t bit)
{
	WT_ASSERT(session, bit < bm->cnt);
	__bit_clear(bm->bitstring, bit);
}

/*
 * __wt_bitmap_test --
 *	Test a bit in the bitmap.
 */
static inline bool
__wt_bitmap_test(WT_SESSION_IMPL *session, WT_BITMAP *bm, uint64_t bit)
{
	WT_ASSERT(session, bit < bm->cnt);
	return (__bit_test(bm->bitstring, bit));
}

/*
 * __wt_bitmap_or_bitmap --
 *	Logical or with another bitmap.
 */
static inline int
__wt_bitmap_or_bitmap(WT_SESSION_IMPL *session, WT_BITMAP *dst, WT_BITMAP *src)
{
	size_t i, nbytes;

	nbytes = __bitmap_size(src);
	WT_RET(__wt_bitmap_ensure(session, dst, src->cnt));
	for (i = 0; i < nbytes; i++)
		dst->bitstring[i] |= src->bitstring[i];
	return (0);
}

/*
 * __wt_bitmap_clear_bitmap --
 *	Clear bits according to those set in another bitmap.
 */
static inline void
__wt_bitmap_clear_bitmap(WT_SESSION_IMPL *session, WT_BITMAP *dst,
    WT_BITMAP *src)
{
	size_t i, nbytes;

	WT_UNUSED(session);
	nbytes = __min_bitmap_size(dst, src);
	for (i = 0; i < nbytes; i++)
		dst->bitstring[i] &= ~src->bitstring[i];
}

/*
 * __wt_bitmap_test_bitmap --
 *	Test bits according to those set in another bitmap.
 *	Returns true if any of the designated bits are set.
 */
static inline bool
__wt_bitmap_test_bitmap(WT_SESSION_IMPL *session, WT_BITMAP *bm,
    WT_BITMAP *test)
{
	size_t i, nbytes;

	WT_UNUSED(session);
	nbytes = __min_bitmap_size(bm, test);
	for (i = 0; i < nbytes; i++)
		if (bm->bitstring[i] & test->bitstring[i])
			return (true);
	return (false);
}

/*
 * __wt_bitmap_test_all_bitmap --
 *	Test bits according to those set in another bitmap.
 *	Returns true if all of the designated bits are set.
 */
static inline bool
__wt_bitmap_test_all_bitmap(WT_SESSION_IMPL *session, WT_BITMAP *bm,
    WT_BITMAP *test)
{
	size_t i, nbytes;

	WT_UNUSED(session);
	nbytes = __min_bitmap_size(bm, test);
	for (i = 0; i < nbytes; i++)
		if ((bm->bitstring[i] & test->bitstring[i]) !=
		    test->bitstring[i])
			return (false);
	return (true);
}

/*
 * __wt_bitmap_copy_bitmap --
 *	Copy one bitmap to another.
 */
static inline int
__wt_bitmap_copy_bitmap(WT_SESSION_IMPL *session, WT_BITMAP *dst,
    WT_BITMAP *src)
{
	size_t i, nbytes;

	nbytes = __bitmap_size(src);
	WT_RET(__wt_bitmap_ensure(session, dst, src->cnt));
	for (i = 0; i < nbytes; i++)
		dst->bitstring[i] = src->bitstring[i];
	return (0);
}

/*
 * __wt_bitmap_test_any --
 *	Test whether any of the bits in the bitmap are set.
 */
static inline bool
__wt_bitmap_test_any(WT_BITMAP *bm)
{
	size_t i, nbytes;

	nbytes = __bitmap_size(bm);
	for (i = 0; i < nbytes; i++)
		if (bm->bitstring[i] != 0)
			return (true);
	return (false);
}

/*
 * __wt_bitmap_clear_all --
 *	Clear all bits in the bitmap.
 */
static inline void
__wt_bitmap_clear_all(WT_BITMAP *bm)
{
	memset(bm->bitstring, 0, __bitmap_alloced(bm));
}

/*
 * __wt_bitmap_free --
 *	Free any storage allocated by the bitmap.
 */
static inline void
__wt_bitmap_free(WT_SESSION_IMPL *session, WT_BITMAP *bm)
{
	__wt_free(session, bm->bitstring);
	bm->cnt = bm->size = 0;
}
