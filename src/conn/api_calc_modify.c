/*-
 * Copyright (c) 2014-2018 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

#if 0
/*
 * wiredtiger_calc_modify --
 *	Calculate a set of WT_MODIFY operations to represent an update.
 */
int
wiredtiger_calc_modify(const WT_ITEM *prev, const WT_ITEM *newv,
    size_t maxdiff, WT_MODIFY *entries, int *nentriesp)
{
	size_t i, end_match, len, start_match;
	int max_entries, nentries;
	const char *np, *pp;

	len = WT_MIN(prev->size, newv->size);
	max_entries = *nentriesp;
	WT_UNUSED(max_entries);

	nentries = prev->size == newv->size ? 1 : 2;
	if (prev->size != newv->size) {
	    entries[0].data.data = newv->data;
	    entries[0].data.size = 4;
	    entries[0].offset = 0;
	    entries[0].size = 4;
	}

	pp = prev->data;
	np = newv->data;
	for (i = 4, pp += 4, np += 4;
	    i < len && *pp == *np;
	    pp++, np++, i++)
	    ;
	start_match = i;

	pp = prev->data;
	np = newv->data;
	for (i = 0, pp += prev->size - 1, np += newv->size - 1;
	    i < len && *pp == *np;
	    pp--, np--, i++)
	    ;
	end_match = i;

	entries[nentries - 1].data.data =
	    (const char *)newv->data + start_match;
	entries[nentries - 1].data.size = newv->size - end_match;
	entries[nentries - 1].offset = start_match;
	entries[nentries - 1].size = prev->size - end_match;

	if (newv->size - end_match > maxdiff ||
	    prev->size - end_match > maxdiff)
		return (WT_NOTFOUND);

	*nentriesp = nentries;
	return (0);
}
#else

#define	WT_CM_BLOCKSIZE 8
#define	WT_CM_MINMATCH 64
#define	WT_CM_STARTGAP (WT_CM_BLOCKSIZE / 2)

typedef struct {
	const uint8_t *s1, *e1; /* Start / end of pre-image. */
	const uint8_t *s2, *e2; /* Start / end of after-image. */

	size_t maxdiff;
	int maxentries;
} WT_CM_STATE;

typedef struct {
	const uint8_t *m1, *m2;
	size_t len;
} WT_CM_MATCH;

/*
 * __cm_extend --
 *	Given a potential match size, extend to find the complete match.
 */
static void
__cm_extend(WT_CM_STATE *cms,
    const uint8_t *m1, const uint8_t *m2, WT_CM_MATCH *match)
{
	const uint8_t *p1, *p2;

	/* Step past the end and before the beginning of the matching block. */
	for (p1 = m1, p2 = m2;
	    p1 < cms->e1 && p2 < cms->e2 && *p1 == *p2;
	    p1++, p2++)
		;

	for (; m1 >= cms->s1 && m2 >= cms->s2 && *m1 == *m2;
	    m1--, m2--)
		;

	match->m1 = m1 + 1;
	match->m2 = m2 + 1;
	match->len = p1 > m1 ? (size_t)(p1 - m1 - 1) : 0;
}

/*
 * __cm_hash --
 *	Calculate a "hash" of a block of bytes.
 */
static uint64_t
__cm_hash(const uint8_t *p)
{
	uint64_t h;

	memcpy(&h, p, sizeof h);
#ifndef WORDS_BIGENDIAN
	h = __wt_bswap64(h);
#endif
	return (h);
}

/*
 * __cm_add_modify --
 *	Add a modify operation to the list of entries.
 *
 *	Fails if all entries are used or the maximum bytes of difference is
 *	exceeded.
 */
static int
__cm_add_modify(WT_CM_STATE *cms, const uint8_t *p1, size_t len1,
    const uint8_t *p2, size_t len2, WT_MODIFY *entries, int *nentriesp)
{
	WT_MODIFY *mod;

	if (*nentriesp >= cms->maxentries || WT_MAX(len1, len2) > cms->maxdiff)
		return (WT_NOTFOUND);

	cms->maxdiff -= WT_MAX(len1, len2);
	mod = entries + (*nentriesp)++;
	mod->size = len1;
	mod->offset = (size_t)(p1 - cms->s1);
	mod->data.data = p2;
	mod->data.size = len2;

	return (0);
}

/*
 * wiredtiger_calc_modify --
 *	Calculate a set of WT_MODIFY operations to represent an update.
 */
int
wiredtiger_calc_modify(const WT_ITEM *prev, const WT_ITEM *newv,
    size_t maxdiff, WT_MODIFY *entries, int *nentriesp)
{
	WT_CM_MATCH match;
	WT_CM_STATE cms;
	size_t gap, i;
	uint64_t h, hend, hstart;
	const uint8_t *p1, *p1end, *p2, *used1, *used2;

	cms.s1 = prev->data;
	cms.e1 = cms.s1 + prev->size;
	cms.s2 = newv->data;
	cms.e2 = cms.s2 + newv->size;
	cms.maxdiff = maxdiff;
	cms.maxentries = *nentriesp;
	*nentriesp = 0;

	/* Ignore matches at the beginning / end. */
	__cm_extend(&cms, cms.s1, cms.s2, &match);
	used1 = cms.s1 + match.len;
	used2 = cms.s2 + match.len;
	if (used1 < cms.e1 && used2 < cms.e2) {
		__cm_extend(&cms, cms.e1 - 1, cms.e2 - 1, &match);
		cms.e1 -= match.len;
		cms.e2 -= match.len;
	}

	if (used1 + WT_CM_BLOCKSIZE >= cms.e1 ||
	    used2 + WT_CM_BLOCKSIZE >= cms.e2)
		goto end;

	/*
	 * Walk through the post-image, maintaining start / end markers
	 * separated by a gap in the pre-image.  If the current point in the
	 * post-image matches either marker, try to extend the match to find a
	 * (large) range of matching bytes.  If the end of the range is reached
	 * in the post-image without finding a good match, double the size of
	 * the gap, update the markers and keep trying.
	 */
	p1 = p1end = cms.s1;
	hend = hstart = __cm_hash(p1);
	for (p2 = cms.s2, i = gap = WT_CM_STARTGAP;
	    p1 + WT_CM_BLOCKSIZE < cms.e1 && p2 + WT_CM_BLOCKSIZE < cms.e2;
	    p1++, p2++, i++) {
		if (i == gap) {
			gap *= 2;
			if (p1 + gap + WT_CM_BLOCKSIZE >= cms.e1)
				gap = (size_t)(cms.e1 - p1 - WT_CM_BLOCKSIZE);
			if (gap > maxdiff)
				return (WT_NOTFOUND);
			hstart = hend;
			p1end = p1 + gap;
			hend = __cm_hash(p1end);
			i = 0;
		}
		h = __cm_hash(p2);
		match.len = 0;
		if (h == hstart)
			__cm_extend(&cms, p1 + i, p2, &match);
		else if (h == hend)
			__cm_extend(&cms, p1end, p2, &match);

		if (match.len >= WT_CM_MINMATCH) {
			WT_RET(__cm_add_modify(&cms,
			    used1, (size_t)(match.m1 - used1),
			    used2, (size_t)(match.m2 - used2),
			    entries, nentriesp));
			used1 = p1 = match.m1 + match.len;
			used2 = p2 = match.m2 + match.len;
			gap = WT_CM_STARTGAP;
			hend = hstart = __cm_hash(p1);
		}
	}

end:	if (used1 < cms.e1 || used2 < cms.e2)
		WT_RET(__cm_add_modify(&cms,
		    used1, (size_t)(cms.e1 - used1),
		    used2, (size_t)(cms.e2 - used2),
		    entries, nentriesp));

	return (0);
}

#endif
