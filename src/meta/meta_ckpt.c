/*-
 * Copyright (c) 2008-2012 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

static int __ckpt_last_addr(WT_SESSION_IMPL *, const char *, WT_ITEM *);
static int __ckpt_last_name(WT_SESSION_IMPL *, const char *, const char **);
static int __ckpt_named_addr(
	WT_SESSION_IMPL *, const char *, const char *, WT_ITEM *);
static int __ckpt_set(WT_SESSION_IMPL *, const char *, const char *);
static int __ckpt_version_chk(WT_SESSION_IMPL *, const char *, const char *);

/*
 * __wt_meta_checkpoint_addr --
 *	Return a file's checkpoint address.
 */
int
__wt_meta_checkpoint_addr(WT_SESSION_IMPL *session,
    const char *fname, const char *checkpoint, WT_ITEM *addr)
{
	WT_DECL_RET;
	const char *config;

	config = NULL;

	/* Retrieve the metadata entry for the file. */
	WT_ERR(__wt_metadata_read(session, fname, &config));

	/* Check the major/minor version numbers. */
	WT_ERR(__ckpt_version_chk(session, fname, config));

	/*
	 * Retrieve the named checkpoint or the last checkpoint.
	 *
	 * If we don't find a named checkpoint, we're done, they're read-only.
	 * If we don't find a default checkpoint, it's creation, return "no
	 * data" and let our caller handle it.
	 */
	if (checkpoint == NULL) {
		if ((ret =
		    __ckpt_last_addr(session, config, addr)) == WT_NOTFOUND) {
			ret = 0;
			addr->data = NULL;
			addr->size = 0;
		}
	} else
		WT_ERR(__ckpt_named_addr(session, checkpoint, config, addr));

err:	__wt_free(session, config);
	return (ret);
}

/*
 * __wt_meta_checkpoint_last_name --
 *	Return the last unnamed checkpoint's name.
 */
int
__wt_meta_checkpoint_last_name(
    WT_SESSION_IMPL *session, const char *fname, const char **namep)
{
	WT_DECL_RET;
	const char *config;

	config = NULL;

	/* Retrieve the metadata entry for the file. */
	WT_ERR(__wt_metadata_read(session, fname, &config));

	/* Check the major/minor version numbers. */
	WT_ERR(__ckpt_version_chk(session, fname, config));

	/* Retrieve the name of the last unnamed checkpoint. */
	WT_ERR(__ckpt_last_name(session, config, namep));

err:	__wt_free(session, config);
	return (ret);
}

/*
 * __wt_meta_checkpoint_clear --
 *	Clear a file's checkpoint.
 */
int
__wt_meta_checkpoint_clear(WT_SESSION_IMPL *session, const char *fname)
{
	/*
	 * If we are unrolling a failed create, we may have already removed the
	 * metadata entry.  If no entry is found to update and we're trying to
	 * clear the checkpoint, just ignore it.
	 */
	WT_RET_NOTFOUND_OK(__ckpt_set(session, fname, NULL));

	return (0);
}

/*
 * __ckpt_set --
 *	Set a file's checkpoint.
 */
static int
__ckpt_set(WT_SESSION_IMPL *session, const char *fname, const char *v)
{
	WT_DECL_RET;
	const char *config, *cfg[3], *newcfg;

	config = newcfg = NULL;

	/* Retrieve the metadata for this file. */
	WT_ERR(__wt_metadata_read(session, fname, &config));

	/* Replace the checkpoint entry. */
	cfg[0] = config;
	cfg[1] = v == NULL ? "checkpoint=()" : v;
	cfg[2] = NULL;
	WT_ERR(__wt_config_collapse(session, cfg, &newcfg));
	WT_ERR(__wt_metadata_update(session, fname, newcfg));

err:	__wt_free(session, config);
	__wt_free(session, newcfg);
	return (ret);
}

/*
 * __ckpt_named_addr --
 *	Return the cookie associated with a file's named checkpoint.
 */
static int
__ckpt_named_addr(WT_SESSION_IMPL *session,
    const char *checkpoint, const char *config, WT_ITEM *addr)
{
	WT_CONFIG ckptconf;
	WT_CONFIG_ITEM a, k, v;

	WT_RET(__wt_config_getones(session, config, "checkpoint", &v));
	WT_RET(__wt_config_subinit(session, &ckptconf, &v));

	/*
	 * Take the first match: there should never be more than a single
	 * checkpoint of any name.
	 */
	while (__wt_config_next(&ckptconf, &k, &v) == 0)
		if (WT_STRING_MATCH(checkpoint, k.str, k.len)) {
			WT_RET(__wt_config_subgets(session, &v, "addr", &a));
			WT_RET(__wt_nhex_to_raw(session, a.str, a.len, addr));
			return (0);
		}
	return (WT_NOTFOUND);
}

/*
 * __ckpt_last_addr --
 *	Return the cookie associated with the file's last checkpoint.
 */
static int
__ckpt_last_addr(
    WT_SESSION_IMPL *session, const char *config, WT_ITEM *addr)
{
	WT_CONFIG ckptconf;
	WT_CONFIG_ITEM a, k, v;
	int64_t found;

	WT_RET(__wt_config_getones(session, config, "checkpoint", &v));
	WT_RET(__wt_config_subinit(session, &ckptconf, &v));
	for (found = 0; __wt_config_next(&ckptconf, &k, &v) == 0;) {
		/* Ignore checkpoints before the ones we've already seen. */
		WT_RET(__wt_config_subgets(session, &v, "order", &a));
		if (found && a.val < found)
			continue;
		found = a.val;

		/*
		 * Copy out the address; our caller wants the raw cookie, not
		 * the hex.
		 */
		WT_RET(__wt_config_subgets(session, &v, "addr", &a));
		if (a.len == 0)
			WT_RET(EINVAL);
		WT_RET(__wt_nhex_to_raw(session, a.str, a.len, addr));
	}

	return (found ? 0 : WT_NOTFOUND);
}

/*
 * __ckpt_last_name --
 *	Return the name associated with the file's last unnamed checkpoint.
 */
static int
__ckpt_last_name(
    WT_SESSION_IMPL *session, const char *config, const char **namep)
{
	WT_CONFIG ckptconf;
	WT_CONFIG_ITEM a, k, v;
	WT_DECL_RET;
	int64_t found;

	*namep = NULL;

	WT_ERR(__wt_config_getones(session, config, "checkpoint", &v));
	WT_ERR(__wt_config_subinit(session, &ckptconf, &v));
	for (found = 0; __wt_config_next(&ckptconf, &k, &v) == 0;) {
		/*
		 * We only care about unnamed checkpoints; applications may not
		 * use any matching prefix as a checkpoint name, the comparison
		 * is pretty simple.
		 */
		if (k.len < strlen(WT_CHECKPOINT) ||
		    strncmp(k.str, WT_CHECKPOINT, strlen(WT_CHECKPOINT)) != 0)
			continue;

		/* Ignore checkpoints before the ones we've already seen. */
		WT_ERR(__wt_config_subgets(session, &v, "order", &a));
		if (found && a.val < found)
			continue;

		if (*namep != NULL)
			__wt_free(session, *namep);
		WT_ERR(__wt_strndup(session, k.str, k.len, namep));
		found = a.val;
	}
	if (!found)
		ret = WT_NOTFOUND;

	if (0) {
err:		__wt_free(session, namep);
	}
	return (ret);
}

/*
 * __ckpt_compare_order --
 *	Qsort comparison routine for the checkpoint list.
 */
static int
__ckpt_compare_order(const void *a, const void *b)
{
	WT_CKPT *ackpt, *bckpt;

	ackpt = (WT_CKPT *)a;
	bckpt = (WT_CKPT *)b;

	return (ackpt->order > bckpt->order ? 1 : -1);
}

/*
 * __wt_meta_ckptlist_get --
 *	Load all available checkpoint information for a file.
 */
int
__wt_meta_ckptlist_get(
    WT_SESSION_IMPL *session, const char *fname, WT_CKPT **ckptbasep)
{
	WT_CKPT *ckpt, *ckptbase;
	WT_CONFIG ckptconf;
	WT_CONFIG_ITEM a, k, v;
	WT_DECL_RET;
	WT_ITEM *buf;
	size_t allocated, slot;
	const char *config;
	char timebuf[64];

	*ckptbasep = NULL;

	buf = NULL;
	ckptbase = NULL;
	allocated = slot = 0;
	config = NULL;

	/* Retrieve the metadata information for the file. */
	WT_RET(__wt_metadata_read(session, fname, &config));

	/* Load any existing checkpoints into the array. */
	WT_ERR(__wt_scr_alloc(session, 0, &buf));
	if (__wt_config_getones(session, config, "checkpoint", &v) == 0 &&
	    __wt_config_subinit(session, &ckptconf, &v) == 0)
		for (; __wt_config_next(&ckptconf, &k, &v) == 0; ++slot) {
			if (slot * sizeof(WT_CKPT) == allocated)
				WT_ERR(__wt_realloc(session, &allocated,
				    (slot + 50) * sizeof(WT_CKPT), &ckptbase));
			ckpt = &ckptbase[slot];

			/*
			 * Copy the name, address (raw and hex), order and time
			 * into the slot.
			 */
			WT_ERR(
			    __wt_strndup(session, k.str, k.len, &ckpt->name));

			WT_ERR(__wt_config_subgets(session, &v, "addr", &a));
			if (a.len == 0)
				goto format;
			WT_ERR(__wt_buf_set(
			    session, &ckpt->addr, a.str, a.len));
			WT_ERR(__wt_nhex_to_raw(
			    session, a.str, a.len, &ckpt->raw));

			WT_ERR(__wt_config_subgets(session, &v, "order", &a));
			if (a.val == 0)
				goto format;
			ckpt->order = a.val;

			WT_ERR(__wt_config_subgets(session, &v, "time", &a));
			if (a.len == 0)
				goto format;
			if (a.len > sizeof(timebuf) - 1)
				goto format;
			memcpy(timebuf, a.str, a.len);
			timebuf[a.len] = '\0';
			if (sscanf(timebuf, "%" SCNuMAX, &ckpt->sec) != 1)
				goto format;

			WT_ERR(__wt_config_subgets(session, &v, "size", &a));
			ckpt->ckpt_size = (uint64_t)a.val;
		}

	/*
	 * Allocate an extra slot for a new value, plus a slot to mark the end.
	 *
	 * This isn't very clean, but there's necessary cooperation between the
	 * schema layer (that maintains the list of checkpoints), the btree
	 * layer (that knows when the root page is written, creating a new
	 * checkpoint), and the block manager (which actually creates the
	 * checkpoint).  All of that cooperation is handled in the WT_CKPT
	 * structure referenced from the WT_BTREE structure.
	 */
	if ((slot + 2) * sizeof(WT_CKPT) >= allocated)
		WT_ERR(__wt_realloc(session, &allocated,
		    (slot + 2) * sizeof(WT_CKPT), &ckptbase));

	/* Sort in creation-order. */
	qsort(ckptbase, slot, sizeof(WT_CKPT), __ckpt_compare_order);

	/* Return the array to our caller. */
	*ckptbasep = ckptbase;

	if (0) {
format:		WT_ERR_MSG(session, WT_ERROR, "corrupted checkpoint list");
err:		__wt_meta_ckptlist_free(session, ckptbase);
	}
	__wt_free(session, config);
	__wt_scr_free(&buf);

	return (ret);
}

/*
 * __wt_meta_ckptlist_set --
 *	Set a file's checkpoint value from the WT_CKPT list.
 */
int
__wt_meta_ckptlist_set(
    WT_SESSION_IMPL *session, const char *fname, WT_CKPT *ckptbase)
{
	WT_CKPT *ckpt;
	WT_DECL_RET;
	WT_ITEM *buf;
	int64_t maxorder;
	const char *sep;

	buf = NULL;

	WT_ERR(__wt_scr_alloc(session, 0, &buf));
	maxorder = 0;
	sep = "";
	WT_ERR(__wt_buf_fmt(session, buf, "checkpoint=("));
	WT_CKPT_FOREACH(ckptbase, ckpt) {
		/*
		 * Each internal checkpoint name is appended with a generation
		 * to make it a unique name.  We're solving two problems: when
		 * two checkpoints are taken quickly, the timer may not be
		 * unique and/or we can even see time travel on the second
		 * checkpoint if we snapshot the time in-between nanoseconds
		 * rolling over.  Second, if we reset the generational counter
		 * when new checkpoints arrive, we could logically re-create
		 * specific checkpoints, racing with cursors open on those
		 * checkpoints.  I can't think of any way to return incorrect
		 * results by racing with those cursors, but it's simpler not
		 * to worry about it.
		 */
		if (ckpt->order > maxorder)
			maxorder = ckpt->order;

		/* Skip deleted checkpoints. */
		if (F_ISSET(ckpt, WT_CKPT_DELETE))
			continue;

		if (F_ISSET(ckpt, WT_CKPT_ADD | WT_CKPT_UPDATE)) {
			/* Convert the raw cookie to a hex string. */
			WT_ERR(__wt_raw_to_hex(session,
			    ckpt->raw.data, ckpt->raw.size, &ckpt->addr));

			if (F_ISSET(ckpt, WT_CKPT_ADD)) {
				ckpt->order = ++maxorder;

				/* Assert we have a checkpoint. */
				WT_ASSERT(session, ckpt->raw.data != NULL);
			}
		}
		if (strcmp(ckpt->name, WT_CHECKPOINT) == 0)
			WT_ERR(__wt_buf_catfmt(session, buf,
			    "%s%s.%" PRId64 "=(addr=\"%.*s\",order=%" PRIu64
			    ",time=%" PRIuMAX ",size=%" PRIu64 ")",
			    sep, ckpt->name, ckpt->order,
			    (int)ckpt->addr.size, (char *)ckpt->addr.data,
			    ckpt->order, ckpt->sec, ckpt->ckpt_size));
		else
			WT_ERR(__wt_buf_catfmt(session, buf,
			    "%s%s=(addr=\"%.*s\",order=%" PRIu64
			    ",time=%" PRIuMAX ",size=%" PRIu64 ")",
			    sep, ckpt->name,
			    (int)ckpt->addr.size, (char *)ckpt->addr.data,
			    ckpt->order, ckpt->sec, ckpt->ckpt_size));
		sep = ",";
	}
	WT_ERR(__wt_buf_catfmt(session, buf, ")"));
	WT_ERR(__ckpt_set(session, fname, buf->mem));

err:	__wt_scr_free(&buf);

	return (ret);
}

/*
 * __wt_meta_ckptlist_free --
 *	Discard the checkpoint array.
 */
void
__wt_meta_ckptlist_free(WT_SESSION_IMPL *session, WT_CKPT *ckptbase)
{
	WT_CKPT *ckpt;

	if (ckptbase == NULL)
		return;

	WT_CKPT_FOREACH(ckptbase, ckpt) {
		__wt_free(session, ckpt->name);
		__wt_buf_free(session, &ckpt->addr);
		__wt_buf_free(session, &ckpt->raw);
		__wt_free(session, ckpt->bpriv);
	}
	__wt_free(session, ckptbase);
}

/*
 * __ckpt_version_chk --
 *	Check the version major/minor numbers.
 */
static int
__ckpt_version_chk(
    WT_SESSION_IMPL *session, const char *fname, const char *config)
{
	WT_CONFIG_ITEM a, v;
	int majorv, minorv;

	WT_RET(__wt_config_getones(session, config, "version", &v));
	WT_RET(__wt_config_subgets(session, &v, "major", &a));
	majorv = (int)a.val;
	WT_RET(__wt_config_subgets(session, &v, "minor", &a));
	minorv = (int)a.val;

	if (majorv > WT_BTREE_MAJOR_VERSION ||
	    (majorv == WT_BTREE_MAJOR_VERSION &&
	    minorv > WT_BTREE_MINOR_VERSION))
		WT_RET_MSG(session, EACCES,
		    "%s is an unsupported version of a WiredTiger file",
		    fname);
	return (0);
}
