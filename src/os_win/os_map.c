/*-
 * Copyright (c) 2014-2016 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_win_map --
 *	Map a file into memory.
 */
int
__wt_win_map(WT_FILE_HANDLE *file_handle,
    WT_SESSION *wt_session, void *mapp, size_t *lenp, void **mappingcookie)
{
	WT_DECL_RET;
	size_t len;
	wt_off_t file_size;
	void *map;
	WT_FILE_HANDLE_WIN *win_fh;
	WT_SESSION_IMPL *session;

	win_fh = (WT_FILE_HANDLE_WIN *)file_handle;
	session = (WT_SESSION_IMPL *)wt_session;

	/*
	 * There's no locking here to prevent the underlying file from changing
	 * underneath us, our caller needs to ensure consistency of the mapped
	 * region vs. any other file activity.
	 */
	WT_RET(__wt_filesize(session, win_fh, &file_size));
	len = (size_t)file_size;

	(void)__wt_verbose(session, WT_VERB_HANDLEOPS,
	    "%s: memory-map: %" WT_SIZET_FMT " bytes", win_fh->name, len);

	*mappingcookie = CreateFileMappingA(
	    win_fh->filehandle, NULL, PAGE_READONLY, 0, 0, NULL);
	if (*mappingcookie == NULL)
		WT_RET_MSG(session, __wt_getlasterror(),
		    "%s: memory-map: CreateFileMappingA", win_fh->name);

	if ((map =
	    MapViewOfFile(*mappingcookie, FILE_MAP_READ, 0, 0, len)) == NULL) {
		/* Retrieve the error before cleaning up. */
		ret = __wt_getlasterror();
		CloseHandle(*mappingcookie);
		*mappingcookie = NULL;

		WT_RET_MSG(session, ret,
		    "%s: memory-map: MapViewOfFile",  win_fh->name);
	}

	*(void **)mapp = map;
	*lenp = len;
	return (0);
}

/*
 * __wt_win_map_preload --
 *	Cause a section of a memory map to be faulted in.
 */
int
__wt_win_map_preload(WT_FILE_HANDLE *file_handle,
    WT_SESSION *wt_session, const void *p, size_t size)
{
	WT_UNUSED(session);
	WT_UNUSED(file_handle);
	WT_UNUSED(p);
	WT_UNUSED(size);

	return (ENOTSUP);
}

/*
 * __wt_win_map_discard --
 *	Discard a chunk of the memory map.
 */
int
__wt_win_map_discard(
    WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session, void *p, size_t size)
{
	WT_UNUSED(session);
	WT_UNUSED(file_handle);
	WT_UNUSED(p);
	WT_UNUSED(size);

	return (ENOTSUP);
}

/*
 * __wt_win_map_unmap --
 *	Remove a memory mapping.
 */
int
__wt_win_map_unmap(WT_FILE_HANDLE *file_handle,
    WT_SESSION *wt_session, void *map, size_t len, void **mappingcookie)
{
	WT_DECL_RET;
	WT_FILE_HANDLE_WIN *win_fh;
	WT_SESSION_IMPL *session;

	win_fh = (WT_FILE_HANDLE_WIN *)file_handle;
	session = (WT_SESSION_IMPL *)wt_session;

	(void)__wt_verbose(session, WT_VERB_HANDLEOPS,
	    "%s: memory-unmap: %" WT_SIZET_FMT " bytes", win_fh->name, len);

	WT_ASSERT(session, *mappingcookie != NULL);

	if (UnmapViewOfFile(map) == 0) {
		ret = __wt_getlasterror();
		__wt_err(session, ret,
		    "%s: memory-unmap: UnmapViewOfFile", win_fh->name);
	}

	if (CloseHandle(*mappingcookie) == 0) {
		ret = __wt_getlasterror();
		__wt_err(session, ret,
		    "%s: memory-unmap: CloseHandle", win_fh->name);
	}

	*mappingcookie = NULL;

	return (ret);
}
