/*-
 * Public Domain 2014-present MongoDB, Inc.
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

#include "wt_internal.h"
#include <unistd.h>

// #define WT_UNION_FS_STOP_SUFFIX ".stop"
// #define WT_UNION_FS_TOMBSTONE_SUFFIX ".deleted"

static int __union_merge_with_next_extents(WT_SESSION_IMPL *session, WT_UNION_ALLOC_LIST *extent);

/*
 * __union_fs_filename --
 *     Generate a filename for the given layer.
 */
static int
__union_fs_filename(
  WT_UNION_FS_LAYER *layer, WT_SESSION_IMPL *session, const char *name, char **pathp)
{
    WT_DECL_RET;
    size_t len;
    char *buf;

    if (__wt_absolute_path(name))
        WT_RET_MSG(session, EINVAL, "Not a relative pathname: %s", name);

    len = strlen(layer->home) + 1 + strlen(name) + 1;
    WT_RET(__wt_calloc(session, 1, len, &buf));
    WT_ERR(__wt_snprintf(buf, len, "%s%s%s", layer->home, __wt_path_separator(), name));
    *pathp = buf;

    if (0) {
err:
        __wt_free(session, buf);
    }
    return (ret);
}

// /*
//  * __union_fs_marker --
//  *     Generate a name of a marker file.
//  */
// static int
// __union_fs_marker(
//   WT_FILE_SYSTEM *fs, WT_SESSION_IMPL *session, const char *name, const char *marker, char **out)
// {
//     size_t p, suffix_len;

//     WT_UNUSED(fs);

//     p = strlen(name);
//     suffix_len = strlen(marker);

//     WT_RET(__wt_malloc(session, p + suffix_len + 1, out));
//     memcpy(*out, name, p);
//     memcpy(*out + p, marker, suffix_len + 1);
//     return (0);
// }

// /*
//  * __union_fs_stop --
//  *     Generate a name of a stop marker.
//  */
// static int
// __union_fs_stop(WT_FILE_SYSTEM *fs, WT_SESSION_IMPL *session, const char *name, char **out)
// {
//     return (__union_fs_marker(fs, session, name, WT_UNION_FS_STOP_SUFFIX, out));
// }

// /*
//  * __union_fs_tombstone --
//  *     Generate a name of a tombstone.
//  */
// static int
// __union_fs_tombstone(
//   WT_FILE_SYSTEM *fs, WT_SESSION_IMPL *session, const char *name, char **tombstonep)
// {
//     return (__union_fs_marker(fs, session, name, WT_UNION_FS_TOMBSTONE_SUFFIX, tombstonep));
// }

// /*
//  * __union_fs_create_marker --
//  *     Create a marker file for the given file.
//  */
// static int
// __union_fs_create_marker(WT_FILE_SYSTEM *fs, WT_SESSION_IMPL *session, const char *name,
//   const char *marker, uint32_t flags)
// {
//     WT_DECL_RET;
//     WT_FILE_HANDLE *fh;
//     WT_FILE_SYSTEM *layer_fs;
//     WT_UNION_FS *u;
//     uint32_t open_flags;
//     char *path, *path_marker;

//     fh = NULL;
//     path = NULL;
//     path_marker = NULL;
//     u = (WT_UNION_FS *)fs;

//     WT_ERR(__union_fs_filename(__union_fs_top(u), session, name, &path));

//     layer_fs = &u->destination.file_system;
//     open_flags = WT_FS_OPEN_CREATE;
//     if (LF_ISSET(WT_FS_DURABLE | WT_FS_OPEN_DURABLE))
//         FLD_SET(open_flags, WT_FS_OPEN_DURABLE);

//     WT_ERR(__union_fs_marker(fs, session, path, marker, &path_marker));
//     WT_ERR(layer_fs->fs_open_file(
//       layer_fs, &session->iface, path_marker, WT_FS_OPEN_FILE_TYPE_DATA, open_flags, &fh));
//     WT_ERR(fh->close(fh, &session->iface));

// err:
//     __wt_free(session, path);
//     __wt_free(session, path_marker);
//     return (0);
// }

// /*
//  * __union_fs_create_stop --
//  *     Create a stop marker for the given file.
//  */
// static int
// __union_fs_create_stop(
//   WT_FILE_SYSTEM *fs, WT_SESSION_IMPL *session, const char *name, uint32_t flags)
// {
//     return (__union_fs_create_marker(fs, session, name, WT_UNION_FS_STOP_SUFFIX, flags));
// }

// /*
//  * __union_fs_create_tombstone --
//  *     Create a tombstone for the given file.
//  */
// static int
// __union_fs_create_tombstone(
//   WT_FILE_SYSTEM *fs, WT_SESSION_IMPL *session, const char *name, uint32_t flags)
// {
//     return (__union_fs_create_marker(fs, session, name, WT_UNION_FS_TOMBSTONE_SUFFIX, flags));
// }

// /*
//  * __union_fs_is_stop --
//  *     Check if the given file is a stop file marker.
//  */
// static bool
// __union_fs_is_stop(WT_FILE_SYSTEM *fs, WT_SESSION_IMPL *session, const char *name)
// {
//     size_t name_len, suffix_len;

//     WT_UNUSED(fs);
//     WT_UNUSED(session);

//     name_len = strlen(name);
//     suffix_len = strlen(WT_UNION_FS_STOP_SUFFIX);
//     if (name_len <= suffix_len)
//         return (false);

//     return (strcmp(name + name_len - suffix_len, WT_UNION_FS_STOP_SUFFIX) == 0);
// }

// /*
//  * __union_fs_is_tombstone --
//  *     Check if the given file is a tombstone.
//  */
// static bool
// __union_fs_is_tombstone(WT_FILE_SYSTEM *fs, WT_SESSION_IMPL *session, const char *name)
// {
//     size_t name_len, suffix_len;

//     WT_UNUSED(fs);
//     WT_UNUSED(session);

//     name_len = strlen(name);
//     suffix_len = strlen(WT_UNION_FS_TOMBSTONE_SUFFIX);
//     if (name_len <= suffix_len)
//         return (false);

//     return (strcmp(name + name_len - suffix_len, WT_UNION_FS_TOMBSTONE_SUFFIX) == 0);
// }

// /*
//  * __union_fs_remove_tombstone --
//  *     Remove a tombstone for the given file.
//  */
// static int
// __union_fs_remove_tombstone(
//   WT_FILE_SYSTEM *fs, WT_SESSION_IMPL *session, const char *name, uint32_t flags)
// {
//     WT_DECL_RET;
//     WT_FILE_SYSTEM *layer_fs;
//     WT_UNION_FS *u;
//     uint32_t remove_flags;
//     char *tombstone;

//     tombstone = NULL;
//     u = (WT_UNION_FS *)fs;

//     layer_fs = &u->destination.file_system;
//     remove_flags = 0;
//     if (LF_ISSET(WT_FS_DURABLE | WT_FS_OPEN_DURABLE))
//         remove_flags |= WT_FS_OPEN_DURABLE;

//     WT_ERR(__union_fs_tombstone(fs, session, name, &tombstone));
//     WT_ERR(layer_fs->fs_remove(layer_fs, &session->iface, tombstone, remove_flags));

// err:
//     __wt_free(session, tombstone);
//     return (0);
// }

static int
__union_fs_has_file(
  WT_UNION_FS_LAYER *layer, WT_SESSION_IMPL *session, const char *name, bool *existsp)
{
    WT_DECL_RET;
    char *path;

    path = NULL;

    /* If the start layer has a stop file, we are finished. */
    // if (start_layer_excl != WT_UNION_FS_TOP) {
    //     layer_fs = __union_fs_top(u)->file_system;
    //     WT_ERR(__union_fs_filename(__union_fs_top(u), session, name, &path));
    //     WT_ERR(__union_fs_stop(fs, session, path, &stop));
    //     WT_ERR(layer_fs->fs_exist(layer_fs, &session->iface, stop, &exist));
    //     if (exist)
    //         WT_ERR(WT_NOTFOUND);
    //     __wt_free(session, path);
    // }
    WT_ERR(__union_fs_filename(layer, session, name, &path));
    // 8(__union_fs_tombstone(fs, session, path, &tombstone));
    //
    // /* Check the tombstone. */
    // WT_ERR(layer_fs->fs_exist(layer_fs, &session->iface, tombstone, &exist));
    // if (exist) {
    //  *existp = false;
    // if (indexp != NULL)
    //  *indexp = (size_t)i;
    // break;
    // }

    /* Check for the file itself. */
    WT_ERR(layer->file_system->fs_exist(layer->file_system, &session->iface, path, existsp));
err:
    __wt_free(session, path);

    return (ret);
}

/*
 * __union_fs_find_layer --
 *     Find a layer for the given file. Return the index of the layer and whether the layer contains
 *     the file (exists = true) or the tombstone (exists = false). Start searching at the given
 *     layer index - 1; use WT_UNION_FS_TOP to indicate starting at the top.
 */
static int
__union_fs_find_layer(
  WT_FILE_SYSTEM *fs, WT_SESSION_IMPL *session, const char *name, LAYER *whichp, bool *existp)
{
    WT_UNION_FS *u;

    WT_ASSERT(session, existp != NULL);

    *existp = false;
    u = (WT_UNION_FS *)fs;

    WT_RET(__union_fs_has_file(&u->destination, session, name, existp));
    if (*existp) {
        /* The file exists in the destination we don't need to look any further. */
        if (whichp != NULL)
            *whichp = DESTINATION;
        return (0);
    }

    WT_RET(__union_fs_has_file(&u->source, session, name, existp));
    if (*existp) {
        /* The file exists in the source we don't need to look any further. */
        if (whichp != NULL)
            *whichp = SOURCE;
    } else
        /* We didn't find the file. */
        return (WT_NOTFOUND);

    return (0);
}

/*
 * __union_fs_reconcile --
 *     Reconcile a file in the top layer with all data from the layers below. The file must be
 *     already open and writable.
 */
static int
__union_fs_reconcile(WT_UNION_FS *u, WT_SESSION_IMPL *session, WT_UNION_FS_FH *fh)
{
    WT_DECL_RET;
    size_t len;
    char *buf;

    WT_UNUSED(u);
    WT_UNUSED(len);
    WT_UNUSED(len);
    WT_UNUSED(fh);

    buf = NULL;
    len = 0;

    // /* Make sure the file is open in the top layer and not read-only. */
    // WT_ASSERT(session, fh->num_layers > 0 && __union_fs_is_top(u, fh->layers[0]->layer->index));
    // WT_ASSERT(session, !fh->readonly);

    // // XXX TODO Actually reconcile.
    // WT_ASSERT(session, fh->num_layers == 1); // XXX Not implemented for > 1 layer!

    // /* Create a stop file, because we have reconciled it. */
    // WT_ERR(__union_fs_create_stop(&u->iface, session, fh->iface.name, 0));

    if (0) {
        goto err;
err:
        __wt_free(session, buf);
    }
    return (ret);
}

/*
 * __union_fs_reconcile_by_name --
 *     Reconcile a file in the top layer with all data from the layers below. The file must not be
 *     already open.
 */
static int
__union_fs_reconcile_by_name(WT_UNION_FS *u, WT_SESSION_IMPL *session, const char *name)
{
    WT_DECL_RET;
    WT_UNION_FS_FH *fh;

    fh = NULL;

    WT_ERR(u->iface.fs_open_file((WT_FILE_SYSTEM *)u, (WT_SESSION *)session, name,
      WT_FS_OPEN_FILE_TYPE_DATA, 0, (WT_FILE_HANDLE **)&fh));
    WT_ERR(__union_fs_reconcile(u, session, fh));

err:
    if (fh != NULL)
        WT_TRET(fh->iface.close((WT_FILE_HANDLE *)fh, (WT_SESSION *)session));
    return (ret);
}
/*
 * __union_fs_directory_list_ext --
 *     Get a list of files from a directory.
 */
static int
__union_fs_directory_list_ext(WT_FILE_SYSTEM *fs, WT_SESSION_IMPL *session, const char *directory,
  const char *prefix, char ***dirlistp, uint32_t *countp, bool single)
{
    WT_DECL_RET;
    WT_UNION_FS *union_fs;
    WT_UNION_FS_LAYER *layer;
    size_t entries_alloc_size;
    uint32_t i, j, layer_num_entries, num_entries, ret_num_entries, reuse;
    char **entries, **layer_entries, *path, **ret_entries;
    bool found;

    WT_UNUSED(single);

    union_fs = (WT_UNION_FS *)fs;

    entries = NULL;
    entries_alloc_size = 0;
    layer_entries = NULL;
    layer = NULL;
    layer_num_entries = 0;
    num_entries = 0;
    path = NULL;
    ret_entries = NULL;
    ret_num_entries = 0;

    layer = &union_fs->destination;

    for (int z = 0; z < 2; z++) {
        if (z == 1) {
            layer = &union_fs->source;
        }
        WT_ERR(__union_fs_filename(layer, session, directory, &path));
        WT_ERR(layer->file_system->fs_directory_list(
          layer->file_system, &session->iface, path, prefix, &layer_entries, &layer_num_entries));
        __wt_free(session, path);

        /* Process the entries from the layer, properly handling tombstones. */
        for (i = 0; i < layer_num_entries; i++) {

            // /* Exclude all stop markers. */
            // if (__union_fs_is_stop(fs, session, layer_entries[i]))
            //     continue;

            // if (__union_fs_is_tombstone(fs, session, layer_entries[i])) {
            //     /* Find the tombstone in a list and mark it as removed. */
            //     l = strlen(layer_entries[i]) - strlen(WT_UNION_FS_TOMBSTONE_SUFFIX);
            //     for (j = 0; j < num_entries; j++) {
            //         if (strncmp(entries[j], layer_entries[i], l) == 0 && strlen(entries[j]) == l) {
            //             entries[j][0] = '\0';
            //             break;
            //         }
            //     }
            // } else {
                /* See if the entry is in the list. Remember any slots that can be reused. */
                found = false;
                reuse = (uint32_t)-1;
                for (j = 0; j < num_entries; j++) {
                    if (strcmp(entries[j], layer_entries[i]) == 0) {
                        found = true;
                        break;
                    }
                    if (reuse != 0 && entries[j][0] == '\0')
                        reuse = (uint32_t)-1;
                }

                if (!found) {
                    if (reuse != (uint32_t)-1) {
                        __wt_free(session, entries[reuse]);
                        WT_ERR(__wt_strdup(session, layer_entries[i], &entries[reuse]));
                    } else {
                        WT_ERR(__wt_realloc_def(
                          session, &entries_alloc_size, num_entries + 1, &entries));
                        WT_ERR(__wt_strdup(session, layer_entries[i], &entries[num_entries]));
                        ++num_entries;
                    }
                }
            // }
        }

        /* Clean up the listing from the layer. */
        WT_ERR(layer->file_system->fs_directory_list_free(
          layer->file_system, &session->iface, layer_entries, layer_num_entries));
        layer_entries = NULL;
    }

    /* Consolidate the array, omitting any removed entries. */
    for (i = 0; i < num_entries; i++)
        if (entries[i][0] != '\0')
            ret_num_entries++;
    if (ret_num_entries == num_entries) {
        ret_entries = entries;
        entries = NULL;
    } else if (ret_num_entries == 0)
        ret_entries = NULL;
    else {
        WT_ERR(__wt_calloc_def(session, ret_num_entries, &ret_entries));
        for (i = 0, j = 0; i < num_entries; i++)
            if (entries[i][0] != '\0') {
                ret_entries[j++] = entries[i];
                entries[i] = NULL;
            }
        WT_ASSERT(session, j == ret_num_entries);
    }

    *dirlistp = ret_entries;
    *countp = ret_num_entries;

err:
    if (layer->file_system != NULL && layer_entries != NULL)
        WT_TRET(layer->file_system->fs_directory_list_free(
          layer->file_system, &session->iface, layer_entries, layer_num_entries));
    if (entries != NULL)
        WT_TRET(fs->fs_directory_list_free(fs, &session->iface, entries, num_entries));
    __wt_free(session, path);
    return (ret);
}

/*
 * __union_fs_directory_list --
 *     Get a list of files from a directory.
 */
static int
__union_fs_directory_list(WT_FILE_SYSTEM *fs, WT_SESSION *wt_session, const char *directory,
  const char *prefix, char ***dirlistp, uint32_t *countp)
{
    return (__union_fs_directory_list_ext(
      fs, (WT_SESSION_IMPL *)wt_session, directory, prefix, dirlistp, countp, false));
}

/*
 * __union_fs_directory_list --
 *     Get one file from a directory.
 */
static int
__union_fs_directory_list_single(WT_FILE_SYSTEM *fs, WT_SESSION *wt_session, const char *directory,
  const char *prefix, char ***dirlistp, uint32_t *countp)
{
    return (__union_fs_directory_list_ext(
      fs, (WT_SESSION_IMPL *)wt_session, directory, prefix, dirlistp, countp, true));
}

/*
 * __union_fs_directory_list_free --
 *     Free memory returned by the directory listing.
 */
static int
__union_fs_directory_list_free(
  WT_FILE_SYSTEM *fs, WT_SESSION *wt_session, char **dirlist, uint32_t count)
{
    WT_SESSION_IMPL *session;

    WT_UNUSED(fs);

    session = (WT_SESSION_IMPL *)wt_session;

    if (dirlist == NULL)
        return (0);

    while (count > 0)
        __wt_free(session, dirlist[--count]);
    __wt_free(session, dirlist);

    return (0);
}

/*
 * __union_fs_exist --
 *     Return if the file exists.
 */
static int
__union_fs_exist(WT_FILE_SYSTEM *fs, WT_SESSION *wt_session, const char *name, bool *existp)
{
    WT_DECL_RET;
    WT_SESSION_IMPL *session;
    bool exist;

    session = (WT_SESSION_IMPL *)wt_session;

    exist = false;
    WT_RET_NOTFOUND_OK(__union_fs_find_layer(fs, session, name, NULL, &exist));

    *existp = ret == 0 && exist;
    return (0);
}

static int
__union_fs_open_file(WT_FILE_SYSTEM *fs, WT_SESSION *wt_session, const char *name,
  WT_FS_OPEN_FILE_TYPE file_type, uint32_t flags, WT_FILE_HANDLE **file_handlep);


/*
 * __union_fs_file_close --
 *     Close the file.
 */
static int
__union_fs_file_close(WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session)
{
    WT_SESSION_IMPL *session;
    WT_UNION_FS_FH *fh;

    fh = (WT_UNION_FS_FH *)file_handle;
    session = (WT_SESSION_IMPL *)wt_session;

    // FIXME - To remove
    {
        // DEBUG CODE - The second time we've closed WiredTiger.wt in union_fs.cpp there 
        // will be multiple writes to it. Open it again to confirm we've recovered all 
        // the extents properly
        static int numm = 0;
        if(strcmp(file_handle->name, "./WiredTiger.wt") == 0) {
            if(++numm == 2) {
                __union_fs_open_file(fh->iface.file_system, wt_session, file_handle->name, 
                    fh->file_type, 0, (WT_FILE_HANDLE **)&fh);
            }
        }
    }

    fh->destination.fh->close(fh->destination.fh, wt_session);
    //TODO: Reconcile the file?
    // TODO: Free the extent linked list.

    if (fh->source != NULL) /* It's possible that we never opened the file in the source. */
        fh->source->close(fh->source, wt_session);
    __wt_free(session, fh->iface.name);
    __wt_free(session, fh);

    return (0);
}

/*
 * __union_fs_file_lock --
 *     Lock/unlock a file.
 */
static int
__union_fs_file_lock(WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session, bool lock)
{
    WT_UNION_FS_FH *fh;

    fh = (WT_UNION_FS_FH *)file_handle;
    return (fh->destination.fh->fh_lock(fh->destination.fh, wt_session, lock));
}

// This may be easier with a partial_start, partial_end, partial macro.
/*
 * Given a read or a write must fall either in an existing extend or on the edge of it the two
 * choices are NONE or FULL.
 *
 * This only works by assuming that the block manager is the only thing that will use this and that
 * it only reads and writes full blocks. If that changes this code will unceremoniously fall over.
 */
typedef enum { NONE, FULL} RW_SERVICE_LEVEL;

#define EXTENT_END(ext) (ext)->off + (wt_off_t)(ext)->len

static RW_SERVICE_LEVEL
__dest_can_service_rw(WT_UNION_FS_FH *union_fh, WT_SESSION_IMPL *session, wt_off_t offset, size_t len) {
    WT_UNION_FS_FH_SINGLE_LAYER *dest_fh;
    WT_UNION_ALLOC_LIST *alloc;
    wt_off_t rw_end;

    WT_UNUSED(session);

    // Walk the extend list until extent offset + size > read offset + size.
    dest_fh = &union_fh->destination;
    rw_end = offset + (wt_off_t)len;


    /* Nothing has been written into the destination yet. */
    if (dest_fh->allocation_list == NULL)
        return (NONE);

    alloc = dest_fh->allocation_list;
    while (alloc != NULL) {
        /* The read is in this allocation. */
        if (offset >= alloc->off && rw_end <= EXTENT_END(alloc)) {
            __wt_verbose_debug3(session, WT_VERB_FILEOPS, "CAN SERVICE %s: Full match", union_fh->iface.name);
            return (FULL);
        }
        alloc = alloc->next;
    }

    return (NONE);
}

/*
 * __union_merge_with_next_extents --
 *
 * Merge the current extent with the following extent(s) if they overlap.
 */
static int __union_merge_with_next_extents(WT_SESSION_IMPL *session, WT_UNION_ALLOC_LIST *extent) {
    WT_UNION_ALLOC_LIST *next_extent;
    wt_off_t new_len;

    next_extent = extent->next;
    WT_ASSERT_ALWAYS(session, next_extent != NULL, "Attempting to merge with NULL!");
    WT_ASSERT_ALWAYS(session, EXTENT_END(extent) >= next_extent->off, 
        "Attempting to merge non-overlapping extents! extent_end = %ld, next->off = %ld", EXTENT_END(extent), next_extent->off);

    extent->next = next_extent->next;
    new_len = EXTENT_END(next_extent) - extent->off; // TODO check for off by one
    // FIXME - A lot of pointless typecasting. Consider dropping wt_off_t in favour of size_t.
    extent->len = (size_t)WT_MAX((wt_off_t)extent->len, new_len);
    __wt_free(session, next_extent);

    // Run it again in case we also overlap with the N+1 extent
    // TODO - loop instead of recursion.
    if(extent->next != NULL && EXTENT_END(extent) >= extent->next->off) {
        WT_RET(__union_merge_with_next_extents(session, extent));
    }
    return (0);
}

/*
 * __dest_update_alloc_list_write --
 *     Track that we wrote something. This will require creating new extends, growing existing ones
 *     and merging overlapping extents.
 */
static int
__dest_update_alloc_list_write(WT_UNION_FS_FH *union_fh, WT_SESSION_IMPL *session, wt_off_t offset, size_t len)
{
    WT_UNION_ALLOC_LIST *alloc, *prev, *new;
    RW_SERVICE_LEVEL sl;
    prev = alloc = new = NULL;

    __wt_verbose_debug2(session, WT_VERB_FILEOPS, "UPDATE EXTENT %s: %ld, %lu, %p", union_fh->iface.name, offset, len, union_fh->destination.allocation_list);

    sl = __dest_can_service_rw(union_fh, session, offset, len);
    if (sl == FULL) {
        /* The full write was serviced from single extent. */
        return (0);
    } else {
        /* Allocate a new extent or grow an existing one. */
        alloc = union_fh->destination.allocation_list;
        while (alloc != NULL) {
            /* Find the first extend that the write is before. */
            if (alloc->off == offset) {

                /* If the write fits the current extent just add it */
                if(EXTENT_END(alloc) >= offset + (wt_off_t)len) {
                    __wt_verbose_debug3(session, WT_VERB_FILEOPS, "EXTENT MATCH %s, no work done", union_fh->iface.name);
                    return (0);
                } else {
                    /* Otherwise grow the extent to match the new write. */
                    WT_ASSERT(session, alloc->len < len);
                    alloc->len = len;
                    if(alloc->next != NULL && alloc->next->off <= EXTENT_END(alloc)) {
                        WT_RET(__union_merge_with_next_extents(session, alloc));
                    }
                    return (0);
                }
            }
            if (alloc->off > offset) {
                break;
            }
            prev = alloc;
            alloc = alloc->next;
        }

        if (prev == NULL) {
            // New extent is at the start.
            /* Allocate a new extent. */
            WT_RET(__wt_calloc_one(session, &new));
            new->off = offset;
            new->len = len;
            new->next = alloc;
            union_fh->destination.allocation_list = new;
            return (0);

        } else if (alloc == NULL) {
            // New extent is after.
            if (EXTENT_END(prev) == offset) {
                // Grow prev.
                prev->len += len;
                return (0);
            }
        } else {
            // New extent is in between.
            if (EXTENT_END(prev) == offset) {
                // Grow prev.
                prev->len += len;
                if (prev->next->off <= EXTENT_END(prev)) {
                    __union_merge_with_next_extents(session, prev);
                }
                return (0);
            }
        }
        /* Allocate a new extent. */
        WT_RET(__wt_calloc_one(session, &new));
        new->off = offset;
        new->len = len;
        new->next = alloc;
        prev->next = new;
        if (EXTENT_END(prev) >= new->off) {
            __union_merge_with_next_extents(session, prev);
        }
    }
    return (0);
}
/*
 * __union_fs_file_write --
 *     File write.
 */
static int
__union_fs_file_write(
  WT_FILE_HANDLE *fh, WT_SESSION *wt_session, wt_off_t offset, size_t len, const void *buf)
{
    WT_SESSION_IMPL *session;
    WT_UNION_FS_FH *union_fh;

    union_fh = (WT_UNION_FS_FH *)fh;
    session = (WT_SESSION_IMPL *)wt_session;

    __wt_verbose_debug1(session, WT_VERB_FILEOPS, "WRITE %s: %ld, %zu", fh->name, offset, len);
    WT_RET(union_fh->destination.fh->fh_write(union_fh->destination.fh, wt_session, offset, len, buf));

    WT_RET(__dest_update_alloc_list_write(union_fh, session, offset, len));

    // TODO: I think this is error checking?
    // XXX
    // WT_ERR(__wt_calloc_def(session, len, &b));
    // WT_ERR(l->fh->fh_read(l->fh, wt_session, offset, len, b));
    // WT_ASSERT(session, memcmp(buf, b, len) == 0);

    // // XXX
    // WT_ERR(__wt_calloc_def(session, (size_t)x + 1048576 * 10, &d));
    // WT_ERR(union_fh->iface.fh_read(&union_fh->iface, wt_session, 0, (size_t)x, d));
    // WT_ASSERT(session, memcmp(c, d, (size_t)x) == 0);

    return (0);
}

/*
 * __read_promote --
 *     Write out the contents of a read into the destination. This will be overkill for cases where
 *     a read is performed to service a write. Which is most cases however this is a PoC.
 *
 *     This is somewhat tricky as we need to compute what parts of the read require copying to the
 *     destination, which requires parsing the existing extent lists in the destination and finding
 *     the gaps to then be filled by N writes.
 *
 *     TODO: Locking needed.
 */
static int
__read_promote(WT_UNION_FS_FH *union_fh, WT_SESSION_IMPL *session, wt_off_t offset, size_t len, char *read) {
    __wt_verbose_debug2(session, WT_VERB_FILEOPS, "    READ PROMOTE %s : %ld, %zu", union_fh->iface.name, offset, len);
    WT_RET(__union_fs_file_write((WT_FILE_HANDLE *)union_fh, (WT_SESSION *)session, offset, len, read));
    return (0);
}

/*
 * __posix_file_read --
 *     File read.
 */
static int
__union_fs_file_read(
  WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session, wt_off_t offset, size_t len, void *buf)
{
    WT_DECL_RET;
    WT_SESSION_IMPL *session;
    WT_UNION_FS_FH *union_fh;
    RW_SERVICE_LEVEL sl;
    char *read_data;

    union_fh = (WT_UNION_FS_FH *)file_handle;
    session = (WT_SESSION_IMPL *)wt_session;
    sl = NONE;

    // XXX We really want to read this faster than one chunk at a time... this is embarrassing.

    __wt_verbose_debug1(session, WT_VERB_FILEOPS, "READ %s : %ld, %zu", file_handle->name, offset, len);

    read_data = (char *)buf;

    sl = __dest_can_service_rw(union_fh, session, offset, len);

    /*
     * TODO: Wiredtiger will read the metadata file after creation but before anything has
     * been written in this case we forward the read to the empty metadata file in the
     * destinaion. Is this correct?
     */
    if (union_fh->source == NULL || sl == FULL) {
        __wt_verbose_debug2(session, WT_VERB_FILEOPS, "    READ FROM DEST (src is NULL? %s)", union_fh->source == NULL ? "YES" : "NO");
        /* Read the full read from the destination. */
        WT_ERR(union_fh->destination.fh->fh_read(union_fh->destination.fh, wt_session, offset, len, read_data));
    } else {
        /* Interestingly you cannot not have a format in verbose. */
         __wt_verbose_debug2(session, WT_VERB_FILEOPS, "    READ FROM %s", "SOURCE");
        /* Read the full read from the source. */
        WT_ERR(union_fh->source->fh_read(union_fh->source, wt_session, offset, len, read_data));
        /* Promote the read */
        WT_ERR(__read_promote(union_fh, session, offset, len, read_data));
    }
err:
    return (ret);
}

/*
 * __union_fs_file_size --
 *     Get the size of a file in bytes, by file handle.
 */
static int
__union_fs_file_size(WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session, wt_off_t *sizep)
{
    WT_UNION_FS_FH *fh;
    wt_off_t destination_size, source_size;

    fh = (WT_UNION_FS_FH *)file_handle;

    WT_RET(fh->destination.fh->fh_size(fh->destination.fh, wt_session, &destination_size));
    if (fh->source != NULL)
        WT_RET(fh->source->fh_size(fh->source, wt_session, &source_size));
    // TODO: This needs fixing somehow.

    *sizep = destination_size > source_size ? destination_size : source_size;
    return (0);
}

/*
 * __union_fs_file_sync --
 *     POSIX fsync. This only sync the destination as the source is readonly.
 */
static int
__union_fs_file_sync(WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session)
{
    WT_UNION_FS_FH *fh;

    fh = (WT_UNION_FS_FH *)file_handle;
    return (fh->destination.fh->fh_sync(fh->destination.fh, wt_session));
}

/*
 * __union_fs_file_read_chunk --
 *     Read a chunk from a file.
 */
// __union_fs_file_read_chunk(
//   WT_UNION_FS_FH *union_fh, WT_SESSION_IMPL *session, size_t chunk_index, void *buf, size_t *lenp)
// {
//     WT_UNION_FS *u;
//     WT_UNION_FS_FH_SINGLE_LAYER *l;
//     size_t read_len, read_offset;

//     u = (WT_UNION_FS *)union_fh->iface.file_system;
//     // l = &union_fh->destination;
//     // if (!__union_fs_chunk_in_layer(l, chunk_index)) {
//     l = &(union_fh->source);
//     WT_ASSERT(session, __union_fs_chunk_in_layer(l, chunk_index));
//     // }
//     read_offset = chunk_index * u->chunk_size;
//     read_len = u->chunk_size;
//     if (read_offset >= l->size) {
//         if (lenp != NULL)
//             *lenp = read_len;
//         return (0);
//     }
//     if (read_offset + read_len > l->size)
//         read_len = l->size - read_offset;
//     WT_ASSERT(session, read_len > 0);
//     if (lenp != NULL)
//         *lenp = read_len;
//     return (l->fh->fh_read(l->fh, (WT_SESSION *)session, (wt_off_t)read_offset, read_len, buf));
// }



/*
 * __union_fs_open_in_source --
 *     Open a file handle in the source.
 */
static int
__union_fs_open_in_source(WT_UNION_FS *u, WT_SESSION_IMPL *session, WT_UNION_FS_FH *union_fh, uint32_t flags) {
    WT_DECL_RET;
    WT_FILE_HANDLE *fh;

    char *path;

    path = NULL;

    // Clear the create flag. TODO: Can we assert something here?
    FLD_CLR(flags, WT_FS_OPEN_CREATE);


    /* Open the file in the layer. */
    WT_ERR(__union_fs_filename(&u->source, session, union_fh->iface.name, &path));
    WT_ERR(u->source.file_system->fs_open_file(
      u->source.file_system, (WT_SESSION *)session, path, union_fh->file_type, flags, &fh));

    union_fh->source = fh;

err:
    __wt_free(session, path);
    return (ret);
}
static int fd_is_valid(int fd)
{
    return fcntl(fd, F_GETFD) != -1 || errno != EBADF;
}
#include <unistd.h>
/*
 * __union_build_extents_from_dest_file_lseek --
 *     When opening a file from destination create its existing extent list from the file system information.
 *     Any holes in the extent list are data that hasn't been copied from source yet.
 */
static void __union_build_extents_from_dest_file_lseek(WT_SESSION_IMPL *session, char *filename, WT_UNION_FS_FH *union_fh) {
    wt_off_t start_offset, next_offset;
    int fd;
    bool another;

    WT_UNUSED(union_fh);
    fd = open(filename, O_RDONLY);
    start_offset = next_offset = 0;
    another = false;

    __wt_verbose_debug2(session, WT_VERB_FILEOPS, "File: %s", filename);
    __wt_verbose_debug2(session, WT_VERB_FILEOPS, "    len: %llu", (unsigned long long) union_fh->destination.size);
    WT_ASSERT(session, fd_is_valid(fd));

    while ((next_offset = lseek(fd, next_offset, SEEK_HOLE)) != -1) {
        WT_ASSERT(session, next_offset > start_offset);
        __wt_verbose_debug1(session, WT_VERB_FILEOPS, "File: %s, has %shole at offset %ld", filename, another ? "another " : "", next_offset);
        __dest_update_alloc_list_write(union_fh, session, start_offset, (size_t)(next_offset - start_offset));
        // We now need to seek data to find the end of the hole
        start_offset = lseek(fd, next_offset, SEEK_DATA);
        if (start_offset == -1) {
            // This should mean theres no more data.
            WT_ASSERT(session, errno == ENXIO);
        }
        another = true;
    }
    if (next_offset == -1)
        WT_ASSERT(session, errno == ENXIO);

    close(fd);
}


#include <linux/fiemap.h> // struct fiemap
#include <linux/fs.h>     // FS_IOS_FIEMAP
#include <sys/ioctl.h>    // ioctl()      

// FIXME - a lot of hacky casting in here
// FIXME - Add WT_ERR/WT_RET logic
// FIXME - Use __wt_* funcs instead of malloc and strcmp

/*
 * __union_build_extents_from_dest_file_ioctl --
 *     When opening a file from destination create its existing extent list from the file system information.
 *     Any holes in the extent list are data that hasn't been copied from source yet.
 */
static void __union_build_extents_from_dest_file_ioctl(WT_SESSION_IMPL *session, char *filename, WT_UNION_FS_FH *union_fh) {
    struct fiemap *fiemap_fetch_extent_num, *fiemap;
    unsigned int num_extents;
    int fd;
    bool fiemap_last_flag_set;
    
    struct fiemap_extent *fiemap_extent_list;

    fd = open(filename, O_RDONLY);

    /////////////////////////////////////////////////////////////////////////////
    // 1. Run fiemap ioctl with fm_extent_count set to zero. Instead of returning 
    // a list of extents it'll tell us how many extents are in the file.
    // TODO - how to confirm this doesn't change after we've read the value?
    //        I think we're safe as no one else would be writing to the file(???)
    /////////////////////////////////////////////////////////////////////////////
    fiemap_fetch_extent_num = malloc(sizeof(struct fiemap));
    memset(fiemap_fetch_extent_num, 0, sizeof(struct fiemap));

    fiemap_fetch_extent_num->fm_start = 0; // Start from the beginning of the file
    fiemap_fetch_extent_num->fm_length = (unsigned long long)union_fh->destination.size; // Get the entire file
    fiemap_fetch_extent_num->fm_flags = 0; // TODO - flags?
    fiemap_fetch_extent_num->fm_extent_count = 0; // Set zero. this means the call returns the number of fiemap_extent_list in the file

    ioctl(fd, FS_IOC_FIEMAP, fiemap_fetch_extent_num);
    num_extents = fiemap_fetch_extent_num->fm_mapped_extents;

    __wt_verbose_debug2(session, WT_VERB_FILEOPS, "File: %s", filename);
    __wt_verbose_debug2(session, WT_VERB_FILEOPS, "    len: %llu", (unsigned long long) union_fh->destination.size);
    __wt_verbose_debug2(session, WT_VERB_FILEOPS, "    num_extents: %u", num_extents);

    /////////////////////////////////////////////////////////////////////////////
    // 2. Now we know the number of extents in the file call fiemap again with this number and parse the extents
    /////////////////////////////////////////////////////////////////////////////

    fiemap = malloc(sizeof(struct fiemap) + sizeof(struct fiemap_extent) * (num_extents));
    memset(fiemap, 0, sizeof(struct fiemap));

    fiemap->fm_start = 0; // Start from the beginning of the file
    fiemap->fm_length = (unsigned long long) union_fh->destination.size; // Get the entire file
    fiemap->fm_flags = 0; // TODO - flags?
    fiemap->fm_extent_count = num_extents;

    ioctl(fd, FS_IOC_FIEMAP, fiemap);


    /////////////////////////////////////////////////////////////////////////////
    // 3. Using the returned fiemap info re-build the extent lists 
    // TODO - Using a custom extent list instead of the WT impl?
    /////////////////////////////////////////////////////////////////////////////

    // fiemap_extent_list lives in an array at the end of the fiemap
    fiemap_extent_list = (struct fiemap_extent *)(fiemap + 1); 
    for (unsigned int i = 0; i < num_extents; i++) {

        __wt_verbose_debug2(session, WT_VERB_FILEOPS, ">       Extent %u: offset: %llu, length: %llu, flags: %u",
               i,
               (unsigned long long)fiemap_extent_list[i].fe_logical,
               (unsigned long long)fiemap_extent_list[i].fe_length,
               fiemap_extent_list[i].fe_flags);

        // Special handling for the final extent
        if (i == num_extents - 1) {
            unsigned long long file_len_per_extents;
            unsigned long long actual_file_len;

            __wt_verbose_debug2(session, WT_VERB_FILEOPS, "DBG LAST EXTENT %d", 1);

            // Make sure the "last extent" flag is set
            fiemap_last_flag_set = (fiemap_extent_list[i].fe_flags & FIEMAP_EXTENT_LAST) != 0;
            WT_ASSERT_ALWAYS(session, fiemap_last_flag_set == true, "Last extent but FIEMAP_EXTENT_LAST not set!");
            
            // TODO - ioctl fiemap tracks disk usage which is *not* the same as the file and 
            // can extend past the end of the file.
            // For example this is the results of stat TOP/WiredTiger.basecfg.wt
            //       File: TOP/WiredTiger.basecfg.set
            //       Size: 316             Blocks: 8          IO Block: 4096   regular file
            // We only set 316 bytes but there's 32KB of backing space, and the extent map tells us we've 
            // used a whole 4KB block.
            // The following code truncates the final extent in the list to match the actual file size. 
            // TODO - think about this. Feels ugly
            file_len_per_extents = fiemap_extent_list[i].fe_logical + fiemap_extent_list[i].fe_length;
            actual_file_len = (unsigned long long)union_fh->destination.size;
            __wt_verbose_debug2(session, WT_VERB_FILEOPS, "extent_len = %llu, actual_len = %llu", file_len_per_extents, actual_file_len);

            if(file_len_per_extents > actual_file_len) {
                fiemap_extent_list[i].fe_length = actual_file_len - fiemap_extent_list[i].fe_logical;
                __wt_verbose_debug2(session, WT_VERB_FILEOPS, "TRUNCING to %llu", (unsigned long long)fiemap_extent_list[i].fe_length);
            }
        }

        __dest_update_alloc_list_write(union_fh, session,
          (wt_off_t)fiemap_extent_list[i].fe_logical, (size_t)fiemap_extent_list[i].fe_length);
    }
    free(fiemap);
    free(fiemap_fetch_extent_num);

    close(fd);
}

/*
 * __union_fs_open_in_destination --
 *     Open a file handle.
 */
static int
__union_fs_open_in_destination(WT_UNION_FS *u, WT_SESSION_IMPL *session, WT_UNION_FS_FH *union_fh, uint32_t flags, bool create)
{
    WT_DECL_RET;
    WT_FILE_HANDLE *fh;
    WT_UNION_FS_FH_SINGLE_LAYER *union_fh_single_layer;
    wt_off_t size;
    char *path;

    path = NULL;
    union_fh_single_layer = &union_fh->destination;

    if (create)
        flags |= WT_FS_OPEN_CREATE;

    WT_CLEAR(*union_fh_single_layer);

    /* Open the file in the layer. */
    WT_ERR(__union_fs_filename(&u->destination, session, union_fh->iface.name, &path));
    WT_ERR(u->destination.file_system->fs_open_file(
      u->destination.file_system, (WT_SESSION *)session, path, union_fh->file_type, flags, &fh));
    union_fh_single_layer->fh = fh;

    /* Get the map of the file. */
    WT_ASSERT(session, union_fh->file_type != WT_FS_OPEN_FILE_TYPE_DIRECTORY);
    WT_ERR(fh->fh_size(fh, (WT_SESSION *)session, &size));
    union_fh_single_layer->size = (wt_off_t)size;

    if (strcmp(union_fh->iface.name, "./WiredTiger.wt") == 0) {
        printf("Metadata\n");
    }

    __union_build_extents_from_dest_file_lseek(session, path, union_fh);
    WT_UNUSED(__union_build_extents_from_dest_file_ioctl);

err:
    __wt_free(session, path);
    return (ret);
}

/*
 * __union_fs_open_file --
 *     Open a union file handle. This will: - If the file exists in the source, open it in both. -
 *     If it doesn't exist it'll only open it in the destination.
 */
static int
__union_fs_open_file(WT_FILE_SYSTEM *fs, WT_SESSION *wt_session, const char *name,
  WT_FS_OPEN_FILE_TYPE file_type, uint32_t flags, WT_FILE_HANDLE **file_handlep)
{
    WT_DECL_RET;
    WT_SESSION_IMPL *session;
    WT_UNION_FS *u;
    WT_UNION_FS_FH *fh;
    LAYER which;
    bool exist, have_tombstone, readonly;

    session = (WT_SESSION_IMPL *)wt_session;
    u = (WT_UNION_FS *)fs;

    exist = false;
    fh = NULL;
    have_tombstone = false;
    WT_UNUSED(have_tombstone);
    readonly = LF_ISSET(WT_FS_OPEN_READONLY);
    WT_UNUSED(readonly);
    WT_UNUSED(which);

    // XXX Handle WT_FS_OPEN_FILE_TYPE_DIRECTORY

    /* Set up the file handle. */
    WT_ERR(__wt_calloc_one(session, &fh));
    WT_ERR(__wt_strdup(session, name, &fh->iface.name));
    fh->iface.file_system = fs;
    fh->file_type = file_type;

    // XXX Handle the exclusive flag and other flags

    /* Open it in the destination layer. */
    WT_ERR_NOTFOUND_OK(__union_fs_has_file(&u->destination, session, name, &exist), true);
    WT_ERR(__union_fs_open_in_destination(u, session, fh, flags, !exist));

    /* If it exists in the source, open it. */
    WT_ERR_NOTFOUND_OK(__union_fs_has_file(&u->source, session, name, &exist), true);
    if (exist)
        WT_ERR(__union_fs_open_in_source(u, session, fh, flags));

    /* If there is a tombstone, delete it. */
    // if (have_tombstone && __union_fs_is_top(u, layer_index))
    // WT_ERR(__union_fs_remove_tombstone(fs, session, fh->layers[0]->fh->name, flags));
    // XXX Initialize the top layer file if it's actually new

    //     // WT_ERR_NOTFOUND_OK(
    //       __union_fs_find_layer(fs, session, name, __union_fs_top(fs)->index, &layer_index,
    //       &exist), true);
    //     if (ret == WT_NOTFOUND) {
    //         exist = false;
    //         ret = 0;
    //     }
    // }

    /* Initialize the jump table. */
    fh->iface.close = __union_fs_file_close;
    fh->iface.fh_lock = __union_fs_file_lock;
    fh->iface.fh_read = __union_fs_file_read;
    fh->iface.fh_size = __union_fs_file_size;
    fh->iface.fh_sync = __union_fs_file_sync;
    fh->iface.fh_write = __union_fs_file_write;

    *file_handlep = (WT_FILE_HANDLE *)fh;

    if (0) {
err:
        if (fh != NULL)
            __union_fs_file_close((WT_FILE_HANDLE *)fh, wt_session);
    }
    return (ret);
}

/*
 * TODO: __union_fs_remove --
 *     Remove a file. We can only delete from the destination directory anyway.
 */
static int
__union_fs_remove(WT_FILE_SYSTEM *fs, WT_SESSION *wt_session, const char *name, uint32_t flags)
{
    WT_DECL_RET;
    WT_FILE_SYSTEM *layer_fs;
    WT_SESSION_IMPL *session;
    WT_UNION_FS *u;
    char *path;
    bool exist;
    LAYER layer;

    session = (WT_SESSION_IMPL *)wt_session;
    u = (WT_UNION_FS *)fs;
    WT_UNUSED(u);
    WT_UNUSED(layer_fs);
    WT_UNUSED(flags);

    exist = false;
    path = NULL;

    WT_RET_NOTFOUND_OK(__union_fs_find_layer(fs, session, name, &layer, &exist));
    if (ret == WT_NOTFOUND || !exist)
        return (0);

    // This needs more thought.
    // /* If the file exists at the top layer, delete it. */
    // if (__union_fs_is_top(u, layer_index)) {
    //     layer_fs = u->layers[layer_index]->file_system;
    //     WT_ERR(__union_fs_filename(u->layers[layer_index], session, name, &path));
    //     WT_ERR(layer_fs->fs_remove(layer_fs, wt_session, path, flags));
    // } else
    //     /* Otherwise create a tombstone in the top layer. */
    //     WT_RET(__union_fs_create_tombstone(fs, session, name, flags));

    __wt_free(session, path);
    return (0);
}

/*
 * __union_fs_rename --
 *     Rename a file.
 */
static int
__union_fs_rename(
  WT_FILE_SYSTEM *fs, WT_SESSION *wt_session, const char *from, const char *to, uint32_t flags)
{
    WT_DECL_RET;
    WT_FILE_SYSTEM *layer_fs;
    WT_SESSION_IMPL *session;
    WT_UNION_FS *u;
    LAYER which;
    char *path_from, *path_to;
    bool exist;

    session = (WT_SESSION_IMPL *)wt_session;
    u = (WT_UNION_FS *)fs;

    WT_UNUSED(exist);
    WT_UNUSED(path_to);
    WT_UNUSED(path_from);
    WT_UNUSED(u);
    WT_UNUSED(session);
    WT_UNUSED(layer_fs);
    WT_UNUSED(ret);
    WT_UNUSED(from);
    WT_UNUSED(to);
    WT_UNUSED(flags);

    exist = false;
    path_from = NULL;
    path_to = NULL;

    // XXX The logic below isn't atomic.
    // TODO: Do we need this? Yes lol....

    /* Reconcile the differences between layers. */
    WT_ERR(__union_fs_reconcile_by_name(u, session, from));

    WT_RET_NOTFOUND_OK(__union_fs_find_layer(fs, session, from, &which, &exist));
    if (ret == WT_NOTFOUND || !exist)
        return (ENOENT);

    /* If the file is the top layer, rename it and leave a tombstone behind. */
    if (which == DESTINATION) {
        layer_fs = u->destination.file_system;
        WT_ERR(__union_fs_filename(&u->destination, session, from, &path_from));
        WT_ERR(__union_fs_filename(&u->destination, session, to, &path_to));
        WT_ERR(layer_fs->fs_rename(layer_fs, wt_session, path_from, path_to, flags));
        __wt_free(session, path_from);
        __wt_free(session, path_to);

        // /* Create a stop file for the target. */
        // WT_ERR(__union_fs_create_stop(fs, session, to, flags));

        // /* Create a tombstone for the source. */
        // WT_ERR(__union_fs_create_tombstone(fs, session, from, flags));

        /* See if there is a file in a lower level. */
        // TODO: This?
        // WT_ERR_NOTFOUND_OK(
        //     __union_fs_find_layer(fs, session, from, layer_index, &layer_index, &exist), true);
        // if (ret == WT_NOTFOUND || !exist)
        //     WT_ERR(ENOENT);
    }

err:
    __wt_free(session, path_from);
    __wt_free(session, path_to);
    return (ret);
}

/*
 * __union_fs_size --
 *     Get the size of a file in bytes, by file name.
 */
static int
__union_fs_size(WT_FILE_SYSTEM *fs, WT_SESSION *wt_session, const char *name, wt_off_t *sizep)
{
    WT_DECL_RET;
    WT_SESSION_IMPL *session;
    WT_UNION_FS *u;
    WT_UNION_FS_LAYER *union_layer;
    LAYER which;
    bool exist;
    char *path;

    session = (WT_SESSION_IMPL *)wt_session;
    u = (WT_UNION_FS *)fs;

    exist = false;
    path = NULL;

    // TODO: This will need to work across layers

    WT_RET_NOTFOUND_OK(__union_fs_find_layer(fs, session, name, &which, &exist));
    if (ret == WT_NOTFOUND || !exist)
        return (ENOENT);

    if (which == DESTINATION) {
        // TODO: Should any file ever exist in destination that doesn't exist in source? Not
        // considering drops at this stage.
        union_layer = &u->destination;
    } else {
        union_layer = &u->source;
    }
    WT_RET(__union_fs_filename(union_layer, session, name, &path));
    ret = union_layer->file_system->fs_size(union_layer->file_system, wt_session, path, sizep);

    __wt_free(session, path);

    return (ret);
}

/*
 * __union_fs_terminate --
 *     Terminate the file system.
 */
static int
__union_fs_terminate(WT_FILE_SYSTEM *fs, WT_SESSION *wt_session)
{
    WT_DECL_RET;
    WT_SESSION_IMPL *session;
    WT_UNION_FS *u;
    int r;

    session = (WT_SESSION_IMPL *)wt_session;
    u = (WT_UNION_FS *)fs;

    /* Source layer. */
    if (u->source.file_system->terminate != NULL) {
        r = u->source.file_system->terminate(u->source.file_system, wt_session);
        if (r != 0 && ret == 0)
            ret = r;
    }
    // __wt_free(session, u->source.home);

    /* Destination layer. */
    if (u->destination.file_system->terminate != NULL) {
        r = u->destination.file_system->terminate(u->destination.file_system, wt_session);
        if (r != 0 && ret == 0)
            ret = r;
    }
    // __wt_free(session, u->destination.home);
    __wt_free(session, u);
    return (ret);
}

/*
 * __wt_os_union_fs --
 *     Initialize a union file system configuration.
 */
int
__wt_os_union_fs(
  WT_SESSION_IMPL *session, const char *source, const char *destination, WT_FILE_SYSTEM *fs)
{
    WT_CONNECTION_IMPL *conn;
    WT_UNION_FS *file_system;

    conn = S2C(session);

    WT_RET(__wt_calloc_one(session, &file_system));

    file_system->destination.which = DESTINATION;
    file_system->source.which = SOURCE;
    file_system->destination.file_system = fs;
    file_system->source.file_system = fs;

    /* Initialize the FS jump table. */
    file_system->iface.fs_directory_list = __union_fs_directory_list;
    file_system->iface.fs_directory_list_single = __union_fs_directory_list_single;
    file_system->iface.fs_directory_list_free = __union_fs_directory_list_free;
    file_system->iface.fs_exist = __union_fs_exist;
    file_system->iface.fs_open_file = __union_fs_open_file;
    file_system->iface.fs_remove = __union_fs_remove;
    file_system->iface.fs_rename = __union_fs_rename;
    file_system->iface.fs_size = __union_fs_size;
    file_system->iface.terminate = __union_fs_terminate;

    /* Switch it into place. */
    conn->file_system = (WT_FILE_SYSTEM *)file_system;

    file_system->destination.home = destination;
    file_system->source.home = source;

    // WT_UNUSED(__union_fs_remove_tombstone);
    // WT_UNUSED(__union_fs_create_tombstone);
    // WT_UNUSED(__union_fs_create_stop);
    // WT_UNUSED(__union_fs_stop);
    return (0);
}
