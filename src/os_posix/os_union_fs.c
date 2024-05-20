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

#define WT_UNION_FS_STOP_SUFFIX ".stop"
#define WT_UNION_FS_TOP ((size_t) - 1)
#define WT_UNION_FS_TOMBSTONE_SUFFIX ".deleted"

// XXX The same file can be opened only once - otherwise different threads don't see each other's
//     changes.

/*
 * __union_fs_chunk_in_layer --
 *     Check if the given chunk is in the given layer.
 */
#define __union_fs_chunk_in_layer(l, chunk_index) \
    (l->complete || l->chunks == NULL || (chunk_index < l->num_chunks && l->chunks[chunk_index]))

/*
 * __union_fs_is_top --
 *     Check if the index is the top layer.
 */
#define __union_fs_is_top(fs, index) (((WT_UNION_FS *)fs)->num_layers == index + 1)

/*
 * __union_fs_top --
 *     Get the top layer. It assumes that there is at least one layer.
 */
#define __union_fs_top(fs) (((WT_UNION_FS *)fs)->layers[((WT_UNION_FS *)fs)->num_layers - 1])

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

/*
 * __union_fs_marker --
 *     Generate a name of a marker file.
 */
static int
__union_fs_marker(
  WT_FILE_SYSTEM *fs, WT_SESSION_IMPL *session, const char *name, const char *marker, char **out)
{
    size_t p, suffix_len;

    WT_UNUSED(fs);

    p = strlen(name);
    suffix_len = strlen(marker);

    WT_RET(__wt_malloc(session, p + suffix_len + 1, out));
    memcpy(*out, name, p);
    memcpy(*out + p, marker, suffix_len + 1);
    return (0);
}

/*
 * __union_fs_stop --
 *     Generate a name of a stop marker.
 */
static int
__union_fs_stop(WT_FILE_SYSTEM *fs, WT_SESSION_IMPL *session, const char *name, char **out)
{
    return (__union_fs_marker(fs, session, name, WT_UNION_FS_STOP_SUFFIX, out));
}

/*
 * __union_fs_tombstone --
 *     Generate a name of a tombstone.
 */
static int
__union_fs_tombstone(
  WT_FILE_SYSTEM *fs, WT_SESSION_IMPL *session, const char *name, char **tombstonep)
{
    return (__union_fs_marker(fs, session, name, WT_UNION_FS_TOMBSTONE_SUFFIX, tombstonep));
}

/*
 * __union_fs_create_marker --
 *     Create a marker file for the given file.
 */
static int
__union_fs_create_marker(WT_FILE_SYSTEM *fs, WT_SESSION_IMPL *session, const char *name,
  const char *marker, uint32_t flags)
{
    WT_DECL_RET;
    WT_FILE_HANDLE *fh;
    WT_FILE_SYSTEM *layer_fs;
    WT_UNION_FS *u;
    char *path, *path_marker;
    uint32_t open_flags;

    fh = NULL;
    path = NULL;
    path_marker = NULL;
    u = (WT_UNION_FS *)fs;

    WT_ERR(__union_fs_filename(__union_fs_top(u), session, name, &path));

    layer_fs = __union_fs_top(u)->file_system;
    open_flags = WT_FS_OPEN_CREATE;
    if (LF_ISSET(WT_FS_DURABLE | WT_FS_OPEN_DURABLE))
        FLD_SET(open_flags, WT_FS_OPEN_DURABLE);

    WT_ERR(__union_fs_marker(fs, session, path, marker, &path_marker));
    WT_ERR(layer_fs->fs_open_file(
      layer_fs, &session->iface, path_marker, WT_FS_OPEN_FILE_TYPE_DATA, open_flags, &fh));
    WT_ERR(fh->close(fh, &session->iface));

err:
    __wt_free(session, path);
    __wt_free(session, path_marker);
    return (0);
}

/*
 * __union_fs_create_stop --
 *     Create a stop marker for the given file.
 */
static int
__union_fs_create_stop(
  WT_FILE_SYSTEM *fs, WT_SESSION_IMPL *session, const char *name, uint32_t flags)
{
    return (__union_fs_create_marker(fs, session, name, WT_UNION_FS_STOP_SUFFIX, flags));
}

/*
 * __union_fs_create_tombstone --
 *     Create a tombstone for the given file.
 */
static int
__union_fs_create_tombstone(
  WT_FILE_SYSTEM *fs, WT_SESSION_IMPL *session, const char *name, uint32_t flags)
{
    return (__union_fs_create_marker(fs, session, name, WT_UNION_FS_TOMBSTONE_SUFFIX, flags));
}

/*
 * __union_fs_is_stop --
 *     Check if the given file is a stop file marker.
 */
static bool
__union_fs_is_stop(WT_FILE_SYSTEM *fs, WT_SESSION_IMPL *session, const char *name)
{
    size_t name_len, suffix_len;

    WT_UNUSED(fs);
    WT_UNUSED(session);

    name_len = strlen(name);
    suffix_len = strlen(WT_UNION_FS_STOP_SUFFIX);
    if (name_len <= suffix_len)
        return (false);

    return (strcmp(name + name_len - suffix_len, WT_UNION_FS_STOP_SUFFIX) == 0);
}

/*
 * __union_fs_is_tombstone --
 *     Check if the given file is a tombstone.
 */
static bool
__union_fs_is_tombstone(WT_FILE_SYSTEM *fs, WT_SESSION_IMPL *session, const char *name)
{
    size_t name_len, suffix_len;

    WT_UNUSED(fs);
    WT_UNUSED(session);

    name_len = strlen(name);
    suffix_len = strlen(WT_UNION_FS_TOMBSTONE_SUFFIX);
    if (name_len <= suffix_len)
        return (false);

    return (strcmp(name + name_len - suffix_len, WT_UNION_FS_TOMBSTONE_SUFFIX) == 0);
}

/*
 * __union_fs_remove_tombstone --
 *     Remove a tombstone for the given file.
 */
static int
__union_fs_remove_tombstone(
  WT_FILE_SYSTEM *fs, WT_SESSION_IMPL *session, const char *name, uint32_t flags)
{
    WT_DECL_RET;
    WT_FILE_SYSTEM *layer_fs;
    WT_UNION_FS *u;
    char *tombstone;
    uint32_t remove_flags;

    tombstone = NULL;
    u = (WT_UNION_FS *)fs;

    layer_fs = __union_fs_top(u)->file_system;
    remove_flags = 0;
    if (LF_ISSET(WT_FS_DURABLE | WT_FS_OPEN_DURABLE))
        remove_flags |= WT_FS_OPEN_DURABLE;

    WT_ERR(__union_fs_tombstone(fs, session, name, &tombstone));
    WT_ERR(layer_fs->fs_remove(layer_fs, &session->iface, tombstone, remove_flags));

err:
    __wt_free(session, tombstone);
    return (0);
}

/*
 * __union_fs_find_layer --
 *     Find a layer for the given file. Return the index of the layer and whether the layer contains
 *     the file (exists = true) or the tombstone (exists = false). Start searching at the given
 *     layer index - 1; use WT_UNION_FS_TOP to indicate starting at the top.
 */
static int
__union_fs_find_layer(WT_FILE_SYSTEM *fs, WT_SESSION_IMPL *session, const char *name,
  size_t start_layer_excl, size_t *indexp, bool *existp)
{
    WT_DECL_RET;
    WT_FILE_SYSTEM *layer_fs;
    WT_UNION_FS *u;
    bool exist;
    char *path, *stop, *tombstone;
    ssize_t i;

    exist = false;
    path = NULL;
    stop = NULL;
    tombstone = NULL;
    u = (WT_UNION_FS *)fs;

    WT_ASSERT(session, u->num_layers > 0);

    /* If the start layer has a stop file, we are finished. */
    if (start_layer_excl != WT_UNION_FS_TOP) {
        layer_fs = __union_fs_top(u)->file_system;
        WT_ERR(__union_fs_filename(__union_fs_top(u), session, name, &path));
        WT_ERR(__union_fs_stop(fs, session, path, &stop));
        WT_ERR(layer_fs->fs_exist(layer_fs, &session->iface, stop, &exist));
        if (exist)
            WT_ERR(WT_NOTFOUND);
        __wt_free(session, path);
    }

    /* Check one layer at a time. */
    for (i = (ssize_t)(start_layer_excl == WT_UNION_FS_TOP ? u->num_layers : start_layer_excl) - 1;
         i >= 0; i--) {
        layer_fs = u->layers[i]->file_system;

        WT_ERR(__union_fs_filename(u->layers[i], session, name, &path));
        WT_ERR(__union_fs_tombstone(fs, session, path, &tombstone));

        /* Check the tombstone. */
        WT_ERR(layer_fs->fs_exist(layer_fs, &session->iface, tombstone, &exist));
        if (exist) {
            *existp = false;
            if (indexp != NULL)
                *indexp = (size_t)i;
            break;
        }

        /* Check for the file itself. */
        WT_ERR(layer_fs->fs_exist(layer_fs, &session->iface, path, &exist));
        if (exist) {
            *existp = true;
            if (indexp != NULL)
                *indexp = (size_t)i;
            break;
        }

        __wt_free(session, path);
        __wt_free(session, tombstone);
    }

    /* We didn't find the file or the tombstone. */
    if (!exist)
        ret = WT_NOTFOUND;

err:
    __wt_free(session, path);
    __wt_free(session, stop);
    __wt_free(session, tombstone);
    return (ret);
}

/*
 * __union_fs_reconcile --
 *     Reconcile a file in the top layer with all data from the layers below. The file must be
 *     already open and writable.
 */
static int
__union_fs_reconcile(WT_UNION_FS *u, WT_SESSION_IMPL *session, WT_FILE_HANDLE_UNION_FS *fh)
{
    WT_DECL_RET;
    size_t len;
    char *buf;

    WT_UNUSED(u);

    buf = NULL;
    len = 0;

    /* Make sure the file is open in the top layer and not read-only. */
    WT_ASSERT(session, fh->num_layers > 0 && __union_fs_is_top(u, fh->layers[0]->layer->index));
    WT_ASSERT(session, !fh->readonly);

    // XXX TODO Actually reconcile.
    WT_ASSERT(session, fh->num_layers == 1); // XXX Not implemented for > 1 layer!

    /* Create a stop file, because we have reconciled it. */
    WT_ERR(__union_fs_create_stop(&u->iface, session, fh->iface.name, 0));

    if (0) {
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
    WT_FILE_HANDLE_UNION_FS *fh;

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
 * __union_fs_add_layer --
 *     Add a layer to the union file system.
 */
static int
__union_fs_add_layer(
  WT_FILE_SYSTEM *fs, WT_SESSION *wt_session, WT_FILE_SYSTEM *new_layer, const char *home)
{
    WT_DECL_RET;
    WT_SESSION_IMPL *session;
    WT_UNION_FS *u;
    WT_UNION_FS_LAYER *layer;

    session = (WT_SESSION_IMPL *)wt_session;
    u = (WT_UNION_FS *)fs;

    layer = NULL;

    WT_ASSERT(session, new_layer != NULL);
    if (u->num_layers >= u->max_layers)
        return (E2BIG);

    /* Initialize the layer. */
    WT_ERR(__wt_calloc_one(session, &layer));
    WT_ERR(__wt_strdup(session, home, &layer->home));
    layer->file_system = new_layer;
    layer->index = u->num_layers;

    /* Add the layer. */
    u->layers[u->num_layers] = layer;
    u->num_layers++;

    if (0) {
err:
        __wt_free(session, layer);
    }
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
    WT_FILE_SYSTEM *layer_fs;
    WT_UNION_FS *u;
    bool found;
    char **entries, **layer_entries, *path, **ret_entries;
    size_t entries_alloc_size, l, layer_index;
    uint32_t i, j, layer_num_entries, num_entries, ret_num_entries, reuse;

    WT_UNUSED(single);

    u = (WT_UNION_FS *)fs;

    entries = NULL;
    entries_alloc_size = 0;
    layer_entries = NULL;
    layer_fs = NULL;
    layer_num_entries = 0;
    num_entries = 0;
    path = NULL;
    ret_entries = NULL;
    ret_num_entries = 0;

    for (layer_index = 0; layer_index < u->num_layers; layer_index++) {
        layer_fs = u->layers[layer_index]->file_system;
        WT_ERR(__union_fs_filename(u->layers[layer_index], session, directory, &path));
        WT_ERR(layer_fs->fs_directory_list(
          layer_fs, &session->iface, path, prefix, &layer_entries, &layer_num_entries));
        __wt_free(session, path);

        /* Process the entries from the layer, properly handling tombstones. */
        for (i = 0; i < layer_num_entries; i++) {

            /* Exclude all stop markers. */
            if (__union_fs_is_stop(fs, session, layer_entries[i]))
                continue;

            if (__union_fs_is_tombstone(fs, session, layer_entries[i])) {
                /* Find the tombstone in a list and mark it as removed. */
                l = strlen(layer_entries[i]) - strlen(WT_UNION_FS_TOMBSTONE_SUFFIX);
                for (j = 0; j < num_entries; j++) {
                    if (strncmp(entries[j], layer_entries[i], l) == 0 && strlen(entries[j]) == l) {
                        entries[j][0] = '\0';
                        break;
                    }
                }
            } else {
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
            }
        }

        /* Clean up the listing from the layer. */
        WT_ERR(layer_fs->fs_directory_list_free(
          layer_fs, &session->iface, layer_entries, layer_num_entries));
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
    if (layer_fs != NULL && layer_entries != NULL)
        WT_TRET(layer_fs->fs_directory_list_free(
          layer_fs, &session->iface, layer_entries, layer_num_entries));
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
    WT_RET_NOTFOUND_OK(__union_fs_find_layer(fs, session, name, WT_UNION_FS_TOP, NULL, &exist));

    *existp = ret == 0 && exist;
    return (0);
}

/*
 * __union_fs_file_close --
 *     Close the file.
 */
static int
__union_fs_file_close(WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session)
{
    WT_DECL_RET;
    WT_FILE_HANDLE_UNION_FS *fh;
    WT_SESSION_IMPL *session;
    size_t i;

    fh = (WT_FILE_HANDLE_UNION_FS *)file_handle;
    session = (WT_SESSION_IMPL *)wt_session;

    /* Close each layer. */
    for (i = 0; i < fh->num_layers; i++) {
        if (fh->layers[i] == NULL)
            continue;
        if (fh->layers[i]->fh != NULL)
            WT_TRET(fh->layers[i]->fh->close(fh->layers[i]->fh, &session->iface));
        __wt_free(session, fh->layers[i]->chunks);
        __wt_free(session, fh->layers[i]);
    }

    __wt_free(session, fh->iface.name);
    __wt_free(session, fh->layers);
    __wt_free(session, fh);
    return (ret);
}

/*
 * __union_fs_file_lock --
 *     Lock/unlock a file.
 */
static int
__union_fs_file_lock(WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session, bool lock)
{
    WT_FILE_HANDLE_UNION_FS *fh;

    fh = (WT_FILE_HANDLE_UNION_FS *)file_handle;
    return (fh->layers[0]->fh->fh_lock(fh->layers[0]->fh, wt_session, lock));
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
    WT_FILE_HANDLE_UNION_FS *fh;
    WT_SESSION_IMPL *session;
    WT_UNION_FS *u;
    WT_FILE_HANDLE_UNION_FS_LAYER *l;
    bool found;
    char *dest;
    size_t chunk_from, chunk_from_inner, chunk_index, chunk_to, chunk_to_inner, i, read_len,
      read_offset;

    fh = (WT_FILE_HANDLE_UNION_FS *)file_handle;
    session = (WT_SESSION_IMPL *)wt_session;
    u = (WT_UNION_FS *)fh->iface.file_system;

    chunk_from = (size_t)offset / u->chunk_size;
    chunk_from_inner = (size_t)offset % u->chunk_size;
    chunk_to = ((size_t)offset + len) / u->chunk_size;
    chunk_to_inner = ((size_t)offset + len) % u->chunk_size;
    if (chunk_to_inner != 0)
        ++chunk_to;

    // XXX We really want to read this faster than one chunk at a time... this is embarrassing.

     fprintf(stderr, "READ %s : %ld %zu\n", file_handle->name, offset, len);

    dest = (char *)buf;

    for (chunk_index = chunk_from; chunk_index < chunk_to; chunk_index++) {
        found = false;
        for (i = 0; i < fh->num_layers; i++) {
            l = fh->layers[i];
            if (__union_fs_chunk_in_layer(l, chunk_index)) {
                read_offset = chunk_index * u->chunk_size;
                read_len = u->chunk_size;
                if (chunk_index == chunk_from) {
                    read_offset += chunk_from_inner;
                    read_len -= chunk_from_inner;
                }

                // Reading past EOF?
                WT_ASSERT(session, !(i > 0 && read_offset >= l->size));

                if (read_offset + read_len > (size_t)offset + len)
                    read_len = (size_t)offset + len - read_offset;
                WT_ASSERT(session, read_len > 0);
                found = true;
                 fprintf(stderr, "READ %s :   << [%zu] %zu %ld %zu\n", file_handle->name,
                   chunk_index, i, (wt_off_t)read_offset, read_len);
                WT_ERR(l->fh->fh_read(l->fh, wt_session, (wt_off_t)read_offset, read_len, dest));
                dest += read_len;
                break;
            }
        }
        WT_ASSERT(session, found);
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
    WT_FILE_HANDLE_UNION_FS *fh;
    size_t i;
    wt_off_t layer_size, size;

    fh = (WT_FILE_HANDLE_UNION_FS *)file_handle;

    size = 0;
    for (i = 0; i < fh->num_layers; i++) {
        WT_RET(fh->layers[i]->fh->fh_size(fh->layers[i]->fh, wt_session, &layer_size));
        if (layer_size > size)
            size = layer_size;
    }
    // fprintf(stderr, "SIZE %s : %zu\n", file_handle->name, size);

    *sizep = size;
    return (0);
}

/*
 * __union_fs_file_sync --
 *     POSIX fsync.
 */
static int
__union_fs_file_sync(WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session)
{
    WT_FILE_HANDLE_UNION_FS *fh;

    fh = (WT_FILE_HANDLE_UNION_FS *)file_handle;
    return (fh->layers[0]->fh->fh_sync(fh->layers[0]->fh, wt_session));
}

/*
 * __union_fs_file_read_chunk --
 *     Read a chunk from a file.
 */
static int
__union_fs_file_read_chunk(WT_FILE_HANDLE_UNION_FS *fh, WT_SESSION_IMPL *session,
  size_t first_layer_index, size_t chunk_index, void *buf, size_t *lenp)
{
    WT_FILE_HANDLE_UNION_FS_LAYER *l;
    WT_UNION_FS *u;
    size_t i, read_len, read_offset;

    u = (WT_UNION_FS *)fh->iface.file_system;

    for (i = first_layer_index; i < fh->num_layers; i++) {
        l = fh->layers[i];
        if (__union_fs_chunk_in_layer(l, chunk_index)) {
            read_offset = chunk_index * u->chunk_size;
            read_len = u->chunk_size;
            if (read_offset >= l->size) {
                if (lenp != NULL)
                    *lenp = read_len;
                return (0);
            }
            if (read_offset + read_len > l->size)
                read_len = l->size - read_offset;
            WT_ASSERT(session, read_len > 0);
            if (lenp != NULL)
                *lenp = read_len;
            return (
              l->fh->fh_read(l->fh, (WT_SESSION *)session, (wt_off_t)read_offset, read_len, buf));
        }
    }

    WT_ASSERT(session, false);
}

/*
 * __union_fs_file_write --
 *     File write.
 */
static int
__union_fs_file_write(
  WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session, wt_off_t offset, size_t len, const void *buf)
{
    /*WT_FILE_HANDLE_UNION_FS *fh;
    fh = (WT_FILE_HANDLE_UNION_FS *)file_handle;
    return (fh->layers[0]->fh->fh_write(fh->layers[0]->fh, wt_session, offset, len, buf));*/

    WT_DECL_RET;
    WT_FILE_HANDLE_UNION_FS *fh;
    WT_FILE_HANDLE_UNION_FS_LAYER *l;
    WT_SESSION_IMPL *session;
    WT_UNION_FS *u;
    bool found, *new_chunks;
    char *src, *tmp, *write_buf, *b, *c, *d;
    size_t chunk_from, chunk_from_inner, chunk_index, chunk_to, chunk_to_inner, get_from_src,
      index_within_tmp, tmp_len, w, write_len, write_offset;

    wt_off_t x;

    fh = (WT_FILE_HANDLE_UNION_FS *)file_handle;
    session = (WT_SESSION_IMPL *)wt_session;
    u = (WT_UNION_FS *)fh->iface.file_system;

    b = NULL;
    c = NULL;
    d = NULL;
    new_chunks = NULL;
    tmp = NULL;

    // XXX
    WT_ERR(u->iface.fs_size(&u->iface, wt_session, fh->iface.name, &x));
    // fprintf(stderr, "VRFY1 %s : %ld %zu x=%zu\n", file_handle->name, offset, len, x);
    WT_ERR(__wt_calloc_def(session, (size_t)x + 1048576 * 100, &c));
    WT_ERR(fh->iface.fh_read(&fh->iface, wt_session, 0, (size_t)x, c));
    memcpy(c + offset, buf, len);

    chunk_from = (size_t)offset / u->chunk_size;
    chunk_from_inner = (size_t)offset % u->chunk_size;
    chunk_to = ((size_t)offset + len) / u->chunk_size;
    chunk_to_inner = ((size_t)offset + len) % u->chunk_size;
    if (chunk_to_inner != 0)
        ++chunk_to;
    else
        chunk_to_inner = u->chunk_size;

    WT_ERR(__wt_calloc_def(session, u->chunk_size, &tmp));

    // XXX We really want to write faster than one chunk at a time... this is embarrassing.

    src = (char *)buf;
     fprintf(stderr, "WRITE %s : %ld %zu\n", file_handle->name, offset, len);

    for (chunk_index = chunk_from; chunk_index < chunk_to; chunk_index++) {
        found = false;

        write_buf = src;
        write_offset = chunk_index * u->chunk_size;
        write_len = u->chunk_size;

        /* Read in a lower-level chunk. */
        if ((chunk_index == chunk_from && chunk_from_inner != 0) ||
          (chunk_index + 1 == chunk_to && chunk_to_inner != 0)) {

            w = 0;
            if (fh->num_layers > 1) {
                 fprintf(stderr, "WRITE %s :   in [%zu] %zu-%zu\n", file_handle->name,
                 chunk_index, chunk_from, chunk_to);
                WT_ERR(__union_fs_file_read_chunk(fh, session, 1, chunk_index, tmp, &tmp_len));
                if (tmp_len < u->chunk_size) {
                    memset(tmp + tmp_len, 0, u->chunk_size - tmp_len);
                    write_len = tmp_len;
                }
                 fprintf(stderr, "WRITE %s :   ^^ [%zu] %zu-%zu %zu\n", file_handle->name,
                   chunk_index, chunk_from, chunk_to, tmp_len);
            } else {
                write_len = 0;
                if (chunk_index == chunk_from) {
                    write_offset += chunk_from_inner;
                    w = chunk_from_inner;
                }
            }

            get_from_src = u->chunk_size;
            if (chunk_index == chunk_from)
                get_from_src = u->chunk_size - chunk_from_inner;
            if (chunk_index + 1 == chunk_to) {
                if (chunk_index == chunk_from)
                    get_from_src = chunk_to_inner - chunk_from_inner;
                else
                    get_from_src = chunk_to_inner;
            }

            index_within_tmp = chunk_index == chunk_from ? chunk_from_inner : 0;

            if (fh->num_layers > 1) {
                memcpy(tmp + index_within_tmp, src, get_from_src);
                write_buf = tmp;
            }

            src += get_from_src;
            if (index_within_tmp + get_from_src > write_len)
                write_len = index_within_tmp + get_from_src - w;
        } else
            src += write_len;

        l = fh->layers[0];
         fprintf(stderr, "WRITE %s :   >> [%zu] %zu %ld %zu\n", file_handle->name, chunk_index,
           (size_t)0, (wt_off_t)write_offset, write_len);
        WT_ERR(l->fh->fh_write(l->fh, wt_session, (wt_off_t)write_offset, write_len, write_buf));

        if (l->chunks != NULL) {
            if (chunk_index >= l->num_chunks) {
                WT_ERR(__wt_calloc_def(session, chunk_index + 1, &new_chunks));
                memcpy(new_chunks, l->chunks, sizeof(*new_chunks) * l->num_chunks);
                __wt_free(session, l->chunks);
                l->chunks = new_chunks;
                l->chunks_alloc = l->num_chunks = chunk_index + 1;
                new_chunks = NULL;
            }
            l->chunks[chunk_index] = true;
        }
    }

    // XXX
    WT_ERR(__wt_calloc_def(session, len, &b));
    l = fh->layers[0];
    WT_ERR(l->fh->fh_read(l->fh, wt_session, offset, len, b));
    WT_ASSERT(session, memcmp(buf, b, len) == 0);

    // XXX
    WT_ERR(__wt_calloc_def(session, (size_t)x + 1048576 * 10, &d));
    WT_ERR(fh->iface.fh_read(&fh->iface, wt_session, 0, (size_t)x, d));
    //WT_ASSERT(session, memcmp(c, d, (size_t)x) == 0);

err:
    __wt_free(session, b);
    __wt_free(session, c);
    __wt_free(session, d);
    __wt_free(session, new_chunks);
    __wt_free(session, tmp);
    return (ret);
}

/*
 * __union_fs_open_file_layer --
 *     Open a file handle.
 */
static int
__union_fs_open_file_layer(WT_UNION_FS *u, WT_SESSION_IMPL *session, WT_FILE_HANDLE_UNION_FS *fh,
  WT_UNION_FS_LAYER *layer, uint32_t flags, bool top)
{
    WT_DECL_RET;
    WT_FILE_HANDLE *layer_fh;
    WT_FILE_HANDLE_UNION_FS_LAYER *l;
    bool zero;
    char *buf, *path;
    size_t i, j, length, num_chunks;
    uint32_t open_flags;
    wt_off_t offset, size;

    buf = NULL;
    l = NULL;
    path = NULL;

    WT_ERR(__wt_calloc_one(session, &l));
    WT_ERR(__wt_calloc_def(session, u->chunk_size, &buf));

    if (top)
        open_flags = flags | WT_FS_OPEN_CREATE;
    else {
        open_flags = flags | WT_FS_OPEN_READONLY;
        FLD_CLR(open_flags, WT_FS_OPEN_CREATE);
    }

    /* Open the file in the layer. */
    WT_ERR(__union_fs_filename(layer, session, fh->iface.name, &path));
    WT_ERR(layer->file_system->fs_open_file(
      layer->file_system, (WT_SESSION *)session, path, fh->file_type, open_flags, &layer_fh));
    l->fh = layer_fh;
    l->index = fh->num_layers;
    l->layer = layer;

    /* Get the map of the file. */
    if (fh->file_type != WT_FS_OPEN_FILE_TYPE_DIRECTORY) {
        WT_ERR(layer_fh->fh_size(layer_fh, (WT_SESSION *)session, &size));
        l->size = (size_t)size;
        num_chunks = (size_t)size / u->chunk_size;
        if ((size_t)size % u->chunk_size != 0)
            ++num_chunks;
        l->chunks_alloc = num_chunks;
        l->num_chunks = num_chunks;
        WT_ERR(__wt_calloc_def(session, num_chunks == 0 ? 1 : num_chunks, &l->chunks));

        for (i = 0; i < num_chunks; i++) {
            // XXX Use file map instead! This is not good on so many accounts, it's embarrassing.
            offset = (wt_off_t)(i * u->chunk_size);
            length = u->chunk_size;
            if ((size_t)offset + length > (size_t)size) {
                WT_ASSERT(session, size > offset);
                length = (size_t)size - (size_t)offset;
            }
            WT_ASSERT(session, length <= u->chunk_size);
            WT_ERR(layer_fh->fh_read(layer_fh, (WT_SESSION *)session, offset, length, buf));
            zero = true;
            for (j = 0; j < length; j++)
                if (buf[j] != 0) {
                    zero = false;
                    break;
                }
            if (!zero)
                l->chunks[i] = true;
        }
    }

    fh->layers[fh->num_layers] = l;
    fh->num_layers++;

    if (0) {
err:
        if (l != NULL)
            __wt_free(session, l->chunks);
        __wt_free(session, l);
    }

    __wt_free(session, buf);
    __wt_free(session, path);
    return (ret);
}

/*
 * __union_fs_open_file --
 *     Open a file handle.
 */
static int
__union_fs_open_file(WT_FILE_SYSTEM *fs, WT_SESSION *wt_session, const char *name,
  WT_FS_OPEN_FILE_TYPE file_type, uint32_t flags, WT_FILE_HANDLE **file_handlep)
{
    WT_DECL_RET;
    WT_FILE_HANDLE_UNION_FS *fh;
    WT_SESSION_IMPL *session;
    WT_UNION_FS *u;
    bool exist, have_tombstone, readonly;
    size_t layer_index;

    session = (WT_SESSION_IMPL *)wt_session;
    u = (WT_UNION_FS *)fs;

    exist = false;
    fh = NULL;
    have_tombstone = false;
    layer_index = 0;
    readonly = LF_ISSET(WT_FS_OPEN_READONLY);

    // XXX Handle WT_FS_OPEN_FILE_TYPE_DIRECTORY

    /* Find the file - see if it even exists. */
    WT_ERR_NOTFOUND_OK(
      __union_fs_find_layer(fs, session, name, WT_UNION_FS_TOP, &layer_index, &exist), true);
    if (ret == WT_NOTFOUND || !exist) {
        if (readonly)
            return (ENOENT);
        if (ret == 0)
            have_tombstone = true;
        exist = false;
        ret = 0;
    }

    /* Set up the file handle. */
    WT_ERR(__wt_calloc_one(session, &fh));
    WT_ERR(__wt_calloc_def(session, u->num_layers, &fh->layers));
    WT_ERR(__wt_strdup(session, name, &fh->iface.name));
    fh->iface.file_system = fs;
    fh->file_type = file_type;
    fh->readonly = readonly;

    // XXX Handle the exclusive flag and other flags

    /* If the file is writable, open it in the top layer. */
    if (!readonly) {
        WT_ERR(__union_fs_open_file_layer(u, session, fh, __union_fs_top(fs), flags, true));

        /* If there is a tombstone, delete it. */
        if (have_tombstone && __union_fs_is_top(u, layer_index))
            WT_ERR(__union_fs_remove_tombstone(fs, session, fh->layers[0]->fh->name, flags));

        // XXX Initialize the top layer file if it's actually new

        WT_ERR_NOTFOUND_OK(
          __union_fs_find_layer(fs, session, name, __union_fs_top(fs)->index, &layer_index, &exist),
          true);
        if (ret == WT_NOTFOUND) {
            exist = false;
            ret = 0;
        }
    }

    /* Open the file in the other layers. */
    while (exist) {
        WT_ERR(__union_fs_open_file_layer(u, session, fh, u->layers[layer_index], flags, false));

        WT_ERR_NOTFOUND_OK(
          __union_fs_find_layer(fs, session, name, layer_index, &layer_index, &exist), true);
        if (ret == WT_NOTFOUND) {
            exist = false;
            ret = 0;
        }
    }

    /* The most complete layer initialization. */
    WT_ASSERT(session, fh->num_layers > 0);
    fh->layers[fh->num_layers - 1]->complete = true;

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
 * __union_fs_remove --
 *     Remove a file.
 */
static int
__union_fs_remove(WT_FILE_SYSTEM *fs, WT_SESSION *wt_session, const char *name, uint32_t flags)
{
    WT_DECL_RET;
    WT_FILE_SYSTEM *layer_fs;
    WT_SESSION_IMPL *session;
    WT_UNION_FS *u;
    bool exist;
    char *path;
    size_t layer_index;

    session = (WT_SESSION_IMPL *)wt_session;
    u = (WT_UNION_FS *)fs;

    exist = false;
    layer_index = 0;
    path = NULL;

    WT_RET_NOTFOUND_OK(
      __union_fs_find_layer(fs, session, name, WT_UNION_FS_TOP, &layer_index, &exist));
    if (ret == WT_NOTFOUND || !exist)
        return (0);

    /* If the file exists at the top layer, delete it. */
    if (__union_fs_is_top(u, layer_index)) {
        layer_fs = u->layers[layer_index]->file_system;
        WT_ERR(__union_fs_filename(u->layers[layer_index], session, name, &path));
        WT_ERR(layer_fs->fs_remove(layer_fs, wt_session, path, flags));
    } else
        /* Otherwise create a tombstone in the top layer. */
        WT_RET(__union_fs_create_tombstone(fs, session, name, flags));

err:
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
    bool exist;
    char *path_from, *path_to;
    size_t layer_index;

    session = (WT_SESSION_IMPL *)wt_session;
    u = (WT_UNION_FS *)fs;

    exist = false;
    layer_index = 0;
    path_from = NULL;
    path_to = NULL;

    // XXX The logic below isn't atomic.

    /* Reconcile the differences between layers. */
    WT_ERR(__union_fs_reconcile_by_name(u, session, from));

    WT_RET_NOTFOUND_OK(
      __union_fs_find_layer(fs, session, from, WT_UNION_FS_TOP, &layer_index, &exist));
    if (ret == WT_NOTFOUND || !exist)
        return (ENOENT);

    /* If the file is the top layer, rename it and leave a tombstone behind. */
    if (__union_fs_is_top(u, layer_index)) {
        layer_fs = u->layers[layer_index]->file_system;
        WT_ERR(__union_fs_filename(u->layers[layer_index], session, from, &path_from));
        WT_ERR(__union_fs_filename(u->layers[layer_index], session, to, &path_to));
        WT_ERR(layer_fs->fs_rename(layer_fs, wt_session, path_from, path_to, flags));
        __wt_free(session, path_from);
        __wt_free(session, path_to);

        /* Create a stop file for the target. */
        WT_ERR(__union_fs_create_stop(fs, session, to, flags));

        /* Create a tombstone for the source. */
        WT_ERR(__union_fs_create_tombstone(fs, session, from, flags));

        /* See if there is a file in a lower level. */
        WT_ERR_NOTFOUND_OK(
          __union_fs_find_layer(fs, session, from, layer_index, &layer_index, &exist), true);
        if (ret == WT_NOTFOUND || !exist)
            WT_ERR(ENOENT);
    }

err:
    __wt_free(session, path_from);
    __wt_free(session, path_to);
    return (0);
}

/*
 * __union_fs_size --
 *     Get the size of a file in bytes, by file name.
 */
static int
__union_fs_size(WT_FILE_SYSTEM *fs, WT_SESSION *wt_session, const char *name, wt_off_t *sizep)
{
    WT_DECL_RET;
    WT_FILE_SYSTEM *layer_fs;
    WT_SESSION_IMPL *session;
    WT_UNION_FS *u;
    bool exist;
    char *path;
    size_t layer_index;

    session = (WT_SESSION_IMPL *)wt_session;
    u = (WT_UNION_FS *)fs;

    exist = false;
    layer_index = 0;
    path = NULL;

    // XXX This may need to work across layers

    WT_RET_NOTFOUND_OK(
      __union_fs_find_layer(fs, session, name, WT_UNION_FS_TOP, &layer_index, &exist));
    if (ret == WT_NOTFOUND || !exist)
        return (ENOENT);

    WT_RET(__union_fs_filename(u->layers[layer_index], session, name, &path));

    layer_fs = u->layers[layer_index]->file_system;
    ret = layer_fs->fs_size(layer_fs, wt_session, path, sizep);

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
    WT_FILE_SYSTEM *layer_fs;
    WT_SESSION_IMPL *session;
    WT_UNION_FS *u;
    int r;
    ssize_t i;

    session = (WT_SESSION_IMPL *)wt_session;
    u = (WT_UNION_FS *)fs;

    for (i = (ssize_t)u->num_layers - 1; i >= 0; i--) {
        layer_fs = u->layers[i]->file_system;
        if (layer_fs->terminate != NULL) {
            r = layer_fs->terminate(layer_fs, wt_session);
            if (r != 0 && ret == 0)
                ret = r;
        }
        __wt_free(session, u->layers[i]->home);
        __wt_free(session, u->layers[i]);
    }

    __wt_free(session, u->layers);
    __wt_free(session, u);
    return (ret);
}

/*
 * __wt_os_union_fs --
 *     Initialize a union file system configuration.
 */
int
__wt_os_union_fs(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_UNION_FS *file_system;

    conn = S2C(session);

    WT_RET(__wt_calloc_one(session, &file_system));

    /* Initialize the layers. */
    file_system->max_layers = 4;
    WT_ERR(__wt_calloc(
      session, file_system->max_layers, sizeof(WT_UNION_FS_LAYER), &file_system->layers));

    /* Initialize the union operations. */
    file_system->add_layer = __union_fs_add_layer;
    file_system->chunk_size = 4096; // XXX Should be higher once recovery is implemented

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

    if (0) {
err:
        __wt_free(session, file_system->layers);
        __wt_free(session, file_system);
    }
    return (ret);
}
