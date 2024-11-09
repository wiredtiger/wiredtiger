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
#define WT_UNION_FS_TOMBSTONE_SUFFIX ".deleted"

// XXX The same file can be opened only once - otherwise different threads don't see each other's
//     changes.

/*
 * __union_fs_chunk_in_layer --
 *     Check if the given chunk is in the given layer.
 *
 * TODO: I deleted a nonsensical l->chunks == NULL check here, figure out why it was here.
 */
#define __union_fs_chunk_in_layer(l, chunk_index) \
    (l->complete || (chunk_index < l->num_chunks && l->chunks[chunk_index]))

/*
 * __union_fs_top --
 *     Get the top layer. It assumes that there is at least one layer.
 */
#define __union_fs_top(fs) (&(((WT_UNION_FS *)fs)->destination))

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
    uint32_t open_flags;
    char *path, *path_marker;

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
    uint32_t remove_flags;
    char *tombstone;

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
    WT_UNION_FS_LAYER *layer;
    WT_UNION_FS *union_fs;
    size_t entries_alloc_size, l;
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

/*
 * __union_fs_file_close --
 *     Close the file.
 */
static int
__union_fs_file_close(WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session)
{
    WT_UNION_FS_FH *fh;
    WT_SESSION_IMPL *session;

    fh = (WT_UNION_FS_FH *)file_handle;
    session = (WT_SESSION_IMPL *)wt_session;

    /* Close each layer. */
    fh->source.fh->close(fh->source.fh, wt_session);
    fh->destination.fh->close(fh->destination.fh, wt_session);
    __wt_free(session, fh->source.chunks);
    __wt_free(session, fh->destination.chunks);
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
    return (fh->source.fh->fh_lock(fh->source.fh, wt_session, lock));
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
    WT_UNION_FS_FH *union_fh;
    WT_UNION_FS_FH_SINGLE_LAYER *l;
    WT_SESSION_IMPL *session;
    WT_UNION_FS *u;
    char *dest;
    size_t chunk_from, chunk_from_inner, chunk_index, chunk_to, chunk_to_inner, read_len,
      read_offset;

    union_fh = (WT_UNION_FS_FH *)file_handle;
    session = (WT_SESSION_IMPL *)wt_session;
    u = (WT_UNION_FS *)union_fh->iface.file_system;

    chunk_from = (size_t)offset / u->chunk_size;
    chunk_from_inner = (size_t)offset % u->chunk_size;
    chunk_to = ((size_t)offset + len) / u->chunk_size;
    chunk_to_inner = ((size_t)offset + len) % u->chunk_size;
    if (chunk_to_inner != 0)
        ++chunk_to;

    // XXX We really want to read this faster than one chunk at a time... this is embarrassing.

    //  fprintf(stderr, "READ %s : %ld %zu\n", file_handle->name, offset, len);

    dest = (char *)buf;

    for (chunk_index = chunk_from; chunk_index < chunk_to; chunk_index++) {
        l = &union_fh->destination;
        if (!__union_fs_chunk_in_layer(l, chunk_index)) {
            l = &(union_fh->source);
            WT_ASSERT(session, __union_fs_chunk_in_layer(l, chunk_index));
        }

        read_offset = chunk_index * u->chunk_size;
        read_len = u->chunk_size;
        if (chunk_index == chunk_from) {
            read_offset += chunk_from_inner;
            read_len -= chunk_from_inner;
        }
        // Reading past EOF? TODO: Fix this assert.
        //WT_ASSERT(session, !(i > 0 && read_offset >= l->size));

        if (read_offset + read_len > (size_t)offset + len)
            read_len = (size_t)offset + len - read_offset;
        WT_ASSERT(session, read_len > 0);
        //  fprintf(stderr, "READ %s :   << [%zu] %zu %ld %zu\n", file_handle->name,
        //    chunk_index, i, (wt_off_t)read_offset, read_len);
        WT_ERR(l->fh->fh_read(l->fh, wt_session, (wt_off_t)read_offset, read_len, dest));
        dest += read_len;
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
    WT_RET(fh->source.fh->fh_size(fh->source.fh, wt_session, &source_size));
    // fprintf(stderr, "SIZE %s : %zu\n", file_handle->name, size);

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
static int
__union_fs_file_read_chunk(WT_UNION_FS_FH *union_fh, WT_SESSION_IMPL *session, size_t chunk_index, void *buf, size_t *lenp)
{
    WT_UNION_FS_FH_SINGLE_LAYER *l;
    WT_UNION_FS *u;
    size_t read_len, read_offset;

    u = (WT_UNION_FS *)union_fh->iface.file_system;
    l = &union_fh->destination;
    if (!__union_fs_chunk_in_layer(l, chunk_index)) {
        l = &(union_fh->source);
        WT_ASSERT(session, __union_fs_chunk_in_layer(l, chunk_index));
    }
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
    return (l->fh->fh_read(l->fh, (WT_SESSION *)session, (wt_off_t)read_offset, read_len, buf));
}

/*
 * __union_fs_file_write --
 *     File write.
 */
static int
__union_fs_file_write(
  WT_FILE_HANDLE *fh, WT_SESSION *wt_session, wt_off_t offset, size_t len, const void *buf)
{
    WT_DECL_RET;
    WT_UNION_FS_FH *union_fh;
    WT_UNION_FS_FH_SINGLE_LAYER *l;
    WT_SESSION_IMPL *session;
    WT_UNION_FS *u;
    bool found, *new_chunks;
    char *src, *tmp, *write_buf, *b, *c, *d;
    size_t chunk_from, chunk_from_inner, chunk_index, chunk_to, chunk_to_inner, get_from_src,
      index_within_tmp, tmp_len, w, write_len, write_offset;

    wt_off_t x;

    WT_UNUSED(found);
    union_fh = (WT_UNION_FS_FH *)fh;
    session = (WT_SESSION_IMPL *)wt_session;
    u = (WT_UNION_FS *)union_fh->iface.file_system;

    b = NULL;
    c = NULL;
    d = NULL;
    new_chunks = NULL;
    tmp = NULL;

    // XXX
    WT_ERR(u->iface.fs_size(&u->iface, wt_session, union_fh->iface.name, &x));
    // fprintf(stderr, "VRFY1 %s : %ld %zu x=%zu\n", fh->name, offset, len, x);
    WT_ERR(__wt_calloc_def(session, (size_t)x + 1048576 * 100, &c));
    WT_ERR(union_fh->iface.fh_read(&union_fh->iface, wt_session, 0, (size_t)x, c));
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
    // TODO: Implement this.

    src = (char *)buf;
    //  fprintf(stderr, "WRITE %s : %ld %zu\n", fh->name, offset, len);
    l = &union_fh->destination;
    for (chunk_index = chunk_from; chunk_index < chunk_to; chunk_index++) {
        found = false;

        write_buf = src;
        write_offset = chunk_index * u->chunk_size;
        write_len = u->chunk_size;
        // TODO: Should there be a chunk_in_layer check here? That way we write to the top layer if
        // it's there?
        /* Read in the chunk. If it is lower level it will need to be moved up. */
        if ((chunk_index == chunk_from && chunk_from_inner != 0) ||
          (chunk_index + 1 == chunk_to && chunk_to_inner != 0)) {
            w = 0;
            fprintf(stderr, "WRITE %s :   in [%zu] %zu-%zu\n", fh->name, chunk_index, chunk_from, chunk_to);
            WT_ERR(__union_fs_file_read_chunk(union_fh, session, chunk_index, tmp, &tmp_len));
            if (tmp_len < u->chunk_size) {
                memset(tmp + tmp_len, 0, u->chunk_size - tmp_len);
                write_len = tmp_len;
            }
            fprintf(stderr, "WRITE %s :   ^^ [%zu] %zu-%zu %zu\n", fh->name,
              chunk_index, chunk_from, chunk_to, tmp_len);


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
            memcpy(tmp + index_within_tmp, src, get_from_src);
            write_buf = tmp;

            src += get_from_src;
            if (index_within_tmp + get_from_src > write_len)
                write_len = index_within_tmp + get_from_src - w;
        } else
            src += write_len;


        //  fprintf(stderr, "WRITE %s :   >> [%zu] %zu %ld %zu\n", fh->name, chunk_index,
        //    (size_t)0, (wt_off_t)write_offset, write_len);
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


    // TODO: I think this is error checking?
    // XXX
    WT_ERR(__wt_calloc_def(session, len, &b));
    WT_ERR(l->fh->fh_read(l->fh, wt_session, offset, len, b));
    WT_ASSERT(session, memcmp(buf, b, len) == 0);

    // XXX
    WT_ERR(__wt_calloc_def(session, (size_t)x + 1048576 * 10, &d));
    WT_ERR(union_fh->iface.fh_read(&union_fh->iface, wt_session, 0, (size_t)x, d));
    // WT_ASSERT(session, memcmp(c, d, (size_t)x) == 0);

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
__union_fs_open_file_layer(WT_UNION_FS *u, WT_SESSION_IMPL *session, WT_UNION_FS_FH *union_fh,
  WT_UNION_FS_LAYER *union_layer, uint32_t flags, bool top)
{
    WT_DECL_RET;
    WT_FILE_HANDLE *fh;
    WT_UNION_FS_FH_SINGLE_LAYER *union_fh_single_layer;
    wt_off_t offset, size;
    size_t i, j, length, num_chunks;
    uint32_t open_flags;
    char *buf, *path;
    bool zero;

    buf = NULL;
    union_fh_single_layer = NULL;
    path = NULL;

    WT_ERR(__wt_calloc_def(session, u->chunk_size, &buf));

    if (top){
        union_fh_single_layer = &union_fh->destination;
        open_flags = flags | WT_FS_OPEN_CREATE;
    } else {
        union_fh_single_layer = &union_fh->source;
        open_flags = flags | WT_FS_OPEN_READONLY;
        FLD_CLR(open_flags, WT_FS_OPEN_CREATE);
    }

    WT_CLEAR(union_fh_single_layer);

    /* Open the file in the layer. */
    WT_ERR(__union_fs_filename(union_layer, session, union_fh->iface.name, &path));
    WT_ERR(union_layer->file_system->fs_open_file(
      union_layer->file_system, (WT_SESSION *)session, path, union_fh->file_type, open_flags, &fh));
    union_fh_single_layer->fh = fh;
    union_fh_single_layer->which = union_layer->which;
    union_fh_single_layer->layer = union_layer;

    /* Get the map of the file. */
    if (union_fh->file_type != WT_FS_OPEN_FILE_TYPE_DIRECTORY) {
        WT_ERR(fh->fh_size(fh, (WT_SESSION *)session, &size));
        union_fh_single_layer->size = (size_t)size;
        num_chunks = (size_t)size / u->chunk_size;
        if ((size_t)size % u->chunk_size != 0)
            ++num_chunks;
        union_fh_single_layer->chunks_alloc = num_chunks;
        union_fh_single_layer->num_chunks = num_chunks;
        WT_ERR(__wt_calloc_def(session, num_chunks == 0 ? 1 : num_chunks, &union_fh_single_layer->chunks));

        for (i = 0; i < num_chunks; i++) {
            // XXX Use file map instead! This is not good on so many accounts, it's embarrassing.
            offset = (wt_off_t)(i * u->chunk_size);
            length = u->chunk_size;
            if ((size_t)offset + length > (size_t)size) {
                WT_ASSERT(session, size > offset);
                length = (size_t)size - (size_t)offset;
            }
            WT_ASSERT(session, length <= u->chunk_size);
            WT_ERR(fh->fh_read(fh, (WT_SESSION *)session, offset, length, buf));
            zero = true;
            for (j = 0; j < length; j++)
                if (buf[j] != 0) {
                    zero = false;
                    break;
                }
            if (!zero)
                union_fh_single_layer->chunks[i] = true;
        }
    }

    if (0) {
err:
        __wt_free(session, union_fh_single_layer->chunks);
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
    WT_UNION_FS_FH *fh;
    WT_SESSION_IMPL *session;
    WT_UNION_FS *u;
    LAYER which;
    bool exist, have_tombstone, readonly;

    session = (WT_SESSION_IMPL *)wt_session;
    u = (WT_UNION_FS *)fs;

    exist = false;
    fh = NULL;
    have_tombstone = false;
    WT_UNUSED(have_tombstone);
    readonly = LF_ISSET(WT_FS_OPEN_READONLY);

    // XXX Handle WT_FS_OPEN_FILE_TYPE_DIRECTORY

    /* Find the file - see if it even exists. */
    WT_ERR_NOTFOUND_OK(__union_fs_find_layer(fs, session, name, &which, &exist), true);
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
    WT_ERR(__wt_strdup(session, name, &fh->iface.name));
    fh->iface.file_system = fs;
    fh->file_type = file_type;

    // XXX Handle the exclusive flag and other flags

    /* Open it in both destination layers. */
    WT_ERR(__union_fs_open_file_layer(u, session, fh, &u->destination, flags, true));
    WT_ERR(__union_fs_open_file_layer(u, session, fh, &u->source, flags, false));

        /* If there is a tombstone, delete it. */
    // if (have_tombstone && __union_fs_is_top(u, layer_index))
            // WT_ERR(__union_fs_remove_tombstone(fs, session, fh->layers[0]->fh->name, flags));
        // XXX Initialize the top layer file if it's actually new

    //     // WT_ERR_NOTFOUND_OK(
    //       __union_fs_find_layer(fs, session, name, __union_fs_top(fs)->index, &layer_index, &exist),
    //       true);
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


    goto err;
    // /* If the file exists at the top layer, delete it. */
    // if (__union_fs_is_top(u, layer_index)) {
    //     layer_fs = u->layers[layer_index]->file_system;
    //     WT_ERR(__union_fs_filename(u->layers[layer_index], session, name, &path));
    //     WT_ERR(layer_fs->fs_remove(layer_fs, wt_session, path, flags));
    // } else
    //     /* Otherwise create a tombstone in the top layer. */
    //     WT_RET(__union_fs_create_tombstone(fs, session, name, flags));

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
    size_t layer_index;
    char *path_from, *path_to;
    bool exist;

    session = (WT_SESSION_IMPL *)wt_session;
    u = (WT_UNION_FS *)fs;

    WT_UNUSED(exist);
    WT_UNUSED(path_to);
    WT_UNUSED(path_from);
    WT_UNUSED(layer_index);
    WT_UNUSED(u);
    WT_UNUSED(session);
    WT_UNUSED(layer_fs);
    WT_UNUSED(ret);
    WT_UNUSED(from);
    WT_UNUSED(to);
    WT_UNUSED(flags);

    exist = false;
    layer_index = 0;
    path_from = NULL;
    path_to = NULL;

    // XXX The logic below isn't atomic.
    // TODO: Do we need this?

    /* Reconcile the differences between layers. */
    //     WT_ERR(__union_fs_reconcile_by_name(u, session, from));

    //     WT_RET_NOTFOUND_OK(
    //       __union_fs_find_layer(fs, session, from, WT_UNION_FS_TOP, &layer_index, &exist));
    //     if (ret == WT_NOTFOUND || !exist)
    //         return (ENOENT);

    //     /* If the file is the top layer, rename it and leave a tombstone behind. */
    //     if (__union_fs_is_top(u, layer_index)) {
    //         layer_fs = u->layers[layer_index]->file_system;
    //         WT_ERR(__union_fs_filename(u->layers[layer_index], session, from, &path_from));
    //         WT_ERR(__union_fs_filename(u->layers[layer_index], session, to, &path_to));
    //         WT_ERR(layer_fs->fs_rename(layer_fs, wt_session, path_from, path_to, flags));
    //         __wt_free(session, path_from);
    //         __wt_free(session, path_to);

    //         /* Create a stop file for the target. */
    //         WT_ERR(__union_fs_create_stop(fs, session, to, flags));

    //         /* Create a tombstone for the source. */
    //         WT_ERR(__union_fs_create_tombstone(fs, session, from, flags));

    //         /* See if there is a file in a lower level. */
    //         WT_ERR_NOTFOUND_OK(
    //           __union_fs_find_layer(fs, session, from, layer_index, &layer_index, &exist), true);
    //         if (ret == WT_NOTFOUND || !exist)
    //             WT_ERR(ENOENT);
    //     }

    // err:
    //     __wt_free(session, path_from);
    //     __wt_free(session, path_to);
    return (0);
}

/*
 * __union_fs_size --
 *     Get the size of a file in bytes, by file name.
 */
static int
__union_fs_size(WT_FILE_SYSTEM *fs, WT_SESSION *wt_session, const char *name, wt_off_t *sizep)
{
    WT_UNUSED(fs);
    WT_UNUSED(wt_session);
    WT_UNUSED(name);
    WT_UNUSED(sizep);
    // WT_DECL_RET;
    // WT_FILE_SYSTEM *layer_fs;
    // WT_SESSION_IMPL *session;
    // WT_UNION_FS *u;
    // bool exist;
    // char *path;
    // size_t layer_index;

    // session = (WT_SESSION_IMPL *)wt_session;
    // u = (WT_UNION_FS *)fs;

    // exist = false;
    // layer_index = 0;
    // path = NULL;

    // // XXX This may need to work across layers
    // // TODO: Implement this? Do we need it?

    // WT_RET_NOTFOUND_OK(
    //   __union_fs_find_layer(fs, session, name, WT_UNION_FS_TOP, &layer_index, &exist));
    // if (ret == WT_NOTFOUND || !exist)
    //     return (ENOENT);

    // WT_RET(__union_fs_filename(u->layers[layer_index], session, name, &path));

    // layer_fs = u->layers[layer_index]->file_system;
    // ret = layer_fs->fs_size(layer_fs, wt_session, path, sizep);

    // __wt_free(session, path);
    // return (ret);

    return (0);
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
    __wt_free(session, u->source.home);

    /* Destination layer. */
    if (u->destination.file_system->terminate != NULL) {
        r = u->destination.file_system->terminate(u->destination.file_system, wt_session);
        if (r != 0 && ret == 0)
            ret = r;
    }
    __wt_free(session, u->destination.home);
    __wt_free(session, u);
    return (ret);
}

/*
 * __wt_os_union_fs --
 *     Initialize a union file system configuration.
 */
int
__wt_os_union_fs(WT_SESSION_IMPL *session, const char *source, const char *destination)
{
    WT_CONNECTION_IMPL *conn;
    WT_UNION_FS *file_system;

    conn = S2C(session);

    WT_RET(__wt_calloc_one(session, &file_system));

    file_system->destination.which = DESTINATION;
    file_system->source.which = SOURCE;

    /* Initialize the union operations. */
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


    file_system->destination.home = destination;
    file_system->source.home = source;

    WT_UNUSED(__union_fs_reconcile_by_name);
    WT_UNUSED(__union_fs_remove_tombstone);
    WT_UNUSED(__union_fs_create_tombstone);
    WT_UNUSED(__union_fs_create_stop);
    WT_UNUSED(__union_fs_stop);
    return (0);
}
