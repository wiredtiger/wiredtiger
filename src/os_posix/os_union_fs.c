/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *  All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"
#include <unistd.h>

#define WT_UNION_FS_TOMBSTONE_SUFFIX ".deleted"

static int __union_fs_file_read(
  WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session, wt_off_t offset, size_t len, void *buf);
static int __union_fs_file_size(
  WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session, wt_off_t *sizep);

#define OFFSET_END(offset, len) (offset + (wt_off_t)len - 1)
#define EXTENT_END(ext) OFFSET_END((ext)->off, (ext)->len)
#define ADDR_IN_EXTENT(addr, ext) ((addr) >= (ext)->off && (addr) <= EXTENT_END(ext))

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

    if (layer->which == WT_UNION_FS_LAYER_DESTINATION) {
        WT_RET(__wt_strdup(session, name, pathp));
    } else {
        char *filename;
        /*
         * Now that we use conn->home for the destination folder name is passed in as
         * `DEST_FOLDER/file.wt`. We need to strip `DEST_FOLDER` and prepend `SOURCE_FOLDER` in the
         * file path.
         */
        filename = basename(name);
        /* +1 for the path separator, +1 for the null terminator. */
        len = strlen(layer->home) + 1 + strlen(filename) + 1;
        WT_RET(__wt_calloc(session, 1, len, &buf));
        WT_ERR(__wt_snprintf(buf, len, "%s%s%s", layer->home, __wt_path_separator(), filename));

        *pathp = buf;
        __wt_verbose_debug3(session, WT_VERB_FILEOPS,
          "Generated SOURCE path: %s\n layer->home = %s, name = %s\n", buf, layer->home, name);
    }

    if (0) {
err:
        __wt_free(session, buf);
    }
    return (ret);
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-function"
/*
 * __union_debug_dump_extent_list --
 *     Dump the contents of a file handle's extent list.
 */
static void
__union_debug_dump_extent_list(WT_SESSION_IMPL *session, WT_UNION_FILE_HANDLE *union_fh)
{
    WT_UNION_HOLE_LIST *hole;
    WT_UNION_HOLE_LIST *prev;
    bool list_valid;

    prev = NULL;
    __wt_verbose_debug1(
      session, WT_VERB_FILEOPS, "Dumping extent list for %s\n", union_fh->iface.name);
    hole = union_fh->destination.hole_list;
    list_valid = true;

    while (hole != NULL) {

        /* Sanity check. This hole doesn't overlap with the previous hole */
        if (prev != NULL) {
            if (EXTENT_END(prev) >= hole->off) {
                __wt_verbose_debug1(session, WT_VERB_FILEOPS,
                  "Error: Holes overlap prev: %ld-%ld, hole:%ld-%ld\n", prev->off, EXTENT_END(prev),
                  hole->off, EXTENT_END(hole));
                list_valid = false;
            }
        }
        __wt_verbose_debug1(
          session, WT_VERB_FILEOPS, "Hole: %ld-%ld\n", hole->off, EXTENT_END(hole));

        prev = hole;
        hole = hole->next;
    }

    WT_ASSERT_ALWAYS(session, list_valid, "Extent list contains overlaps!");
}
#pragma GCC diagnostic pop

/*
 * __union_fs_marker --
 *     Generate a name of a marker file.
 */
static int
__union_fs_marker(WT_SESSION_IMPL *session, const char *name, const char *marker, char **out)
{
    size_t p, suffix_len;

    p = strlen(name);
    suffix_len = strlen(marker);

    WT_RET(__wt_malloc(session, p + suffix_len + 1, out));
    memcpy(*out, name, p);
    memcpy(*out + p, marker, suffix_len + 1);
    return (0);
}

/*
 * __union_fs_create_tombstone --
 *     Create a tombstone for the given file.
 */
static int
__union_fs_create_tombstone(
  WT_FILE_SYSTEM *fs, WT_SESSION_IMPL *session, const char *name, uint32_t flags)
{
    WT_DECL_RET;
    WT_FILE_HANDLE *fh;
    WT_UNION_FS *union_fs;
    uint32_t open_flags;
    char *path, *path_marker;

    union_fs = (WT_UNION_FS *)fs;
    path = path_marker = NULL;

    WT_ERR(__union_fs_filename(&union_fs->destination, session, name, &path));
    WT_ERR(__union_fs_marker(session, path, WT_UNION_FS_TOMBSTONE_SUFFIX, &path_marker));

    open_flags = WT_FS_OPEN_CREATE;
    if (LF_ISSET(WT_FS_DURABLE | WT_FS_OPEN_DURABLE))
        FLD_SET(open_flags, WT_FS_OPEN_DURABLE);

    WT_ERR(union_fs->os_file_system->fs_open_file(union_fs->os_file_system, &session->iface,
      path_marker, WT_FS_OPEN_FILE_TYPE_DATA, open_flags, &fh));
    WT_ERR(fh->close(fh, &session->iface));

    __wt_verbose_debug2(session, WT_VERB_FILEOPS, "Creating tombstone: %s", path_marker);

err:
    __wt_free(session, path);
    __wt_free(session, path_marker);

    return (ret);
}

/*
 * __dest_has_tombstone --
 *     Check whether the destination directory contains a tombstone for a given file.
 */
static int
__dest_has_tombstone(
  WT_UNION_FILE_HANDLE *union_fh, WT_SESSION_IMPL *session, const char *name, bool *existp)
{
    WT_DECL_RET;
    WT_UNION_FS *union_fs;
    char *path, *path_marker;

    union_fs = union_fh->destination.back_pointer;

    WT_ERR(__union_fs_filename(&union_fs->destination, session, name, &path));
    WT_ERR(__union_fs_marker(session, path, WT_UNION_FS_TOMBSTONE_SUFFIX, &path_marker));

    union_fs->os_file_system->fs_exist(
      union_fs->os_file_system, (WT_SESSION *)session, path_marker, existp);
    __wt_verbose_debug2(
      session, WT_VERB_FILEOPS, "Tombstone check for %s (Y/N)? %s", name, *existp ? "Y" : "N");

err:
    __wt_free(session, path);
    __wt_free(session, path_marker);
    return (ret);
}

/*
 * __union_fs_has_file --
 *     Set a boolean to indicate if the given file name exists in the provided layer.
 */
static int
__union_fs_has_file(WT_UNION_FS *union_fs, WT_UNION_FS_LAYER *layer, WT_SESSION_IMPL *session,
  const char *name, bool *existsp)
{
    WT_DECL_RET;
    char *path;

    path = NULL;

    WT_ERR(__union_fs_filename(layer, session, name, &path));
    WT_ERR(
      union_fs->os_file_system->fs_exist(union_fs->os_file_system, &session->iface, path, existsp));
err:
    __wt_free(session, path);

    return (ret);
}

/* Do we need fs_find_layer? We should only interact with the file in the destination. */
/*
 * __union_fs_find_layer --
 *     Find a layer for the given file. Return the index of the layer and whether the layer contains
 *     the file (exists = true) or the tombstone (exists = false). Start searching at the given
 *     layer index - 1; use WT_UNION_FS_TOP to indicate starting at the top.
 */
static int
__union_fs_find_layer(WT_FILE_SYSTEM *fs, WT_SESSION_IMPL *session, const char *name,
  WT_UNION_FS_LAYER_TYPE *whichp, bool *existp)
{
    WT_UNION_FS *union_fs;

    WT_ASSERT(session, existp != NULL);

    *existp = false;
    union_fs = (WT_UNION_FS *)fs;

    WT_RET(__union_fs_has_file(union_fs, &union_fs->destination, session, name, existp));
    if (*existp) {
        /* The file exists in the destination we don't need to look any further. */
        if (whichp != NULL)
            *whichp = WT_UNION_FS_LAYER_DESTINATION;
        return (0);
    }

    WT_RET(__union_fs_has_file(union_fs, &union_fs->source, session, name, existp));
    if (*existp) {
        /* The file exists in the source we don't need to look any further. */
        if (whichp != NULL)
            *whichp = WT_UNION_FS_LAYER_SOURCE;
    } else
        /* We didn't find the file. */
        return (WT_NOTFOUND);

    return (0);
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
        WT_ERR(union_fs->os_file_system->fs_directory_list(union_fs->os_file_system,
          &session->iface, path, prefix, &layer_entries, &layer_num_entries));
        __wt_free(session, path);

        /* Process the entries from the layer, properly handling tombstones. */
        for (i = 0; i < layer_num_entries; i++) {
            // if (__union_fs_is_tombstone(fs, session, layer_entries[i])) {
            //     /* Find the tombstone in a list and mark it as removed. */
            //     l = strlen(layer_entries[i]) - strlen(WT_UNION_FS_TOMBSTONE_SUFFIX);
            //     for (j = 0; j < num_entries; j++) {
            //         if (strncmp(entries[j], layer_entries[i], l) == 0 && strlen(entries[j]) == l)
            //         {
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
                    WT_ERR(
                      __wt_realloc_def(session, &entries_alloc_size, num_entries + 1, &entries));
                    WT_ERR(__wt_strdup(session, layer_entries[i], &entries[num_entries]));
                    ++num_entries;
                }
            }
            // }
        }

        /* Clean up the listing from the layer. */
        WT_ERR(union_fs->os_file_system->fs_directory_list_free(
          union_fs->os_file_system, &session->iface, layer_entries, layer_num_entries));
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
    if (union_fs->os_file_system != NULL && layer_entries != NULL)
        WT_TRET(union_fs->os_file_system->fs_directory_list_free(
          union_fs->os_file_system, &session->iface, layer_entries, layer_num_entries));
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
    /* TODO: This will return tomb stones. We need to not do that. */
    return (__union_fs_directory_list_ext(
      fs, (WT_SESSION_IMPL *)wt_session, directory, prefix, dirlistp, countp, false));
}

/*
 * __union_fs_directory_list_single --
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

static int __union_fs_open_file(WT_FILE_SYSTEM *fs, WT_SESSION *wt_session, const char *name,
  WT_FS_OPEN_FILE_TYPE file_type, uint32_t flags, WT_FILE_HANDLE **file_handlep);

/*
 * __union_fs_free_extent_list --
 *     Free the extents associated with a union file handle.
 */
static void
__union_fs_free_extent_list(WT_SESSION_IMPL *session, WT_UNION_FILE_HANDLE *union_fh)
{
    WT_UNION_HOLE_LIST *hole;
    WT_UNION_HOLE_LIST *temp;

    temp = hole = NULL;
    hole = union_fh->destination.hole_list;
    union_fh->destination.hole_list = NULL;

    while (hole != NULL) {
        temp = hole;
        hole = hole->next;

        temp->next = NULL;
        __wt_free(session, temp);
    }

    return;
}

/*
 * __union_fs_fill_holes_on_file_close --
 *     On file close make sure we've copied across all data from source to destination. This means
 *     there are no holes in the destination file's extent list. If we find one promote read the
 *     content into the destination.
 *
 * NOTE!! This assumes there cannot be holes in source, and that any truncates/extensions of the
 *     destination file are already handled elsewhere.
 *
 * FIXME - This can cause very slow file close/clean shutdowns for customers during live restore.
 *     Maybe we only need this for our test which overwrites WT_TEST on each loop?
 */
static int
__union_fs_fill_holes_on_file_close(WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session)
{
    WT_UNION_FILE_HANDLE *fh;
    WT_UNION_HOLE_LIST *hole;
    /*
     * FIXME - 4MB buffer as a placeholder. When we find a large hole we should break the read into
     * small chunks
     */
    char buf[4096000];

    fh = (WT_UNION_FILE_HANDLE *)file_handle;
    hole = fh->destination.hole_list;

    while (hole != NULL) {
        __wt_verbose_debug3((WT_SESSION_IMPL *)wt_session, WT_VERB_FILEOPS,
          "Found hole in %s at %ld-%ld during file close. Filling", fh->iface.name, hole->off,
          EXTENT_END(hole));
        WT_RET(__union_fs_file_read(
          file_handle, wt_session, hole->off, (size_t)(EXTENT_END(hole) - hole->off), buf));
        hole = hole->next;
    }

    return (0);
}

/*
 * __union_fs_file_close --
 *     Close the file.
 */
static int
__union_fs_file_close(WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session)
{
    WT_SESSION_IMPL *session;
    WT_UNION_FILE_HANDLE *union_fh;

    union_fh = (WT_UNION_FILE_HANDLE *)file_handle;
    session = (WT_SESSION_IMPL *)wt_session;
    __wt_verbose_debug1(
      session, WT_VERB_FILEOPS, "UNION_FS: Closing file: %s\n", file_handle->name);

    __union_fs_fill_holes_on_file_close(file_handle, wt_session);

    union_fh->destination.fh->close(union_fh->destination.fh, wt_session);
    __union_fs_free_extent_list(session, union_fh);

    if (union_fh->source != NULL) /* It's possible that we never opened the file in the source. */
        union_fh->source->close(union_fh->source, wt_session);
    __wt_free(session, union_fh->iface.name);
    __wt_free(session, union_fh);

    return (0);
}

/*
 * __union_fs_file_lock --
 *     Lock/unlock a file.
 */
static int
__union_fs_file_lock(WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session, bool lock)
{
    WT_UNION_FILE_HANDLE *fh;

    fh = (WT_UNION_FILE_HANDLE *)file_handle;
    return (fh->destination.fh->fh_lock(fh->destination.fh, wt_session, lock));
}

/*
 * __union_remove_extlist_hole --
 *     Track that we wrote something by removing its hole from the extent list.
 */
static int
__union_remove_extlist_hole(
  WT_UNION_FILE_HANDLE *union_fh, WT_SESSION_IMPL *session, wt_off_t offset, size_t len)
{
    WT_UNION_HOLE_LIST *hole, *tmp, *new, *prev_hole;
    wt_off_t write_end;

    __wt_verbose_debug2(session, WT_VERB_FILEOPS, "REMOVE HOLE %s: %ld-%ld", union_fh->iface.name,
      offset, OFFSET_END(offset, len));

    write_end = OFFSET_END(offset, len);

    /* FIXME - This 100% needs concurrency control. Locking is easy, but a CAS might be straight
     * forward?
     */
    hole = union_fh->destination.hole_list;
    prev_hole = NULL;
    while (hole != NULL) {

        if (write_end < hole->off) {
            /* We won't find any more overlapping holes. Stop searching. */
            break;
        }

        if (offset <= hole->off && write_end >= EXTENT_END(hole)) {
            /* The write fully overlaps a hole. Delete it. */
            __wt_verbose_debug3(
              session, WT_VERB_FILEOPS, "Fully overlaps hole %ld-%ld", hole->off, EXTENT_END(hole));

            tmp = hole;
            if (prev_hole == NULL)
                union_fh->destination.hole_list = hole->next;
            else
                prev_hole->next = hole->next;
            hole = hole->next;
            __wt_free(session, tmp);
            continue;

        } else if (offset > hole->off && write_end < EXTENT_END(hole)) {
            /* The write is entirely within the hole. Split the hole in two. */

            __wt_verbose_debug3(session, WT_VERB_FILEOPS, "Fully contained by hole %ld-%ld",
              hole->off, EXTENT_END(hole));

            /* First create the hole to the right of the write. */
            WT_RET(__wt_calloc_one(session, &new));
            new->off = write_end + 1;
            new->len = (size_t)(EXTENT_END(hole) - write_end);
            new->next = hole->next;

            /*
             * Then shrink the existing hole so it's to the left of the write and point it at the
             * new hole.
             */
            hole->len = (size_t)(offset - hole->off);
            hole->next = new;

        } else if (offset <= hole->off && ADDR_IN_EXTENT(write_end, hole)) {
            /* The write starts before the hole and ends within it. Shrink the hole. */
            __wt_verbose_debug3(session, WT_VERB_FILEOPS,
              "Partial overlap to the left of hole %ld-%ld", hole->off, EXTENT_END(hole));

            hole->len = (size_t)(EXTENT_END(hole) - write_end);
            hole->off = write_end + 1;

        } else if (ADDR_IN_EXTENT(offset, hole) && write_end >= EXTENT_END(hole)) {
            __wt_verbose_debug3(session, WT_VERB_FILEOPS,
              "Partial overlap to the right of hole %ld-%ld", hole->off, EXTENT_END(hole));
            /* The write starts within the hole and ends after it. Shrink the hole. */
            hole->len = (size_t)(offset - hole->off);

        } else {
            /* No overlap. Safety check */
            WT_ASSERT(session, write_end < hole->off || offset > EXTENT_END(hole));
        }

        prev_hole = hole;
        hole = hole->next;
    }
    return (0);
}

/*
 * __union_can_service_read --
 *     Return if a the read can be serviced by the destination file. This assumes that the block
 *     manager is the only thing that perform reads and it only reads and writes full blocks. If
 *     that changes this code will unceremoniously fall over.
 */
static bool
__union_can_service_read(
  WT_UNION_FILE_HANDLE *union_fh, WT_SESSION_IMPL *session, wt_off_t offset, size_t len)
{
    WT_UNION_HOLE_LIST *hole;
    wt_off_t read_end;
    bool read_begins_in_hole, read_ends_in_hole;

    read_end = OFFSET_END(offset, len);

    hole = union_fh->destination.hole_list;
    while (hole != NULL) {

        if (read_end < hole->off)
            /* All subsequent holes are past the read. We won't find matching holes */
            break;

        read_begins_in_hole = ADDR_IN_EXTENT(offset, hole);
        read_ends_in_hole = ADDR_IN_EXTENT(read_end, hole);
        if (read_begins_in_hole && read_ends_in_hole) {
            /* Our read is entirely within a hole */
            __wt_verbose_debug3(session, WT_VERB_FILEOPS,
              "CANNOT SERVICE %s: Reading from hole. Read: %ld-%ld, hole: %ld-%ld",
              union_fh->iface.name, offset, read_end, hole->off, EXTENT_END(hole));
            return (false);
        } else if (read_begins_in_hole != read_ends_in_hole) {
            /*
             * The read starts in a hole but doesn't finish in it, or vice versa. This should never
             * happen.
             */
            WT_ASSERT_ALWAYS(session, false, "Read partially covers a hole");
        }

        hole = hole->next;
    }

    __wt_verbose_debug3(
      session, WT_VERB_FILEOPS, "CAN SERVICE %s: No hole found", union_fh->iface.name);
    return (true);
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
    WT_UNION_FILE_HANDLE *union_fh;

    union_fh = (WT_UNION_FILE_HANDLE *)fh;
    session = (WT_SESSION_IMPL *)wt_session;

    __wt_verbose_debug1(session, WT_VERB_FILEOPS, "WRITE %s: %ld, %lu", fh->name, offset, len);
    /* TODO - why write to file before setting the extent? */
    WT_RET(
      union_fh->destination.fh->fh_write(union_fh->destination.fh, wt_session, offset, len, buf));
    WT_RET(union_fh->destination.fh->fh_sync(union_fh->destination.fh, wt_session));
    WT_RET(__union_remove_extlist_hole(union_fh, session, offset, len));
    return (0);
}

/*
 * __read_promote --
 *     Write out the contents of a read into the destination. This will be overkill for cases where
 *     a read is performed to service a write. Which is most cases however this is a PoC.
 *
 * TODO: Locking needed.
 */
static int
__read_promote(
  WT_UNION_FILE_HANDLE *union_fh, WT_SESSION_IMPL *session, wt_off_t offset, size_t len, char *read)
{
    __wt_verbose_debug2(session, WT_VERB_FILEOPS, "    READ PROMOTE %s : %ld, %lu",
      union_fh->iface.name, offset, len);
    WT_RET(
      __union_fs_file_write((WT_FILE_HANDLE *)union_fh, (WT_SESSION *)session, offset, len, read));

    return (0);
}

/*
 * __union_fs_file_read --
 *     File read in a union file system.
 */
static int
__union_fs_file_read(
  WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session, wt_off_t offset, size_t len, void *buf)
{
    WT_DECL_RET;
    WT_SESSION_IMPL *session;
    WT_UNION_FILE_HANDLE *union_fh;
    char *read_data;
    bool can_service_read;

    union_fh = (WT_UNION_FILE_HANDLE *)file_handle;
    session = (WT_SESSION_IMPL *)wt_session;

    __wt_verbose_debug1(
      session, WT_VERB_FILEOPS, "READ %s : %ld, %lu", file_handle->name, offset, len);

    read_data = (char *)buf;

    can_service_read = __union_can_service_read(union_fh, session, offset, len);

    /*
     * TODO: WiredTiger will read the metadata file after creation but before anything has been
     * written in this case we forward the read to the empty metadata file in the destination. Is
     * this correct?
     */
    if (union_fh->destination.complete || union_fh->source == NULL || can_service_read) {
        /*
         * TODO: Right now if complete is true source will always be null. So the if statement here
         * has redundancy is there a time when we need it? Maybe with the background thread.
         */
        __wt_verbose_debug2(session, WT_VERB_FILEOPS, "    READ FROM DEST (src is NULL? %s)",
          union_fh->source == NULL ? "YES" : "NO");
        /* Read the full read from the destination. */
        WT_ERR(union_fh->destination.fh->fh_read(
          union_fh->destination.fh, wt_session, offset, len, read_data));
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
    WT_UNION_FILE_HANDLE *fh;
    wt_off_t destination_size;

    fh = (WT_UNION_FILE_HANDLE *)file_handle;

    WT_RET(fh->destination.fh->fh_size(fh->destination.fh, wt_session, &destination_size));
    *sizep = destination_size;
    return (0);
}

/*
 * __union_fs_file_sync --
 *     POSIX fsync. This only sync the destination as the source is readonly.
 */
static int
__union_fs_file_sync(WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session)
{
    WT_UNION_FILE_HANDLE *fh;

    fh = (WT_UNION_FILE_HANDLE *)file_handle;
    return (fh->destination.fh->fh_sync(fh->destination.fh, wt_session));
}

/*
 * __union_fs_file_truncate --
 *     Truncate a file. This operation is only applied to the destination file.
 */
static int
__union_fs_file_truncate(WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session, wt_off_t len)
{
    WT_UNION_FILE_HANDLE *fh;
    wt_off_t old_len;

    fh = (WT_UNION_FILE_HANDLE *)file_handle;

    /*
     * If we truncate a range we'll never need to read that range from the source file. Mark it as
     * such.
     */
    __union_fs_file_size(file_handle, wt_session, &old_len);

    if (old_len == len)
        /* Sometimes we call truncate but don't change the length. Ignore */
        return (0);

    if (len > old_len)
        /*
         * It's technically allowed to extend a file by calling truncate. We're ignoring this case
         * for now.
         */
        WT_ASSERT_ALWAYS(
          (WT_SESSION_IMPL *)wt_session, false, "truncate call used to extend file!");

    __wt_verbose_debug2((WT_SESSION_IMPL *)wt_session, WT_VERB_FILEOPS,
      "truncating file %s from %ld to %ld", file_handle->name, old_len, len);
    __union_remove_extlist_hole(fh, (WT_SESSION_IMPL *)wt_session, len, (size_t)(old_len - len));

    return (fh->destination.fh->fh_truncate(fh->destination.fh, wt_session, len));
}

/*
 * __union_fs_open_in_source --
 *     Open a file handle in the source.
 */
static int
__union_fs_open_in_source(
  WT_UNION_FS *union_fs, WT_SESSION_IMPL *session, WT_UNION_FILE_HANDLE *union_fh, uint32_t flags)
{
    WT_DECL_RET;
    WT_FILE_HANDLE *fh;

    char *path;

    path = NULL;

    /* Clear the create flag. TODO: Can we assert something here? */
    FLD_CLR(flags, WT_FS_OPEN_CREATE);

    /* Open the file in the layer. */
    WT_ERR(__union_fs_filename(&union_fs->source, session, union_fh->iface.name, &path));
    WT_ERR(union_fs->os_file_system->fs_open_file(
      union_fs->os_file_system, (WT_SESSION *)session, path, union_fh->file_type, flags, &fh));

    union_fh->source = fh;

err:
    __wt_free(session, path);
    return (ret);
}

#include <unistd.h>
/*
 * __union_build_holes_from_dest_file_lseek --
 *     When opening a file from destination create its existing hole list from the file system
 *     information. Any holes in the extent list are data that hasn't been copied from source yet.
 */
static int
__union_build_holes_from_dest_file_lseek(
  WT_SESSION_IMPL *session, char *filename, WT_UNION_FILE_HANDLE *union_fh)
{
    WT_DECL_RET;
    wt_off_t data_offset, data_end_offset, file_size;
    int fd;

    data_offset = data_end_offset = 0;
    fd = open(filename, O_RDONLY);

    /* Check that we opened a valid file descriptor. */
    WT_ASSERT(session, fcntl(fd, F_GETFD) != -1 || errno != EBADF);
    WT_ERR(__union_fs_file_size((WT_FILE_HANDLE *)union_fh, (WT_SESSION *)session, &file_size));
    __wt_verbose_debug2(session, WT_VERB_FILEOPS, "File: %s", filename);
    __wt_verbose_debug2(session, WT_VERB_FILEOPS, "    len: %ld", file_size);

    if (file_size > 0) {
        /* Initialize the hole_list as one big hole. Then find data segments and remove them. */
        WT_ERR(__wt_calloc_one(session, &union_fh->destination.hole_list));
        union_fh->destination.hole_list->off = 0;
        union_fh->destination.hole_list->len = (size_t)file_size;
        union_fh->destination.hole_list->next = NULL;
    }

    /*
     * Find the next data block. data_end_offset is initialized to zero so we start from the
     * beginning of the file.
     */
    while ((data_offset = lseek(fd, data_end_offset, SEEK_DATA)) != -1) {

        data_end_offset = lseek(fd, data_offset, SEEK_HOLE);
        /* All data must be followed by a hole */
        WT_ASSERT(session, data_end_offset != -1);
        WT_ASSERT(session, data_end_offset > data_offset - 1);

        __wt_verbose_debug1(session, WT_VERB_FILEOPS, "File: %s, has data from %ld-%ld", filename,
          data_offset, data_end_offset);
        WT_ERR(__union_remove_extlist_hole(
          union_fh, session, data_offset, (size_t)(data_end_offset - data_offset)));
    }

err:
    close(fd);
    return (ret);
}

/*
 * __union_fs_open_in_destination --
 *     Open a file handle.
 */
static int
__union_fs_open_in_destination(WT_UNION_FS *union_fs, WT_SESSION_IMPL *session,
  WT_UNION_FILE_HANDLE *union_fh, uint32_t flags, bool create)
{
    WT_DECL_RET;
    WT_FILE_HANDLE *fh;
    char *path;

    path = NULL;

    if (create)
        flags |= WT_FS_OPEN_CREATE;

    /* Open the file in the layer. */
    WT_ERR(__union_fs_filename(&union_fs->destination, session, union_fh->iface.name, &path));
    WT_ERR(union_fs->os_file_system->fs_open_file(
      union_fs->os_file_system, (WT_SESSION *)session, path, union_fh->file_type, flags, &fh));
    union_fh->destination.fh = fh;
    union_fh->destination.back_pointer = union_fs;

    /* Get the map of the file. */
    WT_ASSERT(session, union_fh->file_type != WT_FS_OPEN_FILE_TYPE_DIRECTORY);
    __union_build_holes_from_dest_file_lseek(session, path, union_fh);
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
    WT_UNION_FILE_HANDLE *union_fh;
    WT_UNION_FS *union_fs;
    WT_UNION_FS_LAYER_TYPE which;
    bool dest_exist, source_exist, have_tombstone, readonly;

    session = (WT_SESSION_IMPL *)wt_session;
    union_fs = (WT_UNION_FS *)fs;

    dest_exist = source_exist = false;
    union_fh = NULL;
    have_tombstone = false;
    WT_UNUSED(have_tombstone);
    readonly = LF_ISSET(WT_FS_OPEN_READONLY);
    WT_UNUSED(readonly);
    WT_UNUSED(which);

    /* TODO: Handle WT_FS_OPEN_FILE_TYPE_DIRECTORY */

    /* Set up the file handle. */
    WT_ERR(__wt_calloc_one(session, &union_fh));
    WT_ERR(__wt_strdup(session, name, &union_fh->iface.name));
    union_fh->iface.file_system = fs;
    union_fh->file_type = file_type;

    /* TODO: Handle the exclusive flag and other flags */

    /* Open it in the destination layer. */
    WT_ERR_NOTFOUND_OK(
      __union_fs_has_file(union_fs, &union_fs->destination, session, name, &dest_exist), true);
    WT_ERR(__union_fs_open_in_destination(union_fs, session, union_fh, flags, !dest_exist));

    WT_ERR(__dest_has_tombstone(union_fh, session, name, &have_tombstone));
    if (have_tombstone)
        /*
         * Set the complete flag, we know that if there is a tombstone we should never look in the
         * source. Therefore the destination must be complete.
         */
        union_fh->destination.complete = true;
    else {
        /*
         * If it exists in the source, open it. If it doesn't exist in the source then by definition
         * the destination file is complete.
         */
        WT_ERR_NOTFOUND_OK(
          __union_fs_has_file(union_fs, &union_fs->source, session, name, &source_exist), true);
        if (source_exist) {
            WT_ERR(__union_fs_open_in_source(union_fs, session, union_fh, flags));

            if (!dest_exist) {
                /*
                 * We're creating a new destination file which is backed by a source file. It
                 * currently has a length of zero, but we want its length to be the same as the
                 * source file. We do this by reading the last byte of the file. This will read
                 * promote the byte into the destination file, setting the file size.
                 */
                wt_off_t source_size;

                union_fh->source->fh_size(union_fh->source, wt_session, &source_size);
                __wt_verbose_debug1(session, WT_VERB_FILEOPS,
                  "Creating destination file backed by source file. Copying size (%ld) from source "
                  "file",
                  source_size);

                /*
                 * Set size by truncating. We're bypassing the union layer so we don't track the
                 * write.
                 */
                union_fh->destination.fh->fh_truncate(
                  union_fh->destination.fh, wt_session, source_size);

                /* Initialize as one big hole. We need to read everything from source. */
                WT_ERR(__wt_calloc_one(session, &union_fh->destination.hole_list));
                union_fh->destination.hole_list->off = 0;
                union_fh->destination.hole_list->len = (size_t)source_size;
                union_fh->destination.hole_list->next = NULL;
            }
        } else
            union_fh->destination.complete = true;
    }

    /* If there is a tombstone, delete it. */
    // TODO: do we need this? I don't think so.
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
    union_fh->iface.close = __union_fs_file_close;
    union_fh->iface.fh_lock = __union_fs_file_lock;
    union_fh->iface.fh_read = __union_fs_file_read;
    union_fh->iface.fh_size = __union_fs_file_size;
    union_fh->iface.fh_sync = __union_fs_file_sync;
    union_fh->iface.fh_truncate = __union_fs_file_truncate;
    union_fh->iface.fh_write = __union_fs_file_write;

    /* TODO: These are unimplemented. */
    union_fh->iface.fh_advise = NULL;
    union_fh->iface.fh_sync_nowait = NULL;
    union_fh->iface.fh_unmap = NULL;
    union_fh->iface.fh_map_preload = NULL;
    union_fh->iface.fh_map_discard = NULL;
    union_fh->iface.fh_map = NULL;
    union_fh->iface.fh_extend = NULL;
    union_fh->iface.fh_extend_nolock = NULL;

    *file_handlep = (WT_FILE_HANDLE *)union_fh;

    if (0) {
err:
        if (union_fh != NULL)
            __union_fs_file_close((WT_FILE_HANDLE *)union_fh, wt_session);
    }
    return (ret);
}

/*
 * __union_fs_remove --
 *     Remove a file. We can only delete from the destination directory anyway.
 */
static int
__union_fs_remove(WT_FILE_SYSTEM *fs, WT_SESSION *wt_session, const char *name, uint32_t flags)
{
    WT_DECL_RET;
    WT_SESSION_IMPL *session;
    WT_UNION_FS *union_fs;
    WT_UNION_FS_LAYER_TYPE layer;
    char *path;
    bool exist;

    session = (WT_SESSION_IMPL *)wt_session;
    union_fs = (WT_UNION_FS *)fs;

    WT_UNUSED(layer);
    exist = false;
    path = NULL;

    WT_RET_NOTFOUND_OK(__union_fs_find_layer(fs, session, name, &layer, &exist));
    if (ret == WT_NOTFOUND || !exist)
        return (0);

    /* It's possible to call remove on a file that hasn't yet been created in the destination. In
     * these cases we only need to create the tombstone */
    if (layer == WT_UNION_FS_LAYER_DESTINATION) {
        WT_ERR(__union_fs_filename(&union_fs->destination, session, name, &path));
        union_fs->os_file_system->fs_remove(union_fs->os_file_system, wt_session, path, flags);
    }

    /* We need file tombstones here but can we be sure this is correct? */
    __union_fs_create_tombstone(fs, session, name, flags);
    /* We don't have a file handle here so WT must have previously closed it. */
err:

    __wt_free(session, path);
    return (ret);
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
    WT_SESSION_IMPL *session;
    WT_UNION_FS *union_fs;
    WT_UNION_FS_LAYER_TYPE which;
    char *path_from, *path_to;
    bool exist;

    session = (WT_SESSION_IMPL *)wt_session;
    union_fs = (WT_UNION_FS *)fs;

    exist = false;
    path_from = NULL;
    path_to = NULL;

    /*
     * WiredTiger frequently renames the turtle file, and some other files. This function is more
     * critical than it may seem at first.
     */

    __wt_verbose_debug1(
      session, WT_VERB_FILEOPS, "UNION_FS: Renaming file from: %s to %s\n", from, to);
    WT_RET_NOTFOUND_OK(__union_fs_find_layer(fs, session, from, &which, &exist));
    if (ret == WT_NOTFOUND || !exist)
        return (ENOENT);

    /* If the file is the top layer, rename it and leave a tombstone behind. */
    if (which == WT_UNION_FS_LAYER_DESTINATION) {
        WT_ERR(__union_fs_filename(&union_fs->destination, session, from, &path_from));
        WT_ERR(__union_fs_filename(&union_fs->destination, session, to, &path_to));
        WT_ERR(union_fs->os_file_system->fs_rename(
          union_fs->os_file_system, wt_session, path_from, path_to, flags));
        __wt_free(session, path_from);
        __wt_free(session, path_to);

        /* Create a tombstone for the file. */
        WT_ERR(__union_fs_create_tombstone(fs, session, to, flags));
        /* Create a tombstone for the old file as well. */
        WT_ERR(__union_fs_create_tombstone(fs, session, from, flags));
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
    WT_UNION_FS *union_fs;
    WT_UNION_FS_LAYER_TYPE which;
    char *path;
    bool exist;

    session = (WT_SESSION_IMPL *)wt_session;
    union_fs = (WT_UNION_FS *)fs;

    exist = false;
    path = NULL;

    WT_RET_NOTFOUND_OK(__union_fs_find_layer(fs, session, name, &which, &exist));
    if (ret == WT_NOTFOUND || !exist)
        return (ENOENT);

    /* The file will always exist in the destination. This the is authoritative file size. */
    WT_ASSERT(session, which == WT_UNION_FS_LAYER_DESTINATION);
    WT_RET(__union_fs_filename(&union_fs->destination, session, name, &path));
    ret = union_fs->os_file_system->fs_size(union_fs->os_file_system, wt_session, path, sizep);

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
    WT_SESSION_IMPL *session;
    WT_UNION_FS *union_fs;

    session = (WT_SESSION_IMPL *)wt_session;
    union_fs = (WT_UNION_FS *)fs;

    WT_ASSERT(session, union_fs->os_file_system != NULL);
    WT_RET(union_fs->os_file_system->terminate(union_fs->os_file_system, wt_session));

    __wt_free(session, union_fs->source.home);
    /* TODO: Do we free ourselves here? */
    return (0);
}

/*
 * __wt_os_union_fs --
 *     Initialize a union file system configuration.
 */
int
__wt_os_union_fs(WT_SESSION_IMPL *session, WT_CONFIG_ITEM *source_cfg, const char *destination,
  WT_FILE_SYSTEM **fsp)
{
    WT_UNION_FS *union_fs;

    WT_RET(__wt_calloc_one(session, &union_fs));
    WT_RET(__wt_os_posix(session, &union_fs->os_file_system));

    /* Initialize the FS jump table. */
    union_fs->iface.fs_directory_list = __union_fs_directory_list;
    union_fs->iface.fs_directory_list_single = __union_fs_directory_list_single;
    union_fs->iface.fs_directory_list_free = __union_fs_directory_list_free;
    union_fs->iface.fs_exist = __union_fs_exist;
    union_fs->iface.fs_open_file = __union_fs_open_file;
    union_fs->iface.fs_remove = __union_fs_remove;
    union_fs->iface.fs_rename = __union_fs_rename;
    union_fs->iface.fs_size = __union_fs_size;
    union_fs->iface.terminate = __union_fs_terminate;

    /* Initialize the layers. */
    union_fs->destination.home = destination;
    union_fs->destination.which = WT_UNION_FS_LAYER_DESTINATION;
    WT_RET(__wt_strndup(session, source_cfg->str, source_cfg->len, &union_fs->source.home));
    union_fs->source.which = WT_UNION_FS_LAYER_SOURCE;

    __wt_verbose_debug1(session, WT_VERB_FILEOPS,
      "WiredTiger started in live restore mode! Source path is: %s, Destination path is %s",
      union_fs->source.home, destination);

    /* Update the callers pointer. */
    *fsp = (WT_FILE_SYSTEM *)union_fs;
    return (0);
}
