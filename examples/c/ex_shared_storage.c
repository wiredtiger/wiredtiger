/*-
 * Public Domain 2014-2020 MongoDB, Inc.
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
 *
 * ex_shared_storage.c
 * 	demonstrates how to use the custom shared storage interface
 */
#include <test_util.h>

/*
 * This example code uses pthread functions for portable locking, we ignore errors for simplicity.
 */
static void
allocate_shared_storage_lock(pthread_rwlock_t *lockp)
{
    error_check(pthread_rwlock_init(lockp, NULL));
}

static void
destroy_shared_storage_lock(pthread_rwlock_t *lockp)
{
    error_check(pthread_rwlock_destroy(lockp));
}

static void
lock_shared_storage(pthread_rwlock_t *lockp)
{
    error_check(pthread_rwlock_wrlock(lockp));
}

static void
unlock_shared_storage(pthread_rwlock_t *lockp)
{
    error_check(pthread_rwlock_unlock(lockp));
}

/*
 * Example shared storage implementation, using memory buffers to represent objects.
 */
typedef struct {
    WT_SHARED_STORAGE iface;

    /*
     * WiredTiger performs schema and I/O operations in parallel, all shared storage and file handle
     * access must be thread-safe. This example uses a single, global shared storage lock for
     * simplicity; real applications might require finer granularity, for example, a single lock for
     * the shared storage handle list and per-handle locks serializing I/O.
     */
    pthread_rwlock_t lock; /* Lock */

    int opened_object_count;
    int opened_unique_object_count;
    int closed_object_count;
    int read_ops;
    int write_ops;

    /* Queue of file handles */
    TAILQ_HEAD(demo_file_handle_qh, demo_file_handle) fileq;

    WT_EXTENSION_API *wtext; /* Extension functions */

} DEMO_SHARED_STORAGE;

typedef struct demo_file_handle {
    WT_FILE_HANDLE iface;

    /*
     * Add custom file handle fields after the interface.
     */
    DEMO_SHARED_STORAGE *demo_ss; /* Enclosing shared storage */

    TAILQ_ENTRY(demo_file_handle) q; /* Queue of handles */
    uint32_t ref;                    /* Reference count */

    char *buf;      /* In-memory contents */
    size_t bufsize; /* In-memory buffer size */

    size_t size; /* Read/write data size */
} DEMO_FILE_HANDLE;

/*
 * Extension initialization function.
 */
#ifdef _WIN32
/*
 * Explicitly export this function so it is visible when loading extensions.
 */
__declspec(dllexport)
#endif
  int demo_shared_storage_create(WT_CONNECTION *, WT_CONFIG_ARG *);

/*
 * Forward function declarations for shared storage API implementation
 */
static int demo_ss_open(WT_SHARED_STORAGE *, WT_SESSION *, void *, const char *,
  uint32_t, WT_FILE_HANDLE **);
static int demo_ss_location_handle(
  WT_SHARED_STORAGE *, WT_SESSION *, const char *, void **);
static int demo_ss_location_handle_free(WT_SHARED_STORAGE *, WT_SESSION *, void *);
static int demo_ss_location_list(
  WT_SHARED_STORAGE *, WT_SESSION *, void *, char ***, uint32_t *);
static int demo_ss_location_list_free(WT_SHARED_STORAGE *, WT_SESSION *, char **, uint32_t);
static int demo_ss_exist(WT_SHARED_STORAGE *, WT_SESSION *, void *, const char *, bool *);
static int demo_ss_remove(WT_SHARED_STORAGE *, WT_SESSION *, void *, const char *, uint32_t);
static int demo_ss_size(WT_SHARED_STORAGE *, WT_SESSION *, void *, const char *, wt_off_t *);
static int demo_ss_terminate(WT_SHARED_STORAGE *, WT_SESSION *);

/*
 * Forward function declarations for file handle API implementation
 */
static int demo_file_close(WT_FILE_HANDLE *, WT_SESSION *);
static int demo_file_lock(WT_FILE_HANDLE *, WT_SESSION *, bool);
static int demo_file_read(WT_FILE_HANDLE *, WT_SESSION *, wt_off_t, size_t, void *);
static int demo_file_size(WT_FILE_HANDLE *, WT_SESSION *, wt_off_t *);
static int demo_file_sync(WT_FILE_HANDLE *, WT_SESSION *);
static int demo_file_truncate(WT_FILE_HANDLE *, WT_SESSION *, wt_off_t);
static int demo_file_write(WT_FILE_HANDLE *, WT_SESSION *, wt_off_t, size_t, const void *);

/*
 * Forward function declarations for internal functions
 */
static int demo_handle_remove(WT_SESSION *, DEMO_FILE_HANDLE *);
static DEMO_FILE_HANDLE *demo_handle_search(WT_SHARED_STORAGE *, void *, const char *);

#define DEMO_FILE_SIZE_INCREMENT 32768

/*
 * string_match --
 *     Return if a string matches a byte string of len bytes.
 */
static bool
byte_string_match(const char *str, const char *bytes, size_t len)
{
    return (strncmp(str, bytes, len) == 0 && (str)[(len)] == '\0');
}

/*
 * demo_shared_storage_create --
 *     Initialization point for demo shared storage
 */
int
demo_shared_storage_create(WT_CONNECTION *conn, WT_CONFIG_ARG *config)
{
    DEMO_SHARED_STORAGE *demo_ss;
    WT_CONFIG_ITEM k, v;
    WT_CONFIG_PARSER *config_parser;
    WT_EXTENSION_API *wtext;
    WT_SHARED_STORAGE *shared_storage;
    int ret = 0;

    wtext = conn->get_extension_api(conn);

    if ((demo_ss = calloc(1, sizeof(DEMO_SHARED_STORAGE))) == NULL) {
        (void)wtext->err_printf(
          wtext, NULL, "demo_shared_storage_create: %s", wtext->strerror(wtext, NULL, ENOMEM));
        return (ENOMEM);
    }
    demo_ss->wtext = wtext;
    shared_storage = (WT_SHARED_STORAGE *)demo_ss;

    /*
     * Applications may have their own configuration information to pass to the underlying
     * filesystem implementation. See the main function for the setup of those configuration
     * strings; here we parse configuration information as passed in by main, through WiredTiger.
     */
    if ((ret = wtext->config_parser_open_arg(wtext, NULL, config, &config_parser)) != 0) {
        (void)wtext->err_printf(wtext, NULL, "WT_EXTENSION_API.config_parser_open: config: %s",
          wtext->strerror(wtext, NULL, ret));
        goto err;
    }

    /* Step through our configuration values. */
    printf("Custom shared storage configuration\n");
    while ((ret = config_parser->next(config_parser, &k, &v)) == 0) {
        if (byte_string_match("config_string", k.str, k.len)) {
            printf(
              "\t"
              "key %.*s=\"%.*s\"\n",
              (int)k.len, k.str, (int)v.len, v.str);
            continue;
        }
        if (byte_string_match("config_value", k.str, k.len)) {
            printf(
              "\t"
              "key %.*s=%" PRId64 "\n",
              (int)k.len, k.str, v.val);
            continue;
        }
        ret = EINVAL;
        (void)wtext->err_printf(wtext, NULL,
          "WT_CONFIG_PARSER.next: unexpected configuration "
          "information: %.*s=%.*s: %s",
          (int)k.len, k.str, (int)v.len, v.str, wtext->strerror(wtext, NULL, ret));
        goto err;
    }

    /* Check for expected parser termination and close the parser. */
    if (ret != WT_NOTFOUND) {
        (void)wtext->err_printf(
          wtext, NULL, "WT_CONFIG_PARSER.next: config: %s", wtext->strerror(wtext, NULL, ret));
        goto err;
    }
    if ((ret = config_parser->close(config_parser)) != 0) {
        (void)wtext->err_printf(
          wtext, NULL, "WT_CONFIG_PARSER.close: config: %s", wtext->strerror(wtext, NULL, ret));
        goto err;
    }

    allocate_shared_storage_lock(&demo_ss->lock);

    /* Initialize the in-memory jump table. */
    shared_storage->ss_location_handle = demo_ss_location_handle;
    shared_storage->ss_location_handle_free = demo_ss_location_handle_free;
    shared_storage->ss_location_list = demo_ss_location_list;
    shared_storage->ss_location_list_free = demo_ss_location_list_free;
    shared_storage->ss_exist = demo_ss_exist;
    shared_storage->ss_open_object = demo_ss_open;
    shared_storage->ss_remove = demo_ss_remove;
    shared_storage->ss_size = demo_ss_size;
    shared_storage->terminate = demo_ss_terminate;

    if ((ret = conn->add_shared_storage(conn, "demo", shared_storage, NULL)) != 0) {
        (void)wtext->err_printf(
          wtext, NULL, "WT_CONNECTION.set_shared_storage: %s", wtext->strerror(wtext, NULL, ret));
        goto err;
    }
    return (0);

err:
    free(demo_ss);
    /* An error installing the shared storage is fatal. */
    exit(1);
}

/*
 * demo_ss_open --
 *     fopen for our demo shared storage
 */
static int
demo_ss_open(WT_SHARED_STORAGE *shared_storage, WT_SESSION *session, void *location_handle,
  const char *name, uint32_t flags, WT_FILE_HANDLE **file_handlep)
{
    DEMO_FILE_HANDLE *demo_fh;
    DEMO_SHARED_STORAGE *demo_ss;
    WT_EXTENSION_API *wtext;
    WT_FILE_HANDLE *file_handle;
    int ret = 0;

    (void)flags;     /* Unused */

    *file_handlep = NULL;

    demo_ss = (DEMO_SHARED_STORAGE *)shared_storage;
    demo_fh = NULL;
    wtext = demo_ss->wtext;

    lock_shared_storage(&demo_ss->lock);
    ++demo_ss->opened_object_count;

    /*
     * First search the file queue, if we find it, assert there's only a single reference, we only
     * support a single handle on any file.
     */
    demo_fh = demo_handle_search(shared_storage, location_handle, name);
    if (demo_fh != NULL) {
        if (demo_fh->ref != 0) {
            (void)wtext->err_printf(wtext, session, "demo_ss_open: %s: file already open", name);
            ret = EBUSY;
            goto err;
        }

        demo_fh->ref = 1;

        *file_handlep = (WT_FILE_HANDLE *)demo_fh;

        unlock_shared_storage(&demo_ss->lock);
        return (0);
    }

    /* The file hasn't been opened before, create a new one. */
    if ((demo_fh = calloc(1, sizeof(DEMO_FILE_HANDLE))) == NULL) {
        ret = ENOMEM;
        goto err;
    }

    /* Initialize private information. */
    demo_fh->demo_ss = demo_ss;
    demo_fh->ref = 1;
    if ((demo_fh->buf = calloc(1, DEMO_FILE_SIZE_INCREMENT)) == NULL) {
        ret = ENOMEM;
        goto err;
    }
    demo_fh->bufsize = DEMO_FILE_SIZE_INCREMENT;
    demo_fh->size = 0;

    /* Initialize public information. */
    file_handle = (WT_FILE_HANDLE *)demo_fh;
    if ((file_handle->name = strdup(name)) == NULL) {
        ret = ENOMEM;
        goto err;
    }

    /*
     * Setup the function call table for our custom shared storage. Set the function pointer to NULL
     * where our implementation doesn't support the functionality.
     */
    file_handle->close = demo_file_close;
    file_handle->fh_advise = NULL;
    file_handle->fh_extend = NULL;
    file_handle->fh_extend_nolock = NULL;
    file_handle->fh_lock = demo_file_lock;
    file_handle->fh_map = NULL;
    file_handle->fh_map_discard = NULL;
    file_handle->fh_map_preload = NULL;
    file_handle->fh_unmap = NULL;
    file_handle->fh_read = demo_file_read;
    file_handle->fh_size = demo_file_size;
    file_handle->fh_sync = demo_file_sync;
    file_handle->fh_sync_nowait = NULL;
    file_handle->fh_truncate = demo_file_truncate;
    file_handle->fh_write = demo_file_write;

    TAILQ_INSERT_HEAD(&demo_ss->fileq, demo_fh, q);
    ++demo_ss->opened_unique_object_count;

    *file_handlep = file_handle;

    if (0) {
err:
        free(demo_fh->buf);
        free(demo_fh);
    }

    unlock_shared_storage(&demo_ss->lock);
    return (ret);
}

/*
 * demo_ss_location_handle --
 *     Return a location handle from a location string.
 */
static int
demo_ss_location_handle(WT_SHARED_STORAGE *shared_storage, WT_SESSION *session,
 const char *location_info, void **location_handlep)
{
    size_t len;
    char *p;

    /*
     * Our "handle" is nothing more than the location string followed
     * by a slash delimiter.  We won't allow slashes in the location
     * info parameter.
     */
    if (strchr(location_info, '/') != NULL)
        return (EINVAL);
    len = strlen(location_info) + 2;
    p = malloc(len);
    strncpy(p, location_info, len);
    strncat(p, "/", len);
    *location_handlep = p;
    return (0);
}

/*
 * demo_ss_location_handle_free --
 *     Free a location handle created by ss_location_handle.
 */
static int demo_ss_location_handle_free(WT_SHARED_STORAGE *shared_storage, WT_SESSION *session,
 void *location_handle)
{
    (void)shared_storage; /* Unused */
    (void)session; /* Unused */

    free(location_handle);
    return (0);
}

/*
 * demo_ss_location_list --
 *     Return a list of object names for the given location.
 */
static int
demo_ss_location_list(WT_SHARED_STORAGE *shared_storage, WT_SESSION *session, void *location_handle,
  char ***dirlistp, uint32_t *countp)
{
    DEMO_FILE_HANDLE *demo_fh;
    DEMO_SHARED_STORAGE *demo_ss;
    size_t len;
    uint32_t allocated, count;
    int ret = 0;
    char *location, *name, **entries;
    void *p;

    (void)session; /* Unused */

    demo_ss = (DEMO_SHARED_STORAGE *)shared_storage;

    *dirlistp = NULL;
    *countp = 0;

    entries = NULL;
    allocated = count = 0;
    location = (char *)location_handle;
    len = strlen(location);

    lock_shared_storage(&demo_ss->lock);
    TAILQ_FOREACH (demo_fh, &demo_ss->fileq, q) {
        name = demo_fh->iface.name;
        if (strncmp(name, location, len) != 0)
            continue;
        name += len;

        /*
         * Increase the list size in groups of 10, it doesn't matter if the list is a bit longer
         * than necessary.
         */
        if (count >= allocated) {
            p = realloc(entries, (allocated + 10) * sizeof(*entries));
            if (p == NULL) {
                ret = ENOMEM;
                goto err;
            }

            entries = p;
            memset(entries + allocated * sizeof(*entries), 0, 10 * sizeof(*entries));
            allocated += 10;
        }
        entries[count++] = strdup(name);
    }

    *dirlistp = entries;
    *countp = count;

err:
    unlock_shared_storage(&demo_ss->lock);
    if (ret == 0)
        return (0);

    if (entries != NULL) {
        while (count > 0)
            free(entries[--count]);
        free(entries);
    }

    return (ret);
}

/*
 * demo_ss_location_list_free --
 *     Free memory allocated by demo_ss_location_list.
 */
static int
demo_ss_location_list_free(
  WT_SHARED_STORAGE *shared_storage, WT_SESSION *session, char **dirlist, uint32_t count)
{
    (void)shared_storage;
    (void)session;

    if (dirlist != NULL) {
        while (count > 0)
            free(dirlist[--count]);
        free(dirlist);
    }
    return (0);
}

/*
 * demo_ss_exist --
 *     Return if the file exists.
 */
static int
demo_ss_exist(
  WT_SHARED_STORAGE *shared_storage, WT_SESSION *session, void *location_handle, const char *name, bool *existp)
{
    DEMO_SHARED_STORAGE *demo_ss;

    (void)session; /* Unused */

    demo_ss = (DEMO_SHARED_STORAGE *)shared_storage;

    lock_shared_storage(&demo_ss->lock);
    *existp = demo_handle_search(shared_storage, location_handle, name) != NULL;
    unlock_shared_storage(&demo_ss->lock);

    return (0);
}

/*
 * demo_ss_remove --
 *     POSIX remove.
 */
static int
demo_ss_remove(WT_SHARED_STORAGE *shared_storage, WT_SESSION *session, void *location_handle,
  const char *name, uint32_t flags)
{
    DEMO_SHARED_STORAGE *demo_ss;
    DEMO_FILE_HANDLE *demo_fh;
    int ret = 0;

    (void)session; /* Unused */
    (void)flags;   /* Unused */

    demo_ss = (DEMO_SHARED_STORAGE *)shared_storage;

    ret = ENOENT;
    lock_shared_storage(&demo_ss->lock);
    if ((demo_fh = demo_handle_search(shared_storage, location_handle, name)) != NULL)
        ret = demo_handle_remove(session, demo_fh);
    unlock_shared_storage(&demo_ss->lock);

    return (ret);
}

/*
 * demo_ss_size --
 *     Get the size of a file in bytes, by file name.
 */
static int
demo_ss_size(WT_SHARED_STORAGE *shared_storage, WT_SESSION *session, void *location_handle,
  const char *name, wt_off_t *sizep)
{
    DEMO_SHARED_STORAGE *demo_ss;
    DEMO_FILE_HANDLE *demo_fh;
    int ret = 0;

    demo_ss = (DEMO_SHARED_STORAGE *)shared_storage;

    ret = ENOENT;
    lock_shared_storage(&demo_ss->lock);
    if ((demo_fh = demo_handle_search(shared_storage, location_handle, name)) != NULL)
        ret = demo_file_size((WT_FILE_HANDLE *)demo_fh, session, sizep);
    unlock_shared_storage(&demo_ss->lock);

    return (ret);
}

/*
 * demo_ss_terminate --
 *     Discard any resources on termination
 */
static int
demo_ss_terminate(WT_SHARED_STORAGE *shared_storage, WT_SESSION *session)
{
    DEMO_FILE_HANDLE *demo_fh, *demo_fh_tmp;
    DEMO_SHARED_STORAGE *demo_ss;
    int ret = 0, tret;

    demo_ss = (DEMO_SHARED_STORAGE *)shared_storage;

    TAILQ_FOREACH_SAFE(demo_fh, &demo_ss->fileq, q, demo_fh_tmp)
    if ((tret = demo_handle_remove(session, demo_fh)) != 0 && ret == 0)
        ret = tret;

    printf("Custom shared storage\n");
    printf("\t%d unique object opens\n", demo_ss->opened_unique_object_count);
    printf("\t%d objects opened\n", demo_ss->opened_object_count);
    printf("\t%d objects closed\n", demo_ss->closed_object_count);
    printf("\t%d reads, %d writes\n", demo_ss->read_ops, demo_ss->write_ops);

    destroy_shared_storage_lock(&demo_ss->lock);
    free(demo_ss);

    return (ret);
}

/*
 * demo_file_close --
 *     ANSI C close.
 */
static int
demo_file_close(WT_FILE_HANDLE *file_handle, WT_SESSION *session)
{
    DEMO_FILE_HANDLE *demo_fh;
    DEMO_SHARED_STORAGE *demo_ss;

    (void)session; /* Unused */

    demo_fh = (DEMO_FILE_HANDLE *)file_handle;
    demo_ss = demo_fh->demo_ss;

    lock_shared_storage(&demo_ss->lock);
    if (--demo_fh->ref == 0)
        ++demo_ss->closed_object_count;
    unlock_shared_storage(&demo_ss->lock);

    return (0);
}

/*
 * demo_file_lock --
 *     Lock/unlock a file.
 */
static int
demo_file_lock(WT_FILE_HANDLE *file_handle, WT_SESSION *session, bool lock)
{
    /* Locks are always granted. */
    (void)file_handle; /* Unused */
    (void)session;     /* Unused */
    (void)lock;        /* Unused */
    return (0);
}

/*
 * demo_file_read --
 *     POSIX pread.
 */
static int
demo_file_read(
  WT_FILE_HANDLE *file_handle, WT_SESSION *session, wt_off_t offset, size_t len, void *buf)
{
    DEMO_FILE_HANDLE *demo_fh;
    DEMO_SHARED_STORAGE *demo_ss;
    WT_EXTENSION_API *wtext;
    size_t off;
    int ret = 0;

    demo_fh = (DEMO_FILE_HANDLE *)file_handle;
    demo_ss = demo_fh->demo_ss;
    wtext = demo_ss->wtext;
    off = (size_t)offset;

    lock_shared_storage(&demo_ss->lock);
    ++demo_ss->read_ops;
    if (off < demo_fh->size) {
        if (len > demo_fh->size - off)
            len = demo_fh->size - off;
        memcpy(buf, (uint8_t *)demo_fh->buf + off, len);
    } else
        ret = EIO; /* EOF */
    unlock_shared_storage(&demo_ss->lock);
    if (ret == 0)
        return (0);

    (void)wtext->err_printf(wtext, session,
      "%s: handle-read: failed to read %zu bytes at offset %zu: %s", demo_fh->iface.name, len, off,
      wtext->strerror(wtext, NULL, ret));
    return (ret);
}

/*
 * demo_file_size --
 *     Get the size of a file in bytes, by file handle.
 */
static int
demo_file_size(WT_FILE_HANDLE *file_handle, WT_SESSION *session, wt_off_t *sizep)
{
    DEMO_FILE_HANDLE *demo_fh;
    DEMO_SHARED_STORAGE *demo_ss;

    (void)session; /* Unused */

    demo_fh = (DEMO_FILE_HANDLE *)file_handle;
    demo_ss = demo_fh->demo_ss;

    lock_shared_storage(&demo_ss->lock);
    *sizep = (wt_off_t)demo_fh->size;
    unlock_shared_storage(&demo_ss->lock);
    return (0);
}

/*
 * demo_file_sync --
 *     Ensure the content of the file is stable. This is a no-op in our memory backed shared
 *     storage.
 */
static int
demo_file_sync(WT_FILE_HANDLE *file_handle, WT_SESSION *session)
{
    (void)file_handle; /* Unused */
    (void)session;     /* Unused */

    return (0);
}

/*
 * demo_buffer_resize --
 *     Resize the write buffer.
 */
static int
demo_buffer_resize(WT_SESSION *session, DEMO_FILE_HANDLE *demo_fh, wt_off_t offset)
{
    DEMO_SHARED_STORAGE *demo_ss;
    WT_EXTENSION_API *wtext;
    size_t off;
    void *p;

    demo_ss = demo_fh->demo_ss;
    wtext = demo_ss->wtext;
    off = (size_t)offset;

    /* Grow the buffer as necessary and clear any new space in the file. */
    if (demo_fh->bufsize >= off)
        return (0);

    if ((p = realloc(demo_fh->buf, off)) == NULL) {
        (void)wtext->err_printf(wtext, session, "%s: failed to resize buffer", demo_fh->iface.name,
          wtext->strerror(wtext, NULL, ENOMEM));
        return (ENOMEM);
    }
    memset((uint8_t *)p + demo_fh->bufsize, 0, off - demo_fh->bufsize);
    demo_fh->buf = p;
    demo_fh->bufsize = off;

    return (0);
}

/*
 * demo_file_truncate --
 *     POSIX ftruncate.
 */
static int
demo_file_truncate(WT_FILE_HANDLE *file_handle, WT_SESSION *session, wt_off_t offset)
{
    DEMO_FILE_HANDLE *demo_fh;
    DEMO_SHARED_STORAGE *demo_ss;
    WT_EXTENSION_API *wtext;

    (void)file_handle; /* Unused */
    (void)session;     /* Unused */

    demo_fh = (DEMO_FILE_HANDLE *)file_handle;
    demo_ss = demo_fh->demo_ss;
    wtext = demo_ss->wtext;

    (void)wtext->err_printf(wtext, session, "%s: truncate not supported in shared storage",
      demo_fh->iface.name, wtext->strerror(wtext, NULL, ENOTSUP));
    return (ENOTSUP);
}

/*
 * demo_file_write --
 *     POSIX pwrite.
 */
static int
demo_file_write(
  WT_FILE_HANDLE *file_handle, WT_SESSION *session, wt_off_t offset, size_t len, const void *buf)
{
    DEMO_FILE_HANDLE *demo_fh;
    DEMO_SHARED_STORAGE *demo_ss;
    WT_EXTENSION_API *wtext;
    size_t off;
    int ret = 0;

    demo_fh = (DEMO_FILE_HANDLE *)file_handle;
    demo_ss = demo_fh->demo_ss;
    wtext = demo_ss->wtext;
    off = (size_t)offset;

    lock_shared_storage(&demo_ss->lock);
    ++demo_ss->write_ops;
    if ((ret = demo_buffer_resize(
           session, demo_fh, offset + (wt_off_t)(len + DEMO_FILE_SIZE_INCREMENT))) == 0) {
        memcpy((uint8_t *)demo_fh->buf + off, buf, len);
        if (off + len > demo_fh->size)
            demo_fh->size = off + len;
    }
    unlock_shared_storage(&demo_ss->lock);
    if (ret == 0)
        return (0);

    (void)wtext->err_printf(wtext, session,
      "%s: handle-write: failed to write %zu bytes at offset %zu: %s", demo_fh->iface.name, len,
      off, wtext->strerror(wtext, NULL, ret));
    return (ret);
}

/*
 * demo_handle_remove --
 *     Destroy an in-memory file handle. Should only happen on remove or shutdown.
 */
static int
demo_handle_remove(WT_SESSION *session, DEMO_FILE_HANDLE *demo_fh)
{
    DEMO_SHARED_STORAGE *demo_ss;
    WT_EXTENSION_API *wtext;

    demo_ss = demo_fh->demo_ss;
    wtext = demo_ss->wtext;

    if (demo_fh->ref != 0) {
        (void)wtext->err_printf(wtext, session, "demo_handle_remove: %s: file is currently open",
          demo_fh->iface.name, wtext->strerror(wtext, NULL, EBUSY));
        return (EBUSY);
    }

    TAILQ_REMOVE(&demo_ss->fileq, demo_fh, q);

    /* Clean up private information. */
    free(demo_fh->buf);

    /* Clean up public information. */
    free(demo_fh->iface.name);

    free(demo_fh);

    return (0);
}

/*
 * demo_handle_search --
 *     Return a matching handle, if one exists.
 */
static DEMO_FILE_HANDLE *
demo_handle_search(WT_SHARED_STORAGE *shared_storage, void *location_handle, const char *name)
{
    DEMO_FILE_HANDLE *demo_fh;
    DEMO_SHARED_STORAGE *demo_ss;
    size_t len;
    char *location;

    demo_ss = (DEMO_SHARED_STORAGE *)shared_storage;
    location = (char *)location_handle;
    len = strlen(location);

    TAILQ_FOREACH (demo_fh, &demo_ss->fileq, q)
        if (strncmp(demo_fh->iface.name, location, len) == 0 &&
          strcmp(&demo_fh->iface.name[len], name) == 0)
            break;
    return (demo_fh);
}

static const char *home;

int
main(void)
{
    WT_CONNECTION *conn;
    const char *open_config;
    int ret = 0;
#if 0    
    WT_CURSOR *cursor;
    WT_SESSION *session;
    const char *key, *tier0_uri, *tier1_uri, *uri;
    int i;
    char kbuf[64];
#endif

    fprintf(stderr, "ex_shared_storage: starting\n");
    /*
     * Create a clean test directory for this run of the test program if the environment variable
     * isn't already set (as is done by make check).
     */
    if (getenv("WIREDTIGER_HOME") == NULL) {
        home = "WT_HOME";
        ret = system("rm -rf WT_HOME && mkdir WT_HOME");
    } else
        home = NULL;

    /*! [WT_SHARED_STORAGE register] */
    /*
     * Setup a configuration string that will load our custom shared storage. Use the special local
     * extension to indicate that the entry point is in the same executable. Finally, pass in two
     * pieces of configuration information to our initialization function as the "config" value.
     */
    open_config =
      "create,log=(enabled=true),extensions=(local={entry=demo_shared_storage_create,"
      "config={config_string=\"demo-shared-storage\",config_value=37}})";
    /* Open a connection to the database, creating it if necessary. */
    if ((ret = wiredtiger_open(home, NULL, open_config, &conn)) != 0) {
        fprintf(stderr, "Error connecting to %s: %s\n", home == NULL ? "." : home,
          wiredtiger_strerror(ret));
        return (EXIT_FAILURE);
    }
    /*! [WT_SHARED_STORAGE register] */

    /*
     * At the moment, the infrastructure that would use the shared storage
     * extension does not exist.
     */
#if 0     
    if ((ret = conn->open_session(conn, NULL, NULL, &session)) != 0) {
        fprintf(stderr, "WT_CONNECTION.open_session: %s\n", wiredtiger_strerror(ret));
        return (EXIT_FAILURE);
    }
    uri = "table:ss";
    tier0_uri = "file:ss_tier0.wt";
    tier1_uri = "shared:demo:ss_tier1";

    if ((ret = session->create(session, tier0_uri, "key_format=S,value_format=S")) != 0) {
        fprintf(stderr, "WT_SESSION.create: %s: %s\n", tier_uri, wiredtiger_strerror(ret));
        return (EXIT_FAILURE);
    }
    if ((ret = session->create(session, tier1_uri,
          "shared=(location=encoded_bucket_name_and_auth),key_format=S,value_format=S")) != 0) {
        fprintf(stderr, "WT_SESSION.create: %s: %s\n", tier_uri, wiredtiger_strerror(ret));
        return (EXIT_FAILURE);
    }

    if ((ret = session->create(session, uri, "key_format=S,value_format=S,"
          "type=tiered=(tiers=(\"file:ss_tier0.wt\",\"shared:demo:ss_tier1\")")) != 0) {
        fprintf(stderr, "WT_SESSION.create: %s: %s\n", uri, wiredtiger_strerror(ret));
        return (EXIT_FAILURE);
    }
    if ((ret = session->open_cursor(session, uri, NULL, NULL, &cursor)) != 0) {
        fprintf(stderr, "WT_SESSION.open_cursor: %s: %s\n", uri, wiredtiger_strerror(ret));
        return (EXIT_FAILURE);
    }
    for (i = 0; i < 1000; ++i) {
        (void)snprintf(kbuf, sizeof(kbuf), "%010d KEY -----", i);
        cursor->set_key(cursor, kbuf);
        cursor->set_value(cursor, "--- VALUE ---");
        if ((ret = cursor->insert(cursor)) != 0) {
            fprintf(stderr, "WT_CURSOR.insert: %s: %s\n", kbuf, wiredtiger_strerror(ret));
            return (EXIT_FAILURE);
        }
    }
    if ((ret = cursor->close(cursor)) != 0) {
        fprintf(stderr, "WT_CURSOR.close: %s\n", wiredtiger_strerror(ret));
        return (EXIT_FAILURE);
    }
    if ((ret = session->open_cursor(session, uri, NULL, NULL, &cursor)) != 0) {
        fprintf(stderr, "WT_SESSION.open_cursor: %s: %s\n", uri, wiredtiger_strerror(ret));
        return (EXIT_FAILURE);
    }
    for (i = 0; i < 1000; ++i) {
        if ((ret = cursor->next(cursor)) != 0) {
            fprintf(stderr, "WT_CURSOR.insert: %s: %s\n", kbuf, wiredtiger_strerror(ret));
            return (EXIT_FAILURE);
        }
        (void)snprintf(kbuf, sizeof(kbuf), "%010d KEY -----", i);
        if ((ret = cursor->get_key(cursor, &key)) != 0) {
            fprintf(stderr, "WT_CURSOR.get_key: %s\n", wiredtiger_strerror(ret));
            return (EXIT_FAILURE);
        }
        if (strcmp(kbuf, key) != 0) {
            fprintf(stderr, "Key mismatch: %s, %s\n", kbuf, key);
            return (EXIT_FAILURE);
        }
    }
    if ((ret = cursor->next(cursor)) != WT_NOTFOUND) {
        fprintf(
          stderr, "WT_CURSOR.insert: expected WT_NOTFOUND, got %s\n", wiredtiger_strerror(ret));
        return (EXIT_FAILURE);
    }
#endif

    if ((ret = conn->close(conn, NULL)) != 0) {
        fprintf(stderr, "Error closing connection to %s: %s\n", home == NULL ? "." : home,
          wiredtiger_strerror(ret));
        return (EXIT_FAILURE);
    }

    return (EXIT_SUCCESS);
}
