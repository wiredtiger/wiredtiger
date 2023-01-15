
#include <wiredtiger.h>
#include <wiredtiger_ext.h>
#include <list>

// #include "azure_connection.h"
#include "wt_internal.h"

struct AZURE_STORE {
    WT_STORAGE_SOURCE store;
    std::list<AZURE_FILE_SYSTEM *> azure_fs;
    // AZURE_LOG_SYSTEM *log;
};

struct AZURE_FILE_SYSTEM {
    WT_FILE_SYSTEM fs;
    azure_store *store;
    std::vector<azure_file_handle> azure_fh;
    // azure_connection *azure_conn;
};

struct AZURE_FILE_HANDLE {
    WT_FILE_HANDLE fh;
    struct AZURE_STORE store;
};

// WT_STORAGE_SOURCE Interface
static int AzureCustomizeFileSystem(
  WT_STORAGE_SOURCE *, WT_SESSION *, const char *, const char *, const char *, WT_FILE_SYSTEM **);
static int AzureAddReference(WT_STORAGE_SOURCE *);
static int AzureTerminate(WT_FILE_SYSTEM *, WT_SESSION *);
static int AzureFlush(
  WT_STORAGE_SOURCE *, WT_SESSION *, WT_FILE_SYSTEM *, const char *, const char *, const char *);
static int AzureFlushFinish(
  WT_STORAGE_SOURCE *, WT_SESSION *, WT_FILE_SYSTEM *, const char *, const char *, const char *);

// WT_FILE_SYSTEM Interface
static int AzureObjectList(
  WT_FILE_SYSTEM *, WT_SESSION *, const char *, const char *, char ***, uint32_t *);
static int AzureObjectListSingle(
  WT_FILE_SYSTEM *, WT_SESSION *, const char *, const char *, char ***, uint32_t *);
static int AzureObjectListFree(WT_FILE_SYSTEM *, WT_SESSION *, char **, uint32_t);
static int AzureFileSystemTerminate(WT_FILE_SYSTEM *, WT_SESSION *);
static int AzureFileExists(WT_FILE_SYSTEM *, WT_SESSION *, const char *, bool *);
static int AzureRemove(WT_FILE_SYSTEM *, WT_SESSION *, const char *, uint32_t);
static int AzureRename(WT_FILE_SYSTEM *, WT_SESSION *, const char *, const char *, uint32_t);
static int AzureObjectSize(WT_FILE_SYSTEM *, WT_SESSION *, const char *, wt_off_t *);
static int AzureFileOpen(
  WT_FILE_SYSTEM *, WT_SESSION *, const char *, WT_FS_OPEN_FILE_TYPE, uint32_t, WT_FILE_HANDLE **);

// WT_FILE_HANDLE Interface
static int AzureFileClose(WT_FILE_HANDLE *, WT_SESSION *);
static int AzureFileLock(WT_FILE_HANDLE *, WT_SESSION *, bool);
static int AzureFileRead(WT_FILE_HANDLE *, WT_SESSION *, wt_off_t, size_t, void *);
static int AzureFileSize(WT_FILE_HANDLE *, WT_SESSION *, wt_off_t *);

static int
AzureCustomizeFileSystem(WT_STORAGE_SOURCE *storage_source, WT_SESSION *session,
  const char *bucket_name, const char *auth_token, const char *config,
  WT_FILE_SYSTEM **file_systemp)
{
    WT_UNUSED(session);
    WT_UNUSED(bucket_name);
    WT_UNUSED(auth_token);
    WT_UNUSED(config);
    WT_UNUSED(file_systemp);

    AZURE_STORE *azure;
    azure = (AZURE_STORE *)storage_source;

    AZURE_FILE_SYSTEM *fileSystem;

    fileSystem->store = azure;
    fileSystem->fs.fs_directory_list = AzureObjectList;
    fileSystem->fs.fs_directory_list_single = AzureObjectListSingle;
    fileSystem->fs.fs_directory_list_free = AzureObjectListFree;
    fileSystem->fs.terminate = AzureFileSystemTerminate;
    fileSystem->fs.fs_exist = AzureFileExists;
    fileSystem->fs.fs_open_file = AzureFileOpen;
    fileSystem->fs.fs_remove = AzureRemove;
    fileSystem->fs.fs_rename = AzureRename;
    fileSystem->fs.fs_size = AzureObjectSize;

    return 0;
}

static int
AzureAddReference(WT_STORAGE_SOURCE *storage_source)
{
    WT_UNUSED(storage_source)
    return 0;
}

static int
AzureFileSystemTerminate(WT_FILE_SYSTEM *file_system, WT_SESSION *session)
{
    WT_UNUSED(file_system);
    WT_UNUSED(session);
    return 0;
}

static int
AzureFlush(WT_STORAGE_SOURCE *storage_source, WT_SESSION *session, WT_FILE_SYSTEM *file_system,
  const char *source, const char *object, const char *config)
{
    WT_UNUSED(storage_source);
    WT_UNUSED(session);
    WT_UNUSED(file_system);
    WT_UNUSED(source);
    WT_UNUSED(object);
    WT_UNUSED(config);
    return 0;
}

static int
AzureFlushFinish(WT_STORAGE_SOURCE *storage_source, WT_SESSION *session,
  WT_FILE_SYSTEM *file_system, const char *source, const char *object, const char *config)
{
    WT_UNUSED(storage_source);
    WT_UNUSED(session);
    WT_UNUSED(file_system);
    WT_UNUSED(source);
    WT_UNUSED(object);
    WT_UNUSED(config);
    return 0;
}

static int
AzureObjectList(WT_FILE_SYSTEM *file_system, WT_SESSION *session, const char *directory,
  const char *prefix, char ***dirlistp, uint32_t *countp)
{
    WT_UNUSED(file_system);
    WT_UNUSED(session);
    WT_UNUSED(directory);
    WT_UNUSED(prefix);
    WT_UNUSED(dirlistp);
    WT_UNUSED(countp);
    return 0;
}

static int
AzureObjectListSingle(WT_FILE_SYSTEM *file_system, WT_SESSION *session, const char *directory,
  const char *prefix, char ***dirlistp, uint32_t *countp)
{
    WT_UNUSED(file_system);
    WT_UNUSED(session);
    WT_UNUSED(directory);
    WT_UNUSED(prefix);
    WT_UNUSED(dirlistp);
    WT_UNUSED(countp);
    return 0;
}

static int
AzureObjectListFree(
  WT_FILE_SYSTEM *file_system, WT_SESSION *session, char **dirlist, uint32_t count)
{
    WT_UNUSED(file_system);
    WT_UNUSED(session);
    WT_UNUSED(dirlist);
    WT_UNUSED(count);
    return 0;
}

static int
AzureFileSystemTerminate(WT_FILE_SYSTEM *file_system, WT_SESSION *session)
{
    WT_UNUSED(file_system);
    WT_UNUSED(session);
    return 0;
}

static int
AzureFileExists(WT_FILE_SYSTEM *file_system, WT_SESSION *session, const char *name, bool *existp)
{
    WT_UNUSED(file_system);
    WT_UNUSED(session);
    WT_UNUSED(name);
    WT_UNUSED(existp);
    return 0;
}

static int
AzureRemove(WT_FILE_SYSTEM *file_system, WT_SESSION *session, const char *name, uint32_t flags)
{
    WT_UNUSED(file_system);
    WT_UNUSED(session);
    WT_UNUSED(name);
    WT_UNUSED(flags);
    return 0;
}

static int
AzureRename(WT_FILE_SYSTEM *file_system, WT_SESSION *session, const char *from, const char *to,
  uint32_t flags)
{
    WT_UNUSED(file_system);
    WT_UNUSED(session);
    WT_UNUSED(from);
    WT_UNUSED(to);
    WT_UNUSED(flags);
    return 0;
}

static int
AzureObjectSize(WT_FILE_SYSTEM *file_system, WT_SESSION *session, const char *name, wt_off_t *sizep)
{
    WT_UNUSED(file_system);
    WT_UNUSED(session);
    WT_UNUSED(name);
    WT_UNUSED(sizep);
    return 0;
}

static int
AzureFileOpen(WT_FILE_SYSTEM *file_system, WT_SESSION *session, const char *name,
  WT_FS_OPEN_FILE_TYPE file_type, uint32_t flags, WT_FILE_HANDLE **file_handlep)
{
    WT_FILE_HANDLE *fileHandle;
    fileHandle->close = AzureFileClose;
    fileHandle->fh_advise = nullptr;
    fileHandle->fh_extend = nullptr;
    fileHandle->fh_extend_nolock = nullptr;
    fileHandle->fh_lock = AzureFileLock;
    fileHandle->fh_map = nullptr;
    fileHandle->fh_map_discard = nullptr;
    fileHandle->fh_map_preload = nullptr;
    fileHandle->fh_unmap = nullptr;
    fileHandle->fh_read = AzureFileRead;
    fileHandle->fh_size = AzureFileSize;
    fileHandle->fh_sync = nullptr;
    fileHandle->fh_sync_nowait = nullptr;
    fileHandle->fh_truncate = nullptr;
    fileHandle->fh_write = nullptr;
    return 0;
}

static int
AzureFileClose(WT_FILE_HANDLE *fileHandle, WT_SESSION *session)
{
    WT_UNUSED(fileHandle);
    WT_UNUSED(session);
    return 0;
}

static int
AzureFileLock(WT_FILE_HANDLE *fileHandle, WT_SESSION *session, bool lock)
{
    WT_UNUSED(fileHandle);
    WT_UNUSED(session);
    WT_UNUSED(lock);
    return 0;
}

static int
AzureFileRead(
  WT_FILE_HANDLE *fileHandle, WT_SESSION *session, wt_off_t offset, size_t len, void *buf)
{
    WT_UNUSED(fileHandle);
    WT_UNUSED(session);
    WT_UNUSED(offset);
    WT_UNUSED(len);
    WT_UNUSED(buf);
    return 0;
}

static int
AzureFileSize(WT_FILE_HANDLE *fileHandle, WT_SESSION *session, wt_off_t *sizep)
{
    WT_UNUSED(fileHandle);
    WT_UNUSED(session);
    WT_UNUSED(sizep);
    return 0;
}

int
wiredtiger_extension_init(WT_CONNECTION *connection, WT_CONFIG_ARG *config)
{
    WT_UNUSED(connection);
    WT_UNUSED(config);

    AZURE_STORE *azure;
    azure->store.ss_customize_file_system = AzureCustomizeFileSystem;
    azure->store.ss_add_reference = AzureAddReference;
    azure->store.terminate = AzureTerminate;
    azure->store.ss_flush = AzureFlush;
    azure->store.ss_flush_finish = AzureFlushFinish;
    return 0;
}
