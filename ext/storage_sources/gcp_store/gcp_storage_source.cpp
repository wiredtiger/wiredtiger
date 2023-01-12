#include <wiredtiger.h>
#include <wiredtiger_ext.h>
#include <fstream>
#include <list>
#include <errno.h>
#include <filesystem>
#include <mutex>

#include "gcp_connection.h"
#include "wt_internal.h"

struct GCPStorage {
    WT_STORAGE_SOURCE store;
    std::list<GCPFileSystem *> gcp_fs;
}

struct GCPFileSystem {
    WT_FILE_SYSTEM fs;
    std::list<GCP_FILE_HANDLE *> gcp_fh;
    GCPConnection *gcp_conn;
}

struct GCPFileHandle {
    WT_FILE_HANDLE fh;
    GCP_STORE *store;
}

static int
GCPCustomizeFileSystem(
  WT_STORAGE_SOURCE *, WT_SESSION *, const char *, const char *, const char *, WT_FILE_SYSTEM **);
static int GCPAddReference(WT_STORAGE_SOURCE *);
static int GCPFileSystemTerminate(WT_FILE_SYSTEM *, WT_SESSION *);
static int GCPFlush(
  WT_STORAGE_SOURCE *, WT_SESSION *, WT_FILE_SYSTEM *, const char *, const char *, const char *);
static int GCPFlushFinish(
  WT_STORAGE_SOURCE *, WT_SESSION *, WT_FILE_SYSTEM *, const char *, const char *, const char *);
static int GCPFileExists(WT_FILE_SYSTEM *, WT_SESSION *, const char *, bool *);
static int GCPFileOpen(
  WT_FILE_SYSTEM *, WT_SESSION *, const char *, WT_FS_OPEN_FILE_TYPE, uint32_t, WT_FILE_HANDLE **);
static int GCPRemove(WT_FILE_SYSTEM *, WT_SESSION *, const char *, uint32_t);
static int GCPRename(WT_FILE_SYSTEM *, WT_SESSION *, const char *, const char *, uint32_t);
static int GCPFileSize(WT_FILE_HANDLE *, WT_SESSION *, wt_off_t *);
static int GCPObjectListAdd(
  const GCPStorage &, char ***, const std::vector<std::string> &, const uint32_t);
static int GCPObjectListSingle(
  WT_FILE_SYSTEM *, WT_SESSION *, const char *, const char *, char ***, uint32_t *);
static int GCPObjectListFree(WT_FILE_SYSTEM *, WT_SESSION *, char **, uint32_t);

static int
GCPCustomizeFileSystem(WT_STORAGE_SOURCE *store, WT_SESSION *session, const char *bucket,
  const char *authToken, const char *config, WT_FILE_SYSTEM **fileSystem)
{
    WT_UNUSED(store);
    WT_UNUSED(session);
    WT_UNUSED(bucket);
    WT_UNUSED(authToken);
    WT_UNUSED(config);
    WT_UNUSED(fileSystem);
    return (0);
}

static int
GCPAddReference(WT_STORAGE_SOURCE *store)
{
    WT_UNUSED(store);
    return (0);
}

static int
GCPFileSystemTerminate(WT_FILE_SYSTEM *fileSystem, WT_SESSION *session)
{
    WT_UNUSED(fileSystem);
    WT_UNUSED(session);
    return (0);
}

static int
GCPFlush(WT_STORAGE_SOURCE *storageSource, WT_SESSION *session, WT_FILE_SYSTEM *fileSystem,
  const char *source, const char *object, const char *config)
{
    WT_UNUSED(storageSource);
    WT_UNUSED(session);
    WT_UNUSED(fileSystem);
    WT_UNUSED(source);
    WT_UNUSED(object);
    WT_UNUSED(config);
    return (0);
}

static int
GCPFlushFinish(WT_STORAGE_SOURCE *storage, WT_SESSION *session, WT_FILE_SYSTEM *fileSystem,
  const char *source, const char *object, const char *config)
{
    WT_UNUSED(storage);
    WT_UNUSED(session);
    WT_UNUSED(fileSystem);
    WT_UNUSED(source);
    WT_UNUSED(object);
    WT_UNUSED(config);
    return (0);
}

static int
GCPFileExists(WT_FILE_SYSTEM *fileSystem, WT_SESSION *session, const char *name, bool *fileExists)
{
    WT_UNUSED(fileSystem);
    WT_UNUSED(session);
    WT_UNUSED(name);
    WT_UNUSED(fileExists);
    return (0);
}

static int
GCPFileOpen(WT_FILE_SYSTEM *fileSystem, WT_SESSION *session, const char *name,
  WT_FS_OPEN_FILE_TYPE fileType, uint32_t flags, WT_FILE_HANDLE **fileHandlePtr)
{
    WT_UNUSED(fileSystem);
    WT_UNUSED(session);
    WT_UNUSED(name);
    WT_UNUSED(fileType);
    WT_UNUSED(flags);
    WT_UNUSED(fileHandlePtr);
    return (0);
}

static int
GCPRemove(WT_FILE_SYSTEM *file_system, WT_SESSION *session, const char *name, uint32_t flags)
{
    WT_UNUSED(file_system);
    WT_UNUSED(session);
    WT_UNUSED(name);
    WT_UNUSED(flags);
    return (0);
}

static int
GCPRename(WT_FILE_SYSTEM *file_system, WT_SESSION *session, const char *from, const char *to,
  uint32_t flags)
{
    WT_UNUSED(file_system);
    WT_UNUSED(session);
    WT_UNUSED(from);
    WT_UNUSED(to);
    WT_UNUSED(flags);
    return (0);
}

static int
GCPFileSize(WT_FILE_HANDLE *fileHandle, WT_SESSION *session, wt_off_t *sizep)
{
    WT_UNUSED(fileHandle);
    WT_UNUSED(session);
    WT_UNUSED(sizep);
    return (0);
}

static int
GCPObjectListAdd(const GCPStorage &GCP, char ***objectList, const std::vector<std::string> &objects,
  const uint32_t count)
{
    WT_UNUSED(GCP);
    WT_UNUSED(objectList);
    WT_UNUSED(objects);
    WT_UNUSED(count);
    return (0);
}

static int
GCPObjectListSingle(WT_FILE_SYSTEM *fileSystem, WT_SESSION *session, const char *directory,
  const char *prefix, char ***objectList, uint32_t *count)
{
    WT_UNUSED(fileSystem);
    WT_UNUSED(session);
    WT_UNUSED(directory);
    WT_UNUSED(prefix);
    WT_UNUSED(objectList);
    WT_UNUSED(count);
    return (0);
}

static int
GCPObjectListFree(
  WT_FILE_SYSTEM *fileSystem, WT_SESSION *session, char **objectList, uint32_t count)
{
    WT_UNUSED(fileSystem);
    WT_UNUSED(session);
    WT_UNUSED(objectList);
    WT_UNUSED(count);
    return (0);
}
