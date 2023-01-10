#include <wiredtiger.h>
#include <wiredtiger_ext.h>
#include <fstream>
#include <list>
#include <errno.h>
#include <filesystem>
#include <mutex>

#include "gcp_connection.h"
#include "gcp_log_system.h"

#define UNUSED(x) (void)(x)

struct GCPStorage {
    WT_STORAGE_SOURCE store;
    std::list<GCPFileSystem *> gcp_fs;
    GCP_LOG_SYSTEM *log;
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

static int GCPCustomizeFileSystem(WT_STORAGE_SOURCE *, WT_SESSION *, const char *, const char *, const char *, WT_FILE_SYSTEM **);
static int GCPAddReference(WT_STORAGE_SOURCE *);
static int GCPFileSystemTerminate(WT_FILE_SYSTEM *, WT_SESSION *);
static int GCPFlush(WT_STORAGE_SOURCE *, WT_SESSION *, WT_FILE_SYSTEM *, const char *, const char *, const char *);
static int GCPFlushFinish(WT_STORAGE_SOURCE *, WT_SESSION *, WT_FILE_SYSTEM *, const char *, const char *, const char *);

static int 
GCPCustomizeFileSystem(WT_STORAGE_SOURCE *store, WT_SESSION *session, const char *bucket, const char *authToken, const char *config, WT_FILE_SYSTEM **fileSystem){
    UNUSED(store);
    UNUSED(session);
    UNUSED(bucket);
    UNUSED(authToken);
    UNUSED(config);
    UNUSED(fileSystem);
    return (0);
}

static int 
GCPAddReference(WT_STORAGE_SOURCE *store) {
    UNUSED(store);
    return (0);
}

static int
GCPFileSystemTerminate(WT_FILE_SYSTEM *fileSystem, WT_SESSION *session)
{
    UNUSED(fileSystem);
    UNUSED(session);
    return (0);
}

static int
GCPFlush(WT_STORAGE_SOURCE *storageSource, WT_SESSION *session, WT_FILE_SYSTEM *fileSystem,
  const char *source, const char *object, const char *config) {
    UNUSED(storageSource);
    UNUSED(session);
    UNUSED(fileSystem);
    UNUSED(source);
    UNUSED(object);
    UNUSED(config);
    return (0);
}

static int
GCPFlushFinish(WT_STORAGE_SOURCE *storage, WT_SESSION *session, WT_FILE_SYSTEM *fileSystem,
  const char *source, const char *object, const char *config) {
    UNUSED(storage);
    UNUSED(session);
    UNUSED(fileSystem);
    UNUSED(source);
    UNUSED(object);
    UNUSED(config);
    return (0);
}
