#include <wiredtiger.h>
#include <wiredtiger_ext.h>
#include <fstream>
#include <list>
#include <errno.h>
#include <filesystem>
#include <mutex>

#include "gcp_connection.h"
#include "gcp_log_system.h"

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