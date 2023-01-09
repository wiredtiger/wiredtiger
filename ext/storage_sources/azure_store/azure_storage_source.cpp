
#include <wiredtiger.h>
#include <wiredtiger_ext.h>
#include <list>

#include "azure_connection.h"

struct AZURE_STORE {
    WT_STORAGE_SOURCE store;
    std::list<AZURE_FILE_SYSTEM*> azure_fs;
    // AZURE_LOG_SYSTEM *log;
};

struct AZURE_FILE_SYSTEM {
    WT_FILE_SYSTEM fs;
    struct AZURE_STORE store;
    std::list<AZURE_FILE_HANDLE> azure_fh;
    AzureConnection* azure_conn;
};

struct AZURE_FILE_HANDLE {
    WT_FILE_HANDLE fh;
    struct AZURE_STORE store;
};