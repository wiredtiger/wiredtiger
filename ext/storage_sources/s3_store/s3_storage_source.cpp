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

#include <wiredtiger.h>
#include <wiredtiger_ext.h>
#include <sys/stat.h>
#include <fstream>
#include <errno.h>
#include <list>
#include <unistd.h>

#include "s3_connection.h"
#include "s3_log_system.h"
#include <aws/core/Aws.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/logging/AWSLogging.h>

#define UNUSED(x) (void)(x)
#define FS2S3(fs) (((S3_FILE_SYSTEM *)(fs))->storage)

struct S3_FILE_HANDLE;

/* S3 storage source structure. */
struct S3_STORAGE {
    WT_STORAGE_SOURCE storageSource; /* Must come first */
    WT_EXTENSION_API *wtApi;         /* Extension API */
    int32_t verbose;
    std::list<S3_FILE_HANDLE *> fhList;
};

struct S3_FILE_SYSTEM{
    /* Must come first - this is the interface for the file system we are implementing. */
    WT_FILE_SYSTEM fileSystem;
    S3_STORAGE *storage;
    WT_FILE_SYSTEM *wtFs;

    S3Connection *connection;
    S3LogSystem *log;
    std::string bucketName;
    std::string cacheDir; /* Directory for cached objects */
    std::string homeDir;  /* Owned by the connection */
};

struct S3_FILE_HANDLE {
    WT_FILE_HANDLE iface; /* Must come first */
    S3_STORAGE *storage; /* Enclosing storage source */
    WT_FILE_HANDLE *fileHandle; /* File handle */
};

/* Configuration variables for connecting to S3CrtClient. */
const Aws::String region = Aws::Region::AP_SOUTHEAST_2;
const double throughputTargetGbps = 5;
const uint64_t partSize = 8 * 1024 * 1024; /* 8 MB. */

/* Setting SDK options. */
Aws::SDKOptions options;

static int S3GetDirectory(const std::string &, const std::string &, bool, std::string &);
static bool S3CacheExists(WT_FILE_SYSTEM *, const std::string &);
static std::string S3Path(const std::string &, const std::string &);
static std::string S3HomePath(WT_FILE_SYSTEM *, const char *);
static std::string S3CachePath(WT_FILE_SYSTEM *, const char *);
static int S3Exist(WT_FILE_SYSTEM *, WT_SESSION *, const char *, bool *);
static int S3CustomizeFileSystem(
  WT_STORAGE_SOURCE *, WT_SESSION *, const char *, const char *, const char *, WT_FILE_SYSTEM **);
static int S3AddReference(WT_STORAGE_SOURCE *);
static int S3FileSystemTerminate(WT_FILE_SYSTEM *, WT_SESSION *);
static int S3Open(WT_FILE_SYSTEM *, WT_SESSION *, const char *,
  WT_FS_OPEN_FILE_TYPE , uint32_t , WT_FILE_HANDLE **);
static bool FileExists(std::string);
static int
local_open(WT_FILE_SYSTEM *file_system, WT_SESSION *session, const char *name,
  WT_FS_OPEN_FILE_TYPE file_type, uint32_t flags, WT_FILE_HANDLE **file_handlep);

static int S3ObjectList(
  WT_FILE_SYSTEM *, WT_SESSION *, const char *, const char *, char ***, uint32_t *);
static int S3ObjectListAdd(
  S3_STORAGE *, char ***, const std::vector<std::string> &, const uint32_t);
static int S3ObjectListSingle(
  WT_FILE_SYSTEM *, WT_SESSION *, const char *, const char *, char ***, uint32_t *);
static int S3ObjectListFree(WT_FILE_SYSTEM *, WT_SESSION *, char **, uint32_t);

/*
 *   S3Exist--
 *     Return if the file exists. First checks the cache, and then the S3 Bucket.
 */
static int
S3Exist(WT_FILE_SYSTEM *fileSystem, WT_SESSION *session, const char *name, bool *exist)
{
    S3_STORAGE *s3;
    int ret;
    s3 = FS2S3(fileSystem);
    S3_FILE_SYSTEM *fs = (S3_FILE_SYSTEM *)fileSystem;

    /* It's not in the cache, try the S3 bucket. */
    *exist = S3CacheExists(fileSystem, name);
    if (!*exist)
        ret = fs->connection->ObjectExists(fs->bucketName, name, *exist);

    return (ret);
}

/*
 * S3Path --
 *     Construct a pathname from the directory and the object name.
 */
static std::string
S3Path(const std::string &dir, const std::string &name)
{
    std::cout << "Concatenating " << dir << " and " << name << std::endl;
    /* Skip over "./" and variations (".//", ".///./././//") at the beginning of the name. */
    int i = 0;
    while (name[i] == '.') {
        if (name[1] != '/')
            break;
        i += 2;
        while (name[i] == '/')
            i++;
    }
    std::string strippedName = name.substr(i, name.length() - i);
    return (dir + "/" + strippedName);
}

/*
 * S3CacheExists --
 *     Checks whether the given file exists in the cache.
 */
static bool
S3CacheExists(WT_FILE_SYSTEM *fileSystem, const std::string &name)
{
    std::string path = S3Path(((S3_FILE_SYSTEM *)fileSystem)->cacheDir, name);
    return (FileExists(path));
}

/*
 * FileExists --
 *     Checks whether the given file exists.
 */
static bool
FileExists(std::string path) {
    std::ifstream f(path);
    return (f.good());
}

/*
 * S3GetDirectory --
 *     Return a copy of a directory name after verifying that it is a directory.
 */
static int
S3GetDirectory(const std::string &home, const std::string &name, bool create, std::string &copy)
{
    copy = "";

    struct stat sb;
    int ret;
    std::string dirName;

    /* For relative pathnames, the path is considered to be relative to the home directory. */
    if (name[0] == '/')
        dirName = name;
    else
        dirName = home + "/" + name;

    ret = stat(dirName.c_str(), &sb);
    if (ret != 0 && errno == ENOENT && create) {
        (void)mkdir(dirName.c_str(), 0777);
        ret = stat(dirName.c_str(), &sb);
    }

    if (ret != 0)
        ret = errno;
    else if ((sb.st_mode & S_IFMT) != S_IFDIR)
        ret = EINVAL;

    copy = dirName;
    return (ret);
}

/*
 * S3Open --
 *    Open for the s3 storage source
 */ 
static int
S3Open(WT_FILE_SYSTEM *file_system, WT_SESSION *session, const char *name,
  WT_FS_OPEN_FILE_TYPE file_type, uint32_t flags, WT_FILE_HANDLE **file_handlep)
{
    std::cout << "S3Open() - Start" << std::endl;
    S3_FILE_HANDLE *s3FileHandle;
    S3_FILE_SYSTEM *s3Fs = (S3_FILE_SYSTEM *)file_system;
    S3_STORAGE *s3 = s3Fs->storage;
    WT_FILE_SYSTEM *wtFs = s3Fs->wtFs;
    WT_FILE_HANDLE *wtFh, *fileHandle;

    std::string bucketPath;
    bool exists = false;
    int ret = 0;

    *file_handlep = NULL;

    if ((flags & WT_FS_OPEN_READONLY) == 0 || (flags & WT_FS_OPEN_CREATE) != 0)
        std::cout << "ss_open_object: readonly access required: " <<  name << std::endl;

    if (file_type != WT_FS_OPEN_FILE_TYPE_DATA && file_type != WT_FS_OPEN_FILE_TYPE_REGULAR)
        std::cout << name << ": open: only data file and regular types supported" << std::endl;


    if ((s3FileHandle = (S3_FILE_HANDLE *)calloc(1, sizeof(S3_FILE_HANDLE))) == NULL)
        return ENOMEM;

    std::string cachePath = S3Path(s3Fs->cacheDir, name);

    std::cout << "S3Open: cachePath=" << cachePath << std::endl;
    
    /* File doesn't exist locally. Make a copy from S3. */
    if (!FileExists(cachePath)) {
        std::cout << "S3Open: " << name << " doesn't exist at " << cachePath << std::endl;
        if ((ret = s3Fs->connection->GetObject(s3Fs->bucketName, name, cachePath)) != 0){
            std::cout << "S3Open: ObjectExists() failure" << std::endl;
            return (ret);
        }
    } else 
        std::cout << "S3Open: Found " << name << "in cache. File exists locally." << std::endl;

    std::cout << "S3Open: Opening WiredTiger's native file handle: " << cachePath << std::endl;
    if ((ret = wtFs->fs_open_file(wtFs, session, cachePath.c_str(), file_type, flags, &wtFh)) != 0)
        return (ret);
    std::cout << "S3Open: after fs_open_file() ret=" << ret << std::endl;
    std::cout << "S3Open: Opened WiredTiger's native file handle: " << cachePath << std::endl;

    s3FileHandle->fileHandle = wtFh;
    s3FileHandle->storage = s3;

    fileHandle = (WT_FILE_HANDLE *)s3FileHandle;

    fileHandle->close = NULL;
    fileHandle->fh_advise = NULL;
    fileHandle->fh_extend = NULL;
    fileHandle->fh_extend_nolock = NULL;
    fileHandle->fh_lock = NULL;
    fileHandle->fh_map = NULL;
    fileHandle->fh_map_discard = NULL;
    fileHandle->fh_map_preload = NULL;
    fileHandle->fh_unmap = NULL;
    fileHandle->fh_read = NULL;
    fileHandle->fh_size = NULL;
    fileHandle->fh_sync = NULL;
    fileHandle->fh_sync_nowait = NULL;
    fileHandle->fh_truncate = NULL;
    fileHandle->fh_write = NULL;

    std::cout << "S3Open: FH Interface assigned." << std::endl;
    if ((fileHandle->name = strdup(name)) == NULL){
        std::cout << "error in strdup()" << std::endl;
        return ENOMEM;
    }

    s3FileHandle->storage->fhList.push_front(s3FileHandle);
    *file_handlep = fileHandle;

    std::cout << "S3Open: File opened - " << name << " - final path: " << fileHandle->name << std::endl;

    wtFh->close(wtFh, session);

    return (0);
}

/*
 * S3CustomizeFileSystem --
 *     Return a customized file system to access the s3 storage source objects.
 */
static int
S3CustomizeFileSystem(WT_STORAGE_SOURCE *storageSource, WT_SESSION *session, const char *bucketName,
  const char *authToken, const char *config, WT_FILE_SYSTEM **fileSystem)
{
    std::cout << "S3CustomizeFileSystem()" << std::endl;
    S3_STORAGE *s3;
    S3_FILE_SYSTEM *fs;
    WT_FILE_SYSTEM *wt_fs;
    int ret;
    WT_CONFIG_ITEM cacheDir;
    std::string cacheStr;

    s3 = (S3_STORAGE *)storageSource;

    /* Mark parameters as unused for now, until implemented. */
    UNUSED(authToken);

    Aws::S3Crt::ClientConfiguration awsConfig;
    awsConfig.region = region;
    awsConfig.throughputTargetGbps = throughputTargetGbps;
    awsConfig.partSize = partSize;

    /* Parse configuration string. */
    ret = s3->wtApi->config_get_string(s3->wtApi, session, config, "cache_directory", &cacheDir);
    if (ret == 0)
        cacheStr = cacheDir.str;
    else if (ret == WT_NOTFOUND)
        ret = 0;
    else
        return (ret);

    Aws::Utils::Logging::InitializeAWSLogging(
      Aws::MakeShared<S3LogSystem>("storage", s3->wtApi, s3->verbose));
    
    if ((ret = s3->wtApi->file_system_get(s3->wtApi, session, &wt_fs)) != 0) 
        return (ret);

    if ((fs = (S3_FILE_SYSTEM *)calloc(1, sizeof(S3_FILE_SYSTEM))) == NULL)
        return (errno);
    fs->storage = s3;
    fs->wtFs = wt_fs;

    /* Store a copy of the home directory and bucket name in the file system. */
    fs->homeDir = session->connection->get_home(session->connection);
    fs->bucketName = bucketName;

    /*
     * The default cache directory is named "cache-<name>", where name is the last component of the
     * bucket name's path. We'll create it if it doesn't exist.
     */
    if (cacheStr.empty()) {
        cacheStr = "cache-" + fs->bucketName;
        fs->cacheDir = cacheStr;
    }
    if ((ret = S3GetDirectory(fs->homeDir, cacheStr, true, fs->cacheDir)) != 0)
        return (ret);

    /* New can fail; will deal with this later. */
    fs->connection = new S3Connection(awsConfig);
    fs->fileSystem.fs_directory_list = S3ObjectList;
    fs->fileSystem.fs_directory_list_single = S3ObjectListSingle;
    fs->fileSystem.fs_directory_list_free = S3ObjectListFree;
    fs->fileSystem.terminate = S3FileSystemTerminate;
    fs->fileSystem.fs_exist = S3Exist;
    fs->fileSystem.fs_open_file = S3Open;

    /* TODO: Move these into tests. Just testing here temporarily to show all functions work. */
    {
        std::vector<std::string> buckets;
        fs->connection->ListBuckets(buckets);
        std::cout << "All buckets under my account:" << std::endl;
        for (const std::string &bucket : buckets)
            std::cout << "  * " << bucket << std::endl;
        std::cout << std::endl;

        /* Have at least one bucket to use. */
        if (!buckets.empty()) {
            const std::string firstBucket = buckets.at(0);

            /* Put object. */
            fs->connection->PutObject(firstBucket, "WiredTiger.turtle", "WiredTiger.turtle");

            /* Testing directory list. */
            WT_SESSION *session = NULL;
            const char *prefix = "WiredTiger";
            char **objectList;
            uint32_t count;

            fs->fileSystem.fs_directory_list(
              &fs->fileSystem, session, firstBucket.c_str(), prefix, &objectList, &count);
            std::cout << "Objects in bucket '" << firstBucket << "':" << std::endl;
            for (int i = 0; i < count; i++)
                std::cout << (objectList)[i] << std::endl;

            std::cout << "Number of objects retrieved: " << count << std::endl;
            fs->fileSystem.fs_directory_list_free(&fs->fileSystem, session, objectList, count);

            fs->fileSystem.fs_directory_list_single(
              &fs->fileSystem, session, firstBucket.c_str(), prefix, &objectList, &count);

            std::cout << "Objects in bucket '" << firstBucket << "':" << std::endl;
            for (int i = 0; i < count; i++)
                std::cout << (objectList)[i] << std::endl;

            std::cout << "Number of objects retrieved: " << count << std::endl;
            fs->fileSystem.fs_directory_list_free(&fs->fileSystem, session, objectList, count);

            /* Delete object. */
            fs->connection->DeleteObject(firstBucket, "WiredTiger.turtle");
        } else
            std::cout << "No buckets in AWS account." << std::endl;
    }

    *fileSystem = &fs->fileSystem;
    return (0);
}

/*
 * S3FileSystemTerminate --
 *     Discard any resources on termination of the file system.
 */
static int
S3FileSystemTerminate(WT_FILE_SYSTEM *fileSystem, WT_SESSION *session)
{
    S3_FILE_SYSTEM *fs = (S3_FILE_SYSTEM *)fileSystem;

    UNUSED(session); /* unused */

    delete (fs->connection);
    free(fs);

    return (0);
}

/*
 * S3ObjectList --
 *     Return a list of object names for the given location.
 */
static int
S3ObjectList(WT_FILE_SYSTEM *fileSystem, WT_SESSION *session, const char *bucket,
  const char *prefix, char ***objectList, uint32_t *count)
{
    S3_FILE_SYSTEM *fs = (S3_FILE_SYSTEM *)fileSystem;
    std::vector<std::string> objects;
    int ret;
    if (ret = fs->connection->ListObjects(bucket, prefix, objects) != 0)
        return (ret);
    *count = objects.size();

    S3ObjectListAdd(fs->storage, objectList, objects, *count);

    return (ret);
}

/*
 * S3ObjectListSingle --
 *     Return a single object name for the given location.
 */
static int
S3ObjectListSingle(WT_FILE_SYSTEM *fileSystem, WT_SESSION *session, const char *bucket,
  const char *prefix, char ***objectList, uint32_t *count)
{
    S3_FILE_SYSTEM *fs = (S3_FILE_SYSTEM *)fileSystem;
    std::vector<std::string> objects;
    int ret;
    if (ret = fs->connection->ListObjects(bucket, prefix, objects, 1, true) != 0)
        return (ret);
    *count = objects.size();

    S3ObjectListAdd(fs->storage, objectList, objects, *count);

    return (ret);
}

/*
 * S3ObjectListFree --
 *     Free memory allocated by S3ObjectList.
 */
static int
S3ObjectListFree(WT_FILE_SYSTEM *fileSystem, WT_SESSION *session, char **objectList, uint32_t count)
{
    (void)fileSystem;
    (void)session;

    if (objectList != NULL) {
        while (count > 0)
            free(objectList[--count]);
        free(objectList);
    }

    return (0);
}

/*
 * S3ObjectListAdd --
 *     Add objects retrieved from S3 bucket into the object list, and allocate the memory needed.
 */
static int
S3ObjectListAdd(
  S3_STORAGE *s3, char ***objectList, const std::vector<std::string> &objects, const uint32_t count)
{
    char **entries = (char **)malloc(sizeof(char *) * count);
    for (int i = 0; i < count; i++) {
        entries[i] = strdup(objects[i].c_str());
    }
    *objectList = entries;

    return (0);
}

/*
 * S3AddReference --
 *     Add a reference to the storage source so we can reference count to know when to really
 *     terminate.
 */
static int
S3AddReference(WT_STORAGE_SOURCE *storageSource)
{
    UNUSED(storageSource);
    return (0);
}

/*
 * S3Terminate --
 *     Discard any resources on termination.
 */
static int
S3Terminate(WT_STORAGE_SOURCE *storage, WT_SESSION *session)
{
    S3_STORAGE *s3;
    s3 = (S3_STORAGE *)storage;

    Aws::ShutdownAPI(options);

    free(s3);
    return (0);
}

/*
 * S3Flush --
 *     Flush file to S3 Store using AWS SDK C++ PutObject.
 */
static int
S3Flush(WT_STORAGE_SOURCE *storageSource, WT_SESSION *session, WT_FILE_SYSTEM *fileSystem,
  const char *source, const char *object, const char *config)
{
    S3_FILE_SYSTEM *fs = (S3_FILE_SYSTEM *)fileSystem;
    return (fs->connection->PutObject(fs->bucketName, object, source));
}

/*
 * S3FlushFinish --
 *     Flush local file to cache.
 */
static int
S3FlushFinish(WT_STORAGE_SOURCE *storage, WT_SESSION *session, WT_FILE_SYSTEM *fileSystem,
  const char *source, const char *object, const char *config)
{
    /* Constructing the pathname for source and cache from file system and local.  */
    std::string srcPath = S3Path(((S3_FILE_SYSTEM *)fileSystem)->homeDir, source);
    std::string destPath = S3Path(((S3_FILE_SYSTEM *)fileSystem)->cacheDir, source);

    /* Linking file with the local file. */
    int ret = link(srcPath.c_str(), destPath.c_str());

    /* Linking file with the local file. */
    if (ret == 0)
        ret = chmod(destPath.c_str(), 0444);
    return ret;
}

/*
 * wiredtiger_extension_init --
 *     A S3 storage source library.
 */
int
wiredtiger_extension_init(WT_CONNECTION *connection, WT_CONFIG_ARG *config)
{
    S3_STORAGE *s3;
    S3_FILE_SYSTEM *fs;
    WT_CONFIG_ITEM v;
    if ((s3 = (S3_STORAGE *)calloc(1, sizeof(S3_STORAGE))) == NULL)
        return (errno);

    s3->wtApi = connection->get_extension_api(connection);

    int ret = s3->wtApi->config_get(s3->wtApi, NULL, config, "verbose", &v);

    // If a verbose level is not found, it will set the level to -3 (Error).
    if (ret == 0 && v.val >= -3 && v.val <= 1)
        s3->verbose = v.val;
    else if (ret == WT_NOTFOUND)
        s3->verbose = -3;
    else {
        free(s3);
        return (ret != 0 ? ret : EINVAL);
    }

    Aws::InitAPI(options);

    /*
     * Allocate a S3 storage structure, with a WT_STORAGE structure as the first field, allowing us
     * to treat references to either type of structure as a reference to the other type.
     */
    s3->storageSource.ss_customize_file_system = S3CustomizeFileSystem;
    s3->storageSource.ss_add_reference = S3AddReference;
    s3->storageSource.terminate = S3Terminate;
    s3->storageSource.ss_flush = S3Flush;
    s3->storageSource.ss_flush_finish = S3FlushFinish;

    /* Load the storage */
    if ((ret = connection->add_storage_source(connection, "s3_store", &s3->storageSource, NULL)) !=
      0)
        free(s3);

    return (ret);
}
