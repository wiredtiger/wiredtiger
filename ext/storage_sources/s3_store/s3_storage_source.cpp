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

#include "s3_connection.h"
#include "s3_log_system.h"
#include <aws/core/Aws.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/logging/AWSLogging.h>

#define UNUSED(x) (void)(x)
#define FS2S3(fs) (((S3_FILE_SYSTEM *)(fs))->storage)

/* S3 storage source structure. */
typedef struct {
    WT_STORAGE_SOURCE storageSource; /* Must come first */
    WT_EXTENSION_API *wtApi;         /* Extension API */
    int32_t verbose;
} S3_STORAGE;

typedef struct {
    /* Must come first - this is the interface for the file system we are implementing. */
    WT_FILE_SYSTEM fileSystem;
    S3Connection *connection;
    S3LogSystem *log;
    S3_STORAGE *storage;
    std::string bucketName;
    std::string cacheDir; /* Directory for cached objects */
    std::string homeDir;  /* Owned by the connection */
} S3_FILE_SYSTEM;

/* Configuration variables for connecting to S3CrtClient. */
const Aws::String region = Aws::Region::AP_SOUTHEAST_2;
const double throughputTargetGbps = 5;
const uint64_t partSize = 8 * 1024 * 1024; /* 8 MB. */

/* Setting SDK options. */
Aws::SDKOptions options;

static int S3GetDirectory(const std::string &, const std::string &, bool, std::string &);
static bool S3CacheExists(WT_FILE_SYSTEM *, const std::string &);
static std::string S3Path(const std::string &, const std::string &);
static int S3Exist(WT_FILE_SYSTEM *, WT_SESSION *, const char *, bool *);
static int S3CustomizeFileSystem(
  WT_STORAGE_SOURCE *, WT_SESSION *, const char *, const char *, const char *, WT_FILE_SYSTEM **);
static int S3AddReference(WT_STORAGE_SOURCE *);
static int S3FileSystemTerminate(WT_FILE_SYSTEM *, WT_SESSION *);

/*
 * S3Exist --
 *     Return if the file exists. First checks the cache, and then the S3 Bucket.
 */
static int
S3Exist(WT_FILE_SYSTEM *fileSystem, WT_SESSION *session, const char *name, bool *existp)
{
    S3_STORAGE *s3;
    int ret;
    *existp = false;
    s3 = FS2S3(fileSystem);
    S3_FILE_SYSTEM *fs = (S3_FILE_SYSTEM *)fileSystem;
    bool exists;

    /* First check to see if the file exists in the cache. */
    exists = S3CacheExists(fileSystem, name);

    /* It's not in the cache, try the s3 bucket. */
    if (!exists)
        ret = fs->connection->ObjectExists(fs->bucketName, name, exists);

    *existp = exists;
    return (ret);
}

/*
 * S3Path --
 *     Construct a pathname from the directory and the object name.
 */
static std::string
S3Path(const std::string &dir, const std::string &name)
{
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
 * S3CustomizeFileSystem --
 *     Return a customized file system to access the s3 storage source objects.
 */
static int
S3CustomizeFileSystem(WT_STORAGE_SOURCE *storageSource, WT_SESSION *session, const char *bucketName,
  const char *authToken, const char *config, WT_FILE_SYSTEM **fileSystem)
{
    S3_STORAGE *s3;
    S3_FILE_SYSTEM *fs;
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

    if ((fs = (S3_FILE_SYSTEM *)calloc(1, sizeof(S3_FILE_SYSTEM))) == NULL)
        return (errno);
    fs->storage = s3;

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
    fs->fileSystem.terminate = S3FileSystemTerminate;
    fs->fileSystem.fs_exist = S3Exist;

    /* TODO: Move these into tests. Just testing here temporarily to show all functions work. */
    {
        /* List S3 buckets. */
        std::vector<std::string> buckets;
        if (fs->connection->ListBuckets(buckets)) {
            std::cout << "All buckets under my account:" << std::endl;
            for (const std::string &bucket : buckets) {
                std::cout << "  * " << bucket << std::endl;
            }
            std::cout << std::endl;
        }

        /* Have at least one bucket to use. */
        if (!buckets.empty()) {
            const Aws::String firstBucket = buckets.at(0);

            /* List objects. */
            std::vector<std::string> bucketObjects;
            if (fs->connection->ListObjects(firstBucket, bucketObjects)) {
                std::cout << "Objects in bucket '" << firstBucket << "':" << std::endl;
                if (!bucketObjects.empty()) {
                    for (const auto &object : bucketObjects) {
                        std::cout << "  * " << object << std::endl;
                    }
                } else {
                    std::cout << "No objects in bucket." << std::endl;
                }
                std::cout << std::endl;
            }

            /* Put object. */
            fs->connection->PutObject(firstBucket, "WiredTiger.turtle", "WiredTiger.turtle");

            /* List objects again. */
            bucketObjects.clear();
            if (fs->connection->ListObjects(firstBucket, bucketObjects)) {
                std::cout << "Objects in bucket '" << firstBucket << "':" << std::endl;
                if (!bucketObjects.empty()) {
                    for (const auto &object : bucketObjects) {
                        std::cout << "  * " << object << std::endl;
                    }
                } else {
                    std::cout << "No objects in bucket." << std::endl;
                }
                std::cout << std::endl;
            }

            /* Delete object. */
            fs->connection->DeleteObject(firstBucket, "WiredTiger.turtle");

            /* List objects again. */
            bucketObjects.clear();
            if (fs->connection->ListObjects(firstBucket, bucketObjects)) {
                std::cout << "Objects in bucket '" << firstBucket << "':" << std::endl;
                if (!bucketObjects.empty()) {
                    for (const auto &object : bucketObjects) {
                        std::cout << "  * " << object << std::endl;
                    }
                } else {
                    std::cout << "No objects in bucket." << std::endl;
                }
                std::cout << std::endl;
            }
        } else {
            std::cout << "No buckets in AWS account." << std::endl;
        }
    }

    *fileSystem = &fs->fileSystem;
    return 0;
}

/*
 * S3FileSystemTerminate --
 *     Discard any resources on termination of the file system.
 */
static int
S3FileSystemTerminate(WT_FILE_SYSTEM *fileSystem, WT_SESSION *session)
{
    S3_FILE_SYSTEM *fs;

    UNUSED(session); /* unused */

    fs = (S3_FILE_SYSTEM *)fileSystem;
    delete (fs->connection);
    free(fs);

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

    /* Load the storage */
    if ((ret = connection->add_storage_source(connection, "s3_store", &s3->storageSource, NULL)) !=
      0)
        free(s3);

    return (ret);
}
