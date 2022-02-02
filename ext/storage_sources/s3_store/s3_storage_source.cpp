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

#include <aws/core/Aws.h>
#include "s3_connection.h"

#define UNUSED(x) (void)(x)
#define FS2S3(fs) (((S3_FILE_SYSTEM *)(fs))->s3Storage)

/* S3 storage source structure. */
typedef struct {
    WT_STORAGE_SOURCE storageSource; /* Must come first */
    WT_EXTENSION_API *wtApi;         /* Extension API */
} S3_STORAGE;

typedef struct {
    /* Must come first - this is the interface for the file system we are implementing. */
    WT_FILE_SYSTEM fileSystem;
    char *cacheDir; /* Directory for cached objects */
    S3_STORAGE *s3Storage;
    S3Connection *conn;
    const char *homeDir; /* Owned by the connection */
    const char *bucketName;
} S3_FILE_SYSTEM;

/* Configuration variables for connecting to S3CrtClient. */
const Aws::String region = Aws::Region::AP_SOUTHEAST_2;
const double throughputTargetGbps = 5;
const uint64_t partSize = 8 * 1024 * 1024; /* 8 MB. */

/* Setting SDK options. */
Aws::SDKOptions options;

static int S3GetDirectory(const char *, const char *, ssize_t, bool, char **);
static int S3CacheExists(WT_FILE_SYSTEM *, const char *, bool *);
static int S3CachePath(WT_FILE_SYSTEM *, const char *, char **);
static int S3Path(const char *, const char *, char **);
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
    s3 = FS2S3(fileSystem);
    S3_FILE_SYSTEM *s3Fs = (S3_FILE_SYSTEM *)fileSystem;
    *existp = false;

    if ((ret = S3CacheExists(fileSystem, name, existp)) != 0)
        return (ret);

    /* It's not in the cache, try the s3 bucket. */
    if (!*existp)
        ret = s3Fs->conn->ObjectExists(s3Fs->bucketName, name, *existp);
    /*
     * If an object with the given key does not exist the HEAD request will return a 404.
     * https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html Do not fail in this case.
     */
    if (ret == 404 || ret == 0)
        return (0);
    else
        return (ret);
}

/*
 * S3Path --
 *     Construct a pathname from the directory and the object name.
 */
static int
S3Path(const char *dir, const char *name, char **path)
{
    /* Skip over "./" and variations (".//", ".///./././//") at the beginning of the name. */
    while (*name == '.') {
        if (name[1] != '/')
            break;
        name += 2;
        while (*name == '/')
            name++;
    }
    int strSize = std::snprintf(nullptr, 0, "%s/%s", dir, name) + 1; // Extra space for '\0'
    if (strSize <= 0)
        return (1);

    auto size = static_cast<size_t>(strSize);
    char *buf = new char[size];
    std::snprintf(buf, size, "%s/%s", dir, name);
    *path = buf;
    return (0);
}

/*
 * S3CachePath --
 *     Construct the path to the file in the cache.
 */
static int
S3CachePath(WT_FILE_SYSTEM *fileSystem, const char *name, char **path)
{
    return (S3Path(((S3_FILE_SYSTEM *)fileSystem)->cacheDir, name, path));
}

/*
 * S3CacheExists --
 *     Checks whether the given file exists in the cache.
 */
static int
S3CacheExists(WT_FILE_SYSTEM *fileSystem, const char *name, bool *existp)
{
    int ret;
    char *path;
    if ((ret = S3CachePath(fileSystem, name, &path)) != 0)
        return (ret);

    std::ifstream f(path);
    if (f.good())
        *existp = true;
    delete path;
    return (0);
}

/*
 * S3GetDirectory --
 *     Return a copy of a directory name after verifying that it is a directory.
 */
static int
S3GetDirectory(const char *home, const char *s, ssize_t len, bool create, char **copy)
{
    struct stat sb;
    size_t buflen;
    int ret;
    char *dirname;
    *copy = NULL;

    if (len == -1)
        len = (ssize_t)strlen(s);

    /* For relative pathnames, the path is considered to be relative to the home directory. */
    if (*s == '/')
        dirname = strndup(s, (size_t)len + 1); /* Room for null */
    else {
        buflen = (size_t)len + strlen(home) + 2; /* Room for slash, null */
        if ((dirname = (char *)malloc(buflen)) != NULL)
            if (snprintf(dirname, buflen, "%s/%.*s", home, (int)len, s) >= (int)buflen)
                return (EINVAL);
    }
    if (dirname == NULL)
        return (ENOMEM);

    ret = stat(dirname, &sb);
    if (ret != 0 && errno == ENOENT && create) {
        (void)mkdir(dirname, 0777);
        ret = stat(dirname, &sb);
    }
    if (ret != 0)
        ret = errno;
    else if ((sb.st_mode & S_IFMT) != S_IFDIR)
        ret = EINVAL;
    if (ret != 0)
        free(dirname);
    else
        *copy = dirname;
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
    const char *p;
    char buf[1024];
    s3 = (S3_STORAGE *)storageSource;

    /* Mark parameters as unused for now, until implemented. */
    UNUSED(authToken);

    Aws::S3Crt::ClientConfiguration awsConfig;
    awsConfig.region = region;
    awsConfig.throughputTargetGbps = throughputTargetGbps;
    awsConfig.partSize = partSize;

    /* Parse configuration string. */
    if ((ret = s3->wtApi->config_get_string(
           s3->wtApi, session, config, "cache_directory", &cacheDir)) != 0) {
        if (ret == WT_NOTFOUND) {
            ret = 0;
            cacheDir.len = 0;
        } else
            std::cout << "Error parsing the config string:" << std::endl;
    }

    if ((fs = (S3_FILE_SYSTEM *)calloc(1, sizeof(S3_FILE_SYSTEM))) == NULL)
        return (errno);
    fs->s3Storage = s3;
    /*
     * The home directory owned by the connection will not change, and will be valid memory, for as
     * long as the connection is open. That is longer than this file system will be open, so we can
     * use the string without copying.
     */
    fs->homeDir = session->connection->get_home(session->connection);

    /* Store a copy of the bucket name in the file system. */
    fs->bucketName = strdup(bucketName);

    /*
     * The default cache directory is named "cache-<name>", where name is the last component of the
     * bucket name's path. We'll create it if it doesn't exist.
     */
    if (cacheDir.len == 0) {
        if ((p = strrchr(bucketName, '/')) != NULL)
            p++;
        else
            p = bucketName;
        if (snprintf(buf, sizeof(buf), "cache-%s", p) >= (int)sizeof(buf)) {
        }
        cacheDir.str = buf;
        cacheDir.len = strlen(buf);
    }
    if ((ret = S3GetDirectory(
           fs->homeDir, cacheDir.str, (ssize_t)cacheDir.len, true, &fs->cacheDir)) != 0) {
        std::cout << "Error occurred while creating the cache directory." << std::endl;
    }

    /* New can fail; will deal with this later. */
    fs->conn = new S3Connection(awsConfig);
    fs->fileSystem.terminate = S3FileSystemTerminate;
    fs->fileSystem.fs_exist = S3Exist;

    /* TODO: Move these into tests. Just testing here temporarily to show all functions work. */
    {
        /* List S3 buckets. */
        std::vector<std::string> buckets;
        if (fs->conn->ListBuckets(buckets)) {
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
            if (fs->conn->ListObjects(firstBucket, bucketObjects)) {
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
            fs->conn->PutObject(firstBucket, "WiredTiger.turtle", "WiredTiger.turtle");

            /* List objects again. */
            bucketObjects.clear();
            if (fs->conn->ListObjects(firstBucket, bucketObjects)) {
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
            fs->conn->DeleteObject(firstBucket, "WiredTiger.turtle");

            /* List objects again. */
            bucketObjects.clear();
            if (fs->conn->ListObjects(firstBucket, bucketObjects)) {
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
    delete (fs->conn);
    free((void *)fs->bucketName);
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
    int ret;

    if ((s3 = (S3_STORAGE *)calloc(1, sizeof(S3_STORAGE))) == NULL)
        return (errno);

    s3->wtApi = connection->get_extension_api(connection);
    UNUSED(config);

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
