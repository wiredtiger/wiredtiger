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
#include <errno.h>

#include <aws/core/Aws.h>
#include "aws_bucket_conn.h"

#define UNUSED(x) (void)(x)
#define FS2S3(fs) (((S3_FILE_SYSTEM *)(fs))->s3_storage)
/* S3 storage source structure. */
typedef struct {
    WT_STORAGE_SOURCE storage_source; /* Must come first */
    WT_EXTENSION_API *wt_api;         /* Extension API */   
    uint64_t op_count;
} S3_STORAGE;

typedef struct {
    /* Must come first - this is the interface for the file system we are implementing. */
    WT_FILE_SYSTEM file_system;
    char *cache_dir;      /* Directory for cached objects */
    S3_STORAGE *s3_storage;
    aws_bucket_conn *conn;
    const char *home_dir; /* Owned by the connection */
    const char *bucket_name;
} S3_FILE_SYSTEM;

/* Configuration variables for connecting to S3CrtClient. */
const Aws::String region = Aws::Region::AP_SOUTHEAST_2;
const double throughput_target_gbps = 5;
const uint64_t part_size = 8 * 1024 * 1024; /* 8 MB. */

/* Setting SDK options. */
Aws::SDKOptions options;

static int s3_customize_file_system(
  WT_STORAGE_SOURCE *, WT_SESSION *, const char *, const char *, const char *, WT_FILE_SYSTEM **);
static int s3_add_reference(WT_STORAGE_SOURCE *);
static int s3_fs_terminate(WT_FILE_SYSTEM *, WT_SESSION *);
static int s3_get_directory(const char *home, const char *s, ssize_t len, bool create, char **copy);
static int s3_stat(WT_FILE_SYSTEM *file_system, WT_SESSION *session, const char *name, const char *caller, bool must_exist, struct stat *statp);
static int s3_cache_path(WT_FILE_SYSTEM *file_system, const char *name, char **pathp);
static int s3_path(WT_FILE_SYSTEM *file_system, const char *dir, const char *name, char **pathp);
static int s3_exist(WT_FILE_SYSTEM *file_system, WT_SESSION *session, const char *name, bool *existp);

/*
 * s3_exist --
 *     Return if the file exists.
 */
static int
s3_exist(WT_FILE_SYSTEM *file_system, WT_SESSION *session, const char *name, bool *existp)
{
    struct stat sb;
    S3_STORAGE *s3;
    int ret;

    s3 = FS2S3(file_system);
    s3->op_count++;
    *existp = false;

    if ((ret = s3_stat(file_system, session, name, "ss_exist", false, &sb)) == 0)
        *existp = true;
    else if (ret == ENOENT){
        // std::cout << "s3_exist(): ENOENT - ret:" << ret << std::endl;
        ret = 0;
    }
    // std::cout << "s3_exist() - ret:" << ret << std::endl;
    return (ret);
}

/*
 * s3_path --
 *     Construct a pathname from the file system and local name.
 */
static int
s3_path(WT_FILE_SYSTEM *file_system, const char *dir, const char *name, char **pathp)
{
    size_t len;
    int ret;
    char *p;

    ret = 0;

    /* Skip over "./" and variations (".//", ".///./././//") at the beginning of the name. */
    while (*name == '.') {
        if (name[1] != '/')
            break;
        name += 2;
        while (*name == '/')
            name++;
    }
    len = strlen(dir) + strlen(name) + 2;
    if ((p = (char *)malloc(len)) == NULL)
        /* Out of memory */
        return (ENOMEM); 
    if (snprintf(p, len, "%s/%s", dir, name) >= (int)len)
        /* Overflow sprintf */
        return (EINVAL); 
    *pathp = p;
    return (ret);
}

/*
 * s3_cache_path --
 *     Construct the cache pathname from the file system and s3 name.
 */
static int
s3_cache_path(WT_FILE_SYSTEM *file_system, const char *name, char **pathp)
{
    return (s3_path(file_system, ((S3_FILE_SYSTEM *)file_system)->cache_dir, name, pathp));
}

/*
 * s3_stat --
 *     Perform the stat system call for a name in the file system.
 */
static int
s3_stat(WT_FILE_SYSTEM *file_system, WT_SESSION *session, const char *name, const char *caller,
  bool must_exist, struct stat *statp)
{
    int ret;
    char *path;
    S3_FILE_SYSTEM *s3_fs = (S3_FILE_SYSTEM*)file_system;
    path = NULL;

    /*
     * We check to see if the file exists in the cache first, and if not the bucket directory.
     */
    if ((ret = s3_cache_path(file_system, name, &path)) != 0)
        goto err;
    
    ret = stat(path, statp);
    
    if (ret != 0 && errno == ENOENT) {
        /* It's not in the cache, try the s3 bucket. */
        ret = s3_fs->conn->object_exists(s3_fs->bucket_name, name);
    }   

err:
    free(path);
    return (ret);
}

/*
 * s3_get_directory --
 *     Return a copy of a directory name after verifying that it is a directory.
 */
static int
s3_get_directory(const char *home, const char *s, ssize_t len, bool create, char **copy)
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
 * s3_customize_file_system --
 *     Return a customized file system to access the s3 storage source objects.
 */
static int
s3_customize_file_system(WT_STORAGE_SOURCE *storage_source, WT_SESSION *session,
  const char *bucket_name, const char *auth_token, const char *config,
  WT_FILE_SYSTEM **file_systemp)
{
    S3_STORAGE *s3;
    S3_FILE_SYSTEM *fs;
    int ret;
    WT_CONFIG_ITEM cachedir;    
    WT_FILE_SYSTEM *wt_fs;
    const char *p;
    char buf[1024];

    s3 = (S3_STORAGE *)storage_source;

    /* Mark parameters as unused for now, until implemented. */
    UNUSED(bucket_name);
    UNUSED(auth_token);

    Aws::S3Crt::ClientConfiguration aws_config;
    aws_config.region = region;
    aws_config.throughputTargetGbps = throughput_target_gbps;
    aws_config.partSize = part_size;

    /* Parse configuration string. */
    if ((ret = s3->wt_api->config_get_string(
           s3->wt_api, session, config, "cache_directory", &cachedir)) != 0) {
        if (ret == WT_NOTFOUND) {
            std::cout << "Cache directory not specified. Will default to cache-${BUCKET_NAME}." << std::endl;
            ret = 0;
            cachedir.len = 0;
        } else {
            std::cout << "Error parsing the config string:" << std::endl;
        }
    }

    if ((ret = s3->wt_api->file_system_get(s3->wt_api, session, &wt_fs)) != 0) {
        std::cout << "Cannot get WT File system." << std::endl;
    }
    
    if ((fs = (S3_FILE_SYSTEM *)calloc(1, sizeof(S3_FILE_SYSTEM))) == NULL)
        return (errno);
   /*
     * The home directory owned by the connection will not change, and will be valid memory, for as
     * long as the connection is open. That is longer than this file system will be open, so we can
     * use the string without copying.
     */
    fs->home_dir = session->connection->get_home(session->connection);

    fs->bucket_name = strdup(bucket_name);

    /*
     * The default cache directory is named "cache-<name>", where name is the last component of the
     * bucket name's path. We'll create it if it doesn't exist.
     */
    if (cachedir.len == 0) {
        if ((p = strrchr(bucket_name, '/')) != NULL)
            p++;
        else
            p = bucket_name;
        if (snprintf(buf, sizeof(buf), "cache-%s", p) >= (int)sizeof(buf)) {
        }
        cachedir.str = buf;
        cachedir.len = strlen(buf);
    }

    if ((ret = s3_get_directory(
           fs->home_dir, cachedir.str, (ssize_t)cachedir.len, true, &fs->cache_dir)) != 0) {
        std::cout << "Error occurred while making the cache directory" << std::endl;
    }
 
    printf("Cache directory: %s\n", cachedir.str); 

    fs->s3_storage = s3;
    /* New can fail; will deal with this later. */
    fs->conn = new aws_bucket_conn(aws_config);
    fs->file_system.terminate = s3_fs_terminate;
    fs->file_system.fs_exist = s3_exist;
    
    /* TODO: Move these into tests. Just testing here temporarily to show all functions work. */
    {
        /* List S3 buckets. */
        std::vector<std::string> buckets;
        if (fs->conn->list_buckets(buckets)) {
            std::cout << "All buckets under my account:" << std::endl;
            for (const std::string &bucket : buckets) {
                std::cout << "  * " << bucket << std::endl;
            }
            std::cout << std::endl;
        }

        /* Have at least one bucket to use. */
        if (!buckets.empty()) {
            const Aws::String first_bucket = buckets.at(0);

            /* List objects. */
            std::vector<std::string> bucket_objects;
            if (fs->conn->list_objects(first_bucket, bucket_objects)) {
                std::cout << "Objects in bucket '" << first_bucket << "':" << std::endl;
                if (!bucket_objects.empty()) {
                    for (const auto &object : bucket_objects) {
                        std::cout << "  * " << object << std::endl;
                    }
                } else {
                    std::cout << "No objects in bucket." << std::endl;
                }
                std::cout << std::endl;
            }

            fs->conn->object_exists(first_bucket, "permanent_object.txt");
            fs->conn->object_exists(first_bucket, "fake_object.txt");

            /* Put object. */
            fs->conn->put_object(first_bucket, "WiredTiger.turtle", "WiredTiger.turtle");

            /* List objects again. */
            bucket_objects.clear();
            if (fs->conn->list_objects(first_bucket, bucket_objects)) {
                std::cout << "Objects in bucket '" << first_bucket << "':" << std::endl;
                if (!bucket_objects.empty()) {
                    for (const auto &object : bucket_objects) {
                        std::cout << "  * " << object << std::endl;
                    }
                } else {
                    std::cout << "No objects in bucket." << std::endl;
                }
                std::cout << std::endl;
            }

            /* Delete object. */
            fs->conn->delete_object(first_bucket, "WiredTiger.turtle");

            /* List objects again. */
            bucket_objects.clear();
            if (fs->conn->list_objects(first_bucket, bucket_objects)) {
                std::cout << "Objects in bucket '" << first_bucket << "':" << std::endl;
                if (!bucket_objects.empty()) {
                    for (const auto &object : bucket_objects) {
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

    *file_systemp = &fs->file_system;
    return 0;
}

/*
 * s3_fs_terminate --
 *     Discard any resources on termination of the file system.
 */
static int
s3_fs_terminate(WT_FILE_SYSTEM *file_system, WT_SESSION *session)
{
    S3_FILE_SYSTEM *s3_fs;

    UNUSED(session); /* unused */

    s3_fs = (S3_FILE_SYSTEM *)file_system;
    delete (s3_fs->conn);
    free((void *)s3_fs->bucket_name);
    free(s3_fs);

    return (0);
}

/*
 * s3_add_reference --
 *     Add a reference to the storage source so we can reference count to know when to really
 *     terminate.
 */
static int
s3_add_reference(WT_STORAGE_SOURCE *storage_source)
{
    UNUSED(storage_source);
    return (0);
}

/*
 * s3_terminate --
 *     Discard any resources on termination.
 */
static int
s3_terminate(WT_STORAGE_SOURCE *storage, WT_SESSION *session)
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

    s3->wt_api = connection->get_extension_api(connection);
    UNUSED(config);

    Aws::InitAPI(options);

    /*
     * Allocate a S3 storage structure, with a WT_STORAGE structure as the first field, allowing us
     * to treat references to either type of structure as a reference to the other type.
     */
    s3->storage_source.ss_customize_file_system = s3_customize_file_system;
    s3->storage_source.ss_add_reference = s3_add_reference;
    s3->storage_source.terminate = s3_terminate;

    /* Load the storage */
    if ((ret = connection->add_storage_source(connection, "s3_store", &s3->storage_source, NULL)) !=
      0)
        free(s3);

    return (ret);
}
