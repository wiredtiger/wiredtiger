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

#include <aws/core/Aws.h>
#include "aws_bucket_conn.h"

#define UNUSED(x) (void)(x)

/* S3 storage source structure. */
typedef struct {
    WT_STORAGE_SOURCE storage_source; /* Must come first */
    WT_EXTENSION_API *wt_api;         /* Extension API */
} S3_STORAGE;

typedef struct {
    /* Must come first - this is the interface for the file system we are implementing. */
    WT_FILE_SYSTEM file_system;
    S3_STORAGE *s3_storage;
    aws_bucket_conn *conn;
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

static int s3_directory_list(
  WT_FILE_SYSTEM *, WT_SESSION *, const char *, const char *, char ***, uint32_t *);
static int s3_directory_list_add(S3_STORAGE *, char ***, const std::vector<std::string> *, const uint32_t);
static int s3_directory_list_single(
  WT_FILE_SYSTEM *, WT_SESSION *, const char *, const char *, char ***, uint32_t *);
static int s3_directory_list_free(WT_FILE_SYSTEM *, WT_SESSION *, char **, uint32_t);

/*
 * s3_customize_file_system --
 *     Return a customized file system to access the s3 storage source objects.
 */
static int
s3_customize_file_system(WT_STORAGE_SOURCE *storage_source, WT_SESSION *session,
  const char *bucket_name, const char *auth_token, const char *config,
  WT_FILE_SYSTEM **file_systemp)
{
    S3_FILE_SYSTEM *fs;
    int ret;

    /* Mark parameters as unused for now, until implemented. */
    UNUSED(session);
    UNUSED(bucket_name);
    UNUSED(auth_token);
    UNUSED(config);

    Aws::S3Crt::ClientConfiguration aws_config;
    aws_config.region = region;
    aws_config.throughputTargetGbps = throughput_target_gbps;
    aws_config.partSize = part_size;

    if ((fs = (S3_FILE_SYSTEM *)calloc(1, sizeof(S3_FILE_SYSTEM))) == NULL)
        return (errno);

    fs->s3_storage = (S3_STORAGE *)storage_source;

    /* New can fail; will deal with this later. */
    fs->conn = new aws_bucket_conn(aws_config);
    fs->file_system.fs_directory_list = s3_directory_list;
    fs->file_system.fs_directory_list_single = s3_directory_list_single;
    fs->file_system.fs_directory_list_free = s3_directory_list_free;
    fs->file_system.terminate = s3_fs_terminate;

    /* TODO: Move these into tests. Just testing here temporarily to show all functions work. */
    {
        std::vector<std::string> buckets;
        if (fs->conn->list_buckets(buckets)) {
            std::cout << "All buckets under my account:" << std::endl;
            for (const std::string &bucket : buckets)
                std::cout << "  * " << bucket << std::endl;
            std::cout << std::endl;
        }

        /* Have at least one bucket to use. */
        if (!buckets.empty()) {
            const std::string first_bucket = buckets.at(0);

            /* Put object. */
            fs->conn->put_object(first_bucket, "WiredTiger.turtle", "WiredTiger.turtle");

            /* Testing directory list. */
            WT_SESSION *session = NULL;
            const char *directory = first_bucket.c_str();
            const char *prefix = "WiredTiger";
            char ***dirlist = NULL;
            uint32_t countp;

            fs->file_system.fs_directory_list(
              &fs->file_system, session, directory, prefix, dirlist, &countp);
            std::cout << "Number of objects retrieved: " << countp << std::endl;

            // fs->file_system.fs_directory_list_single(
            //   &fs->file_system, session, directory, prefix, dirlist, &countp);
            // std::cout << "Number of objects retrieved: " << countp << std::endl;

            /* Delete object. */
            fs->conn->delete_object(first_bucket, "WiredTiger.turtle");

        } else
            std::cout << "No buckets in AWS account." << std::endl;
    }

    *file_systemp = &fs->file_system;
    return (0);
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
    free(s3_fs);

    return (0);
}

/*
 * s3_directory_list --
 *     Return a list of object names for the given location.
 */
static int
s3_directory_list(WT_FILE_SYSTEM *file_system, WT_SESSION *session, const char *directory,
  const char *prefix, char ***dirlistp, uint32_t *countp)
{
    S3_FILE_SYSTEM *s3_fs;
    s3_fs = (S3_FILE_SYSTEM *)file_system;
    uint32_t count = 0;
    std::vector<std::string> objects =
      s3_fs->conn->list_objects(std::string(directory), std::string(prefix), count, 5, 5);
    std::cout << "Objects in bucket '" << directory << "':" << std::endl;
    if (!objects.empty()) {
        for (const auto &object : objects)
            std::cout << "  * " << object << std::endl;
    } else
        std::cout << "No objects in bucket." << std::endl;

    char **entries = NULL;
    /* TODO: Put objects into dirlistp. */
    s3_directory_list_add(s3_fs->s3_storage, &entries, &objects, count);

    dirlistp = (char***)malloc(sizeof(char **));
    *dirlistp = entries;

    *countp = count;

    std::cout << "printing here" << std::endl;
    for (int i = 0; i < *countp; i++) {
        std::cout << (*dirlistp)[i] << std::endl;
    }
    return (0);
}

/*
 * s3_directory_list_single --
 *     Return a single file name for the given location.
 */
static int
s3_directory_list_single(WT_FILE_SYSTEM *file_system, WT_SESSION *session, const char *directory,
  const char *prefix, char ***dirlistp, uint32_t *countp)
{
    S3_FILE_SYSTEM *s3_fs;
    s3_fs = (S3_FILE_SYSTEM *)file_system;
    std::vector<std::string> objects =
      s3_fs->conn->list_objects(std::string(directory), std::string(prefix), *countp, 1, 1);
    std::cout << "Object in bucket '" << directory << "':" << std::endl;
    if (!objects.empty()) {
        for (const auto &object : objects)
            std::cout << "  * " << object << std::endl;
    } else
        std::cout << "No objects in bucket." << std::endl;

    // std::cout << "s3_directory_list_add" << std::endl;
    // s3_directory_list_add(s3_fs->s3_storage, dirlistp, &objects, *countp);

    return (0);
}

/*
 * s3_directory_list_free --
 *     Free memory allocated by s3_directory_list.
 */
static int
s3_directory_list_free(
  WT_FILE_SYSTEM *file_system, WT_SESSION *session, char **dirlist, uint32_t count)
{
    (void)session;

    if (dirlist != NULL) {
        while (count > 0)
            free(dirlist[--count]);
        free(dirlist);
    }
    return (0);
}

/*
 * s3_directory_list_add --
 *     Add an entry to the directory list, growing as needed.
 */
static int
s3_directory_list_add(
  S3_STORAGE *s3, char ***entriesp, const std::vector<std::string> *objects, const uint32_t count)
{
    char **entries = (char **)malloc(sizeof(char *) * count);
    for (int i = 0; i < count; i++) {
        entries[i] = strdup((*objects).at(i).c_str());
    }

    *entriesp = entries;

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
