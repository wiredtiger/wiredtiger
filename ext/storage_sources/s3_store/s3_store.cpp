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

#include <iostream>
#include <aws/core/Aws.h>
#include <aws/s3-crt/S3CrtClient.h>
#include "aws_bucket_conn.h"

#define UNUSED(x) (void)(x)

/* s3 storage source structure. */
typedef struct {
    WT_STORAGE_SOURCE storage_source; /* Must come first */
    WT_EXTENSION_API *wt_api;         /* Extension API */

} S3_STORAGE;

typedef struct {
    /* Must come first - this is the interface for the file system we are implementing. */
    WT_FILE_SYSTEM file_system;
    S3_STORAGE *s3_storage;

    aws_bucket_conn *conn;

    /* This is WiredTiger's file system, it is used in implementing the s3 file system. */
    // WT_FILE_SYSTEM *wt_fs;

} S3_FILE_SYSTEM;

typedef struct s3_file_handle {
    WT_FILE_HANDLE iface; /* Must come first */
    S3_STORAGE *s3;       /* Enclosing storage source */
    WT_FILE_HANDLE *fh;   /* File handle */

} S3_FILE_HANDLE;

/* Configuration variables for connecting to S3CrtClient. */
Aws::String region = Aws::Region::AP_SOUTHEAST_2;
const double throughput_target_gbps = 5;
const uint64_t part_size = 8 * 1024 * 1024; // 8 MB.

static int s3_customize_file_system(
  WT_STORAGE_SOURCE *, WT_SESSION *, const char *, const char *, const char *, WT_FILE_SYSTEM **);
static int s3_add_reference(WT_STORAGE_SOURCE *);
static int s3_fs_terminate(WT_FILE_SYSTEM *, WT_SESSION *);


/*
 * s3_customize_file_system --
 *      TODO: Return a customized file system to access the s3 storage source objects.
 *      Currently lists buckets.
 * =================== NOT IMPLEMENTED ===============
 */
static int
s3_customize_file_system(WT_STORAGE_SOURCE *storage_source, WT_SESSION *session,
  const char *bucket_name, const char *auth_token, const char *config,
  WT_FILE_SYSTEM **file_systemp)
{
    S3_FILE_SYSTEM *fs;
    int ret;

    /* Mark parameters as unused for now, until implemented. */
    UNUSED(storage_source);
    UNUSED(session);
    // UNUSED(bucket_name);
    UNUSED(auth_token);
    UNUSED(config);
    UNUSED(file_systemp);

    Aws::S3Crt::ClientConfiguration aws_config;
    aws_config.region = region;
    aws_config.throughputTargetGbps = throughput_target_gbps;
    aws_config.partSize = part_size;

    /* TODO: Move these into tests. */

    if ((fs = (S3_FILE_SYSTEM*)calloc(1, sizeof(S3_FILE_SYSTEM))) == NULL)
        return (errno);

    fs->s3_storage = (S3_STORAGE*)storage_source;

    // new can fail; will deal with this later. 
    fs->conn = new aws_bucket_conn(aws_config);
    fs->file_system.terminate = s3_fs_terminate; 


    // All content will moved into testing; just testing here temporarily to show all functions work.
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
        if (buckets.size() >= 1) {
            const Aws::String first_bucket = buckets.at(0);

            /* List objects. */
            std::vector<Aws::S3Crt::Model::Object> bucket_objects;
            if (fs->conn->list_objects(first_bucket, bucket_objects)) {
                std::cout << "Objects in bucket '" << first_bucket << "':" << std::endl;
                if (bucket_objects.size() >= 1) {
                    for (Aws::S3Crt::Model::Object &object : bucket_objects) {
                        std::cout << "  * " << object.GetKey() << std::endl;
                    }
                } else {
                    std::cout << "No objects in bucket." << std::endl;
                    ;
                }
                std::cout << std::endl;
            }

            /* Put object. */
            fs->conn->put_object(first_bucket, "test.json", "../../../test.json");

            /* List objects again. */
            if (fs->conn->list_objects(first_bucket, bucket_objects)) {
                std::cout << "Objects in bucket '" << first_bucket << "':" << std::endl;
                if (bucket_objects.size() >= 1) {
                    for (Aws::S3Crt::Model::Object &object : bucket_objects) {
                        std::cout << "  * " << object.GetKey() << std::endl;
                    }
                } else {
                    std::cout << "No objects in bucket." << std::endl;
                    ;
                }
                std::cout << std::endl;
            }

            /* Delete object. */
            fs->conn->delete_object(first_bucket, "test.json");

            /* List objects again. */
            if (fs->conn->list_objects(first_bucket, bucket_objects)) {
                std::cout << "Objects in bucket '" << first_bucket << "':" << std::endl;
                if (bucket_objects.size() >= 1) {
                    for (Aws::S3Crt::Model::Object &object : bucket_objects) {
                        std::cout << "  * " << object.GetKey() << std::endl;
                    }
                } else {
                    std::cout << "No objects in bucket." << std::endl;
                    ;
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


static int
s3_fs_terminate(WT_FILE_SYSTEM *file_system, WT_SESSION *session)
{
    S3_FILE_SYSTEM *s3_fs;

    (void)session; /* unused */

    s3_fs = (S3_FILE_SYSTEM *)file_system;
    delete(s3_fs->conn);
    free(s3_fs);

    return (0);
}


/*
 * s3_add_reference --
 *     Add a reference to the storage source so we can reference count to know when to really
 *     terminate.
 * =================== NOT IMPLEMENTED ===============
 */
static int
s3_add_reference(WT_STORAGE_SOURCE *storage_source)
{
    UNUSED(storage_source);
    // std::cout << "s3_add_reference()";
    return (0);
}

static int
s3_terminate(WT_STORAGE_SOURCE *storage, WT_SESSION *session)
{
    /* NEED A WAY to pass the options to the ShutdownAPI call */
    S3_STORAGE *s3;
    s3 = (S3_STORAGE *)storage;

    Aws::SDKOptions options;
    Aws::ShutdownAPI(options);

    free (s3);
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

    if ((s3 = (S3_STORAGE*)calloc(1, sizeof(S3_STORAGE))) == NULL)
        return (errno);


    s3->wt_api = connection->get_extension_api(connection);
    UNUSED(config);

    /* Setting SDK options. */
    Aws::SDKOptions options;
    Aws::InitAPI(options);

    /*
     * Allocate a s3 storage structure, with a WT_STORAGE structure as the first field, allowing
     * us to treat references to either type of structure as a reference to the other type.
     */
    s3->storage_source.ss_customize_file_system = s3_customize_file_system;
    s3->storage_source.ss_add_reference = s3_add_reference;
    s3->storage_source.terminate = s3_terminate;
    // s3->storage_source.ss_flush = s3_flush;

    /* Load the storage */
    if ((ret = connection->add_storage_source(connection, "s3_store", &s3->storage_source, NULL)) !=
      0)
        free(s3);
        
    return (ret);
}