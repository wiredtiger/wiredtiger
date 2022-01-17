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
#include "AwsBucketConn.h"

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

    /* This is WiredTiger's file system, it is used in implementing the s3 file system. */
    WT_FILE_SYSTEM *wt_fs;

} S3_FILE_SYSTEM;

typedef struct s3_file_handle {
    WT_FILE_HANDLE iface; /* Must come first */
    S3_STORAGE *s3;       /* Enclosing storage source */
    WT_FILE_HANDLE *fh;   /* File handle */

} S3_FILE_HANDLE;

/* Configuration variables for connecting to S3CrtClient. */
Aws::String region = Aws::Region::US_EAST_1;
const double throughput_target_gbps = 5;
const uint64_t part_size = 8 * 1024 * 1024; // 8 MB.

static int s3_customize_file_system(
  WT_STORAGE_SOURCE *, WT_SESSION *, const char *, const char *, const char *, WT_FILE_SYSTEM **);
static int s3_add_reference(WT_STORAGE_SOURCE *);

// bool s3_list_buckets(const Aws::S3Crt::S3CrtClient &s3CrtClient);

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
    /* Mark parameters as unused for now, until implemented. */
    UNUSED(storage_source);
    UNUSED(session);
    UNUSED(bucket_name);
    UNUSED(auth_token);
    UNUSED(config);
    UNUSED(file_systemp);

    Aws::S3Crt::ClientConfiguration aws_config;
    aws_config.region = region;
    aws_config.throughputTargetGbps = throughput_target_gbps;
    aws_config.partSize = part_size;

    awsBucketConn conn(aws_config);
    conn.s3_list_buckets();
    // conn.set_test(aws_config);
    // Aws::S3Crt::S3CrtClient m_s3_crt_client;

    // Aws::S3Crt::S3CrtClient s3_crt_client(aws_config);

    // s3_list_buckets(aws_config);

    // conn.initBucketConn();
    // std::cout << "List buckets" << std::endl;
    // conn.s3_list_buckets();

    return 0;
}

/* s3_list_buckets --
 *      List all Amazon Simple Storage Service (Amazon S3) buckets under the account.
 */
bool
s3_list_buckets(const Aws::S3Crt::S3CrtClient &s3CrtClient)
{
    Aws::S3Crt::Model::ListBucketsOutcome outcome = s3CrtClient.ListBuckets();

    if (outcome.IsSuccess()) {
        std::cout << "All buckets under my account:" << std::endl;
        for (auto const &bucket : outcome.GetResult().GetBuckets()) {
            std::cout << "  * " << bucket.GetName() << std::endl;
        }
        std::cout << std::endl;
        return true;
    } else {
        std::cout << "ListBuckets error:\n" << outcome.GetError() << std::endl << std::endl;
        return false;
    }
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
    std::cout << "s3_add_reference()";
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

    if ((s3 = (S3_STORAGE *)calloc(1, sizeof(S3_STORAGE))) == NULL) {
        return (errno);
    }
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

    /* Load the storage */
    if ((ret = connection->add_storage_source(connection, "s3_store", &s3->storage_source, NULL)) !=
      0) {
        free(s3);
    }
    return (ret);
}