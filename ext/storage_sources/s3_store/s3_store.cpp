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
Aws::String region = Aws::Region::AP_SOUTHEAST_2;
const double throughput_target_gbps = 5;
const uint64_t part_size = 8 * 1024 * 1024; // 8 MB.

static int s3_customize_file_system(
  WT_STORAGE_SOURCE *, WT_SESSION *, const char *, const char *, const char *, WT_FILE_SYSTEM **);
static int s3_add_reference(WT_STORAGE_SOURCE *);

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
    const Aws::S3Crt::S3CrtClient &s3CrtClient = aws_config;

    awsBucketConn conn(aws_config);
    conn.s3_list_buckets();
    conn.list_bucket_objects("rubysfirstbucket");
    conn.put_object("../../luna.json", "rubysfirstbucket", "luna.json");
    conn.delete_object("rubysfirstbucket", "luna.json");
    awsBucketConn conn2(aws_config);
    conn2.list_bucket_objects("rubyssecondbucket");



    return 0;
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

static int s3_terminate(WT_STORAGE_SOURCE *storage, WT_SESSION *session)
{
    /* NEED A WAY to pass the options to the ShutdownAPI call */
    S3_STORAGE *s3;
    s3 = (S3_STORAGE *)storage;

    Aws::SDKOptions options;
    Aws::ShutdownAPI(options);

    delete (s3);
    return (0); 
}

/*
 * wiredtiger_extension_init --
 *     A S3 storage source library.
 */
int
wiredtiger_extension_init(WT_CONNECTION *connection, WT_CONFIG_ARG *config)
{
    S3_STORAGE *s3 = new S3_STORAGE;
    int ret;

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

    /* Load the storage */
    if ((ret = connection->add_storage_source(connection, "s3_store", &s3->storage_source, NULL)) !=
      0) {
        free(s3);
    }
    return (ret);
}