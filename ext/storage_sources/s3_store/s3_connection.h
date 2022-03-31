
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
#ifndef S3CONNECTION
#define S3CONNECTION

#include <aws/auth/credentials.h>
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/s3-crt/S3CrtClient.h>

#include <string>
#include <vector>

/*
 * Class to represent an active connection to the AWS S3 endpoint. Allows for interaction with S3
 * client.
 */
class S3Connection {
    public:
    S3Connection(const Aws::Auth::AWSCredentials &credentials,
      const Aws::S3Crt::ClientConfiguration &config, const std::string &bucketName,
      const std::string &objPrefix = "");
    S3Connection(const Aws::S3Crt::ClientConfiguration &config, const std::string &bucketName,
      const std::string &objPrefix = "");
    int ListObjects(const std::string &prefix, std::vector<std::string> &objects,
      uint32_t batchSize = 1000, bool listSingle = false) const;
    int PutObject(const std::string &objectKey, const std::string &fileName) const;
    int DeleteObject(const std::string &objectKey) const;
    int ObjectExists(const std::string &objectKey, bool &exists, size_t &objectSize) const;
    int GetObject(const std::string &objectKey, const std::string &path) const;

    ~S3Connection() = default;

    private:
    const Aws::S3Crt::S3CrtClient _s3CrtClient;
    const std::string _bucketName;
    const std::string _objectPrefix;

    int BucketExists(bool &exists) const;
};
#endif
