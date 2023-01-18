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

#include "gcp_connection.h"

#include <fstream>

<<<<<<< HEAD

GCPConnection(const std::string &bucketName,
      const std::string &objPrefix = "")
    : 
{
    return(0);
}

/*
 * Builds a list of object names, with prefix matching, from an S3 bucket into a vector. The
 * batchSize parameter specifies the maximum number of objects returned in each AWS response, up to
 * 1000. Return an errno value given an HTTP response code if the aws request does not succeed.
 */
int
GCPConnection::ListObjects(const std::string &prefix, std::vector<std::string> &objects,
  uint32_t batchSize, bool listSingle) const
{
    
    return (0);
}

// Puts an object into an S3 bucket. Return an errno value given an HTTP response code if
// the aws request does not succeed.
int
GCPConnection::PutObject(const std::string &objectKey, const std::string &fileName) const
{
    return (0);
}

// Deletes an object from S3 bucket. Return an errno value given an HTTP response code if
// the aws request does not succeed.
int
GCPConnection::DeleteObject(const std::string &objectKey) const
{
    return (0);
}

// Retrieves an object from S3. The object is downloaded to disk at the specified location.
int
GCPConnection::GetObject(const std::string &objectKey, const std::string &path) const
{
    return (0);
}

// Checks whether an object with the given key exists in the S3 bucket and also retrieves
// size of the object.
int
GCPConnection::ObjectExists(const std::string &objectKey, bool &exists, size_t &objectSize) const
{
    return (0);
}

// Checks whether the bucket configured for the class is accessible to us or not.
int
GCPConnection::BucketExists(bool &exists) const
{
    return (0);
=======
gcp_connection::gcp_connection(const std::string &bucket_name)
    : _gcp_client(google::cloud::storage::Client()), _bucket_name(bucket_name)
{
}

// Builds a list of object names from the bucket.
int
gcp_connection::list_objects(std::vector<std::string> &objects) const
{
    return 0;
}

// Puts an object into a google cloud bucket.
int
gcp_connection::put_object(const std::string &object_key, const std::string &file_name) const
{
    return 0;
}

// Deletes an object from google cloud bucket.
int
gcp_connection::delete_object(const std::string &object_key) const
{
    return 0;
}

// Retrieves an object from the google cloud bucket.
int
gcp_connection::get_object(const std::string &object_key, const std::string &path) const
{
    return 0;
}

// Checks whether an object with the given key exists in the google cloud bucket and also retrieves
// size of the object.
int
gcp_connection::object_exists(
  const std::string &object_key, bool &exists, size_t &object_size) const
{
    return 0;
}

// Checks whether the google cloud bucket is accessible to us or not.
int
gcp_connection::bucket_exists(bool &exists) const
{
    return 0;
>>>>>>> develop
}
