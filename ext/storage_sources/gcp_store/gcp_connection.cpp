#include "s3_connection.h"

#include <aws/s3-crt/model/DeleteObjectRequest.h>
#include <aws/s3-crt/model/ListObjectsV2Request.h>
#include <aws/s3-crt/model/PutObjectRequest.h>
#include <aws/s3-crt/model/GetObjectRequest.h>
#include <aws/s3-crt/model/HeadObjectRequest.h>
#include <aws/s3-crt/model/HeadBucketRequest.h>

#include <fstream>
#include <iostream>

GCPConnection(const GCPCredentials &credentials,
      const GCPConfig &config, const std::string &bucketName,
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
}
