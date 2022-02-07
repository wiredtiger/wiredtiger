#include <aws/core/Aws.h>
#include <aws/s3-crt/model/DeleteObjectRequest.h>
#include <aws/s3-crt/model/ListObjectsRequest.h>
#include <aws/s3-crt/model/PutObjectRequest.h>
#include <aws/s3-crt/model/HeadObjectRequest.h>

#include "s3_connection.h"

#include <fstream>
#include <iostream>
#include <string>
#include <vector>

/*
 * ListObjects --
 *     Builds a list of object names from a S3 bucket into a vector. Returns true if success,
 *     otherwise false.
 */
int
S3Connection::ListObjects(std::vector<std::string> &objects) const
{
    Aws::S3Crt::Model::ListObjectsRequest request;
    request.WithBucket(m_bucketName);
    Aws::S3Crt::Model::ListObjectsOutcome outcomes = m_S3CrtClient.ListObjects(request);

    if (outcomes.IsSuccess()) {
        for (const auto &object : outcomes.GetResult().GetContents())
            objects.push_back(object.GetKey());
        return (0);
    }

    std::cerr << "Error in ListObjects: " << outcomes.GetError().GetMessage() << std::endl;
    return (1);
}

/*
 * PutObject --
 *     Puts an object into an S3 bucket. Returns true if success, otherwise false.
 */
int
S3Connection::PutObject(const std::string &objectKey, const std::string &fileName) const
{
    Aws::S3Crt::Model::PutObjectRequest request;
    request.SetBucket(m_bucketName);
    request.SetKey(m_objectPrefix + objectKey);

    std::shared_ptr<Aws::IOStream> inputData = Aws::MakeShared<Aws::FStream>(
      "s3-source", fileName.c_str(), std::ios_base::in | std::ios_base::binary);

    request.SetBody(inputData);

    Aws::S3Crt::Model::PutObjectOutcome outcome = m_S3CrtClient.PutObject(request);

    if (outcome.IsSuccess()) {
        return (0);
    }
   
    std::cerr << "Error in PutObject: " << outcome.GetError().GetMessage() << std::endl;
    return (1);
}

/*
 * DeleteObject --
 *     Deletes an object from S3 bucket. Returns true if success, otherwise false.
 */
int
S3Connection::DeleteObject(const std::string &objectKey) const
{
    Aws::S3Crt::Model::DeleteObjectRequest request;
    request.SetBucket(m_bucketName);
    request.SetKey(m_objectPrefix + objectKey);

    Aws::S3Crt::Model::DeleteObjectOutcome outcome = m_S3CrtClient.DeleteObject(request);

    if (outcome.IsSuccess())
        return (0);

    std::cerr << "Error in DeleteObject: " << outcome.GetError().GetMessage() << std::endl;
    return (1);
}

/*
 * ObjectExists --
 *     Checks whether an object with the given key exists in the S3 bucket.
 */
int
S3Connection::ObjectExists(const std::string &objectKey, bool &exists) const
{
    exists = false;

    Aws::S3Crt::Model::HeadObjectRequest request;
    request.SetBucket(m_bucketName);
    request.SetKey(m_objectPrefix + objectKey);
    Aws::S3Crt::Model::HeadObjectOutcome outcome = m_S3CrtClient.HeadObject(request);

    /*
     * If an object with the given key does not exist the HEAD request will return a 404.
     * https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html Do not fail in this case.
     */
    if (outcome.IsSuccess()) {
        exists = true;
        return (0);
    } else if (outcome.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND)
        return (0);

    /*
     * Fix later, return a proper error code. Not sure if we always have
     * outcome.GetError().GetResponseCode()
     */
    return (ENOTSUP);
}

/*
 * S3Connection --
 *     Constructor for AWS S3 bucket connection.
 */
S3Connection::S3Connection(const Aws::S3Crt::ClientConfiguration &config,
  const std::string &bucketName, const std::string &objPrefix)
    : m_S3CrtClient(config), m_bucketName(bucketName), m_objectPrefix(objPrefix) {}
