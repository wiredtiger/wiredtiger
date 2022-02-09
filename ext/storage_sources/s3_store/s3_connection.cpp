#include <aws/core/Aws.h>
#include <aws/s3-crt/model/DeleteObjectRequest.h>
#include <aws/s3-crt/model/ListObjectsV2Request.h>
#include <aws/s3-crt/model/PutObjectRequest.h>
#include <aws/s3-crt/model/HeadObjectRequest.h>

#include "s3_connection.h"

#include <fstream>
#include <iostream>
#include <string>
#include <vector>

/*
 * S3Connection --
 *     Constructor for AWS S3 bucket connection.
 */
S3Connection::S3Connection(const Aws::S3Crt::ClientConfiguration &config,
  const std::string &bucketName, const std::string &objPrefix)
    : _s3CrtClient(config), _bucketName(bucketName), _objectPrefix(objPrefix)
{
}

/*
 * ListObjects --
 *     Builds a list of object names, with prefix matching, from an S3 bucket into a vector. The
 *     batchSize parameter specifies the maximum number of objects returned in each AWS response, up
 *     to 1000. Returns 0 if success, otherwise 1.
 */
int
S3Connection::ListObjects(const std::string &prefix, std::vector<std::string> &objects,
  uint32_t batchSize, bool listSingle) const
{
    Aws::S3Crt::Model::ListObjectsV2Request request;
    request.SetBucket(_bucketName);
    request.SetPrefix(_objectPrefix + prefix);
    listSingle ? request.SetMaxKeys(1) : request.SetMaxKeys(batchSize);

    Aws::S3Crt::Model::ListObjectsV2Outcome outcomes = _s3CrtClient.ListObjectsV2(request);
    if (!outcomes.IsSuccess())
        return (1);
    auto result = outcomes.GetResult();
    for (const auto &object : result.GetContents())
        objects.push_back(object.GetKey());

    if (listSingle)
        return (0);

    /* Continuation token will be an empty string if we have returned all possible objects. */
    std::string continuationToken = result.GetNextContinuationToken();
    while (continuationToken != "") {
        request.SetContinuationToken(continuationToken);
        outcomes = _s3CrtClient.ListObjectsV2(request);
        if (!outcomes.IsSuccess())
            return (1);
        result = outcomes.GetResult();
        for (const auto &object : result.GetContents())
            objects.push_back(object.GetKey());
        continuationToken = result.GetNextContinuationToken();
    }
    return (0);
}

/*
 * PutObject --
 *     Puts an object into an S3 bucket. Returns 0 if success, otherwise 1.
 */
int
S3Connection::PutObject(const std::string &objectKey, const std::string &fileName) const
{
    std::shared_ptr<Aws::IOStream> inputData = Aws::MakeShared<Aws::FStream>(
      "s3-source", fileName.c_str(), std::ios_base::in | std::ios_base::binary);

    Aws::S3Crt::Model::PutObjectRequest request;
    request.SetBucket(_bucketName);
    request.SetKey(_objectPrefix + objectKey);
    request.SetBody(inputData);

    Aws::S3Crt::Model::PutObjectOutcome outcome = _s3CrtClient.PutObject(request);
    if (outcome.IsSuccess()) {
        return (0);
    }

    std::cerr << "Error in PutObject: " << outcome.GetError().GetMessage() << std::endl;
    return (1);
}

/*
 * DeleteObject --
 *     Deletes an object from S3 bucket. Returns 0 if success, otherwise 1.
 */
int
S3Connection::DeleteObject(const std::string &objectKey) const
{
    Aws::S3Crt::Model::DeleteObjectRequest request;
    request.SetBucket(_bucketName);
    request.SetKey(_objectPrefix + objectKey);

    Aws::S3Crt::Model::DeleteObjectOutcome outcome = _s3CrtClient.DeleteObject(request);
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
    request.SetBucket(_bucketName);
    request.SetKey(_objectPrefix + objectKey);
    Aws::S3Crt::Model::HeadObjectOutcome outcome = _s3CrtClient.HeadObject(request);

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
    std::cerr << "Error in ObjectExists." << std::endl;
    return (1);
}
