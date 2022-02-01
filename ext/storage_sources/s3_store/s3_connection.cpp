#include <aws/core/Aws.h>
#include <aws/s3-crt/model/DeleteObjectRequest.h>
#include <aws/s3-crt/model/ListObjectsV2Request.h>
#include <aws/s3-crt/model/PutObjectRequest.h>
#include "s3_connection.h"

#include <fstream>
#include <iostream>
#include <string>
#include <vector>

/*
 * ListBuckets --
 *     Builds a list of buckets from AWS account into a vector. Returns 0 if success, otherwise
 *     1.
 */
int
S3Connection::ListBuckets(std::vector<std::string> &buckets) const
{
    auto outcome = m_S3CrtClient.ListBuckets();
    if (outcome.IsSuccess()) {
        for (const auto &bucket : outcome.GetResult().GetBuckets())
            buckets.push_back(bucket.GetName());
        return (0);
    } else {
        std::cerr << "Error in ListBuckets: " << outcome.GetError().GetMessage() << std::endl
                  << std::endl;
        return (1);
    }
}

/*
 * ListObjects --
 *     Builds a list of object names from an S3 bucket into a vector. Returns 0 if success,
 * otherwise 1.
 */
int
S3Connection::ListObjects(const std::string &bucketName, const std::string &prefix,
  std::vector<std::string> &objects, uint32_t &countp, uint32_t nPerIter, uint32_t maxObjects) const
{
    objects.clear();
    Aws::S3Crt::Model::ListObjectsV2Request request;

    request.SetBucket(bucketName);
    request.SetPrefix(prefix);
    request.SetMaxKeys(nPerIter);

    countp = 0;
    if (maxObjects != 0 && (maxObjects - countp) < nPerIter)
        request.SetMaxKeys(maxObjects - countp);

    Aws::S3Crt::Model::ListObjectsV2Outcome outcomes = m_S3CrtClient.ListObjectsV2(request);

    if (outcomes.IsSuccess()) {
        auto result = outcomes.GetResult();
        std::string continuationToken = result.GetNextContinuationToken();
        for (const auto &object : result.GetContents())
            objects.push_back(object.GetKey());
        countp += result.GetContents().size();

        /* Continuation token will be an empty string if we have returned all possible objects. */
        while (continuationToken != "" && (maxObjects == 0 || (maxObjects - countp) > 0)) {
            if (maxObjects != 0 && (maxObjects - countp) < nPerIter)
                request.SetMaxKeys(maxObjects - countp);
            request.SetContinuationToken(continuationToken);

            outcomes = m_S3CrtClient.ListObjectsV2(request);
            if (outcomes.IsSuccess()) {
                result = outcomes.GetResult();
                continuationToken = result.GetNextContinuationToken();
                for (const auto &object : result.GetContents())
                    objects.push_back(object.GetKey());
                countp += result.GetContents().size();
            }
        }
        return (0);
    } else
        return (1);
}

/*
 * PutObject --
 *     Puts an object into an S3 bucket. Returns 0 if success, otherwise 1.
 */
int
S3Connection::PutObject(
  const std::string &bucketName, const std::string &objectKey, const std::string &fileName) const
{
    Aws::S3Crt::Model::PutObjectRequest request;
    request.SetBucket(bucketName);
    request.SetKey(objectKey);

    std::shared_ptr<Aws::IOStream> inputData = Aws::MakeShared<Aws::FStream>(
      "s3-source", fileName.c_str(), std::ios_base::in | std::ios_base::binary);

    request.SetBody(inputData);

    Aws::S3Crt::Model::PutObjectOutcome outcome = m_S3CrtClient.PutObject(request);

    if (outcome.IsSuccess()) {
        return (0);
    } else {
        std::cerr << "Error in PutObject: " << outcome.GetError().GetMessage() << std::endl;
        return (1);
    }
}

/*
 * DeleteObject --
 *     Deletes an object from S3 bucket. Returns 0 if success, otherwise 1.
 */
int
S3Connection::DeleteObject(const std::string &bucketName, const std::string &objectKey) const
{
    Aws::S3Crt::Model::DeleteObjectRequest request;
    request.SetBucket(bucketName);
    request.SetKey(objectKey);

    Aws::S3Crt::Model::DeleteObjectOutcome outcome = m_S3CrtClient.DeleteObject(request);

    if (outcome.IsSuccess()) {
        return (0);
    } else {
        std::cerr << "Error in DeleteObject: " << outcome.GetError().GetMessage() << std::endl;
        return (1);
    }
}

/*
 * S3Connection --
 *     Constructor for AWS S3 bucket connection.
 */
S3Connection::S3Connection(const Aws::S3Crt::ClientConfiguration &config) : m_S3CrtClient(config){};
