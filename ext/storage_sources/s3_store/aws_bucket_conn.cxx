#include <aws/core/Aws.h>
#include <aws/s3-crt/model/DeleteObjectRequest.h>
#include <aws/s3-crt/model/ListObjectsV2Request.h>
#include <aws/s3-crt/model/PutObjectRequest.h>
#include "aws_bucket_conn.h"

#include <fstream>
#include <iostream>
#include <string>
#include <vector>

/*
 * list_buckets --
 *     Builds a list of buckets from AWS account into a vector. Returns true if success, otherwise
 *     false.
 */
bool
aws_bucket_conn::list_buckets(std::vector<std::string> &buckets) const
{
    auto outcome = m_s3_crt_client.ListBuckets();
    if (outcome.IsSuccess()) {
        for (const auto &bucket : outcome.GetResult().GetBuckets())
            buckets.push_back(bucket.GetName());
        return (true);
    } else {
        std::cerr << "Error in list_buckets: " << outcome.GetError().GetMessage() << std::endl;
        return (false);
    }
}

/*
 * list_objects --
 *     Return a list of object names from a S3 bucket into a vector.
 */
std::vector<std::string>
aws_bucket_conn::list_objects(const std::string &bucket_name, const std::string &prefix,
  uint32_t &countp, uint32_t max_objects, uint32_t n_per_iter) const
{
    std::vector<std::string> objects;
    Aws::S3Crt::Model::ListObjectsV2Request request;

    request.SetBucket(bucket_name);
    request.SetPrefix(prefix);
    request.SetMaxKeys(n_per_iter);

    countp = 0;
    Aws::S3Crt::Model::ListObjectsV2Outcome outcomes = m_s3_crt_client.ListObjectsV2(request);

    if (outcomes.IsSuccess()) {
        auto result = outcomes.GetResult();
        std::string continuation_token = result.GetNextContinuationToken();
        for (const auto &object : result.GetContents())
            objects.push_back(object.GetKey());
        countp += result.GetContents().size();

        /* Continuation token will be an empty string if we have returned all possible objects. */
        while (continuation_token != "" && (max_objects == 0 || (max_objects - countp) > 0)) {
            if (max_objects != 0 && (max_objects - countp) < n_per_iter)
                request.SetMaxKeys(max_objects - countp);
            request.SetContinuationToken(continuation_token);

            outcomes = m_s3_crt_client.ListObjectsV2(request);
            if (outcomes.IsSuccess()) {
                result = outcomes.GetResult();
                continuation_token = result.GetNextContinuationToken();
                for (const auto &object : result.GetContents())
                    objects.push_back(object.GetKey());
                countp += result.GetContents().size();
            }
        }
        return (objects);
    } else
        throw std::runtime_error("Error in list_buckets: " + outcomes.GetError().GetMessage());
}

/*
 * put_object --
 *     Puts an object into an S3 bucket. Returns true if success, otherwise false.
 */
bool
aws_bucket_conn::put_object(
  const std::string &bucket_name, const std::string &object_key, const std::string &file_name) const
{
    Aws::S3Crt::Model::PutObjectRequest request;
    request.SetBucket(bucket_name);
    request.SetKey(object_key);

    std::shared_ptr<Aws::IOStream> input_data = Aws::MakeShared<Aws::FStream>(
      "s3-source", file_name.c_str(), std::ios_base::in | std::ios_base::binary);
    request.SetBody(input_data);

    /* This check is required to fail on missing file. */
    if (!input_data->good()) {
        std::cout << "Failed to open file: \"" << file_name << "\"." << std::endl;
        return (false);
    }

    Aws::S3Crt::Model::PutObjectOutcome outcome = m_s3_crt_client.PutObject(request);
    if (outcome.IsSuccess())
        return (true);
    else {
        std::cerr << "Error in put_object: " << outcome.GetError().GetMessage() << std::endl;
        return (false);
    }
}

/*
 * delete_object --
 *     Deletes an object from S3 bucket. Returns true if success, otherwise false.
 */
bool
aws_bucket_conn::delete_object(const std::string &bucket_name, const std::string &object_key) const
{
    Aws::S3Crt::Model::DeleteObjectRequest request;
    request.SetBucket(bucket_name);
    request.SetKey(object_key);

    Aws::S3Crt::Model::DeleteObjectOutcome outcome = m_s3_crt_client.DeleteObject(request);

    if (outcome.IsSuccess())
        return (true);
    else {
        std::cerr << "Error in delete_object: " << outcome.GetError().GetMessage() << std::endl;
        return (false);
    }
}

/*
 * aws_bucket_conn --
 *     Constructor for AWS bucket connection.
 */
aws_bucket_conn::aws_bucket_conn(const Aws::S3Crt::ClientConfiguration &config)
    : m_s3_crt_client(config){};
