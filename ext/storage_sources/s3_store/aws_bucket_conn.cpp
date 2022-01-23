#include <aws/core/Aws.h>
#include <aws/s3-crt/S3CrtClient.h>
#include <aws/s3-crt/model/DeleteObjectRequest.h>
#include <aws/s3-crt/model/ListObjectsRequest.h>
#include <aws/s3-crt/model/Object.h>
#include <aws/s3-crt/model/PutObjectRequest.h>
#include "aws_bucket_conn.h"

#include <fstream>
#include <iostream>
#include <string>
#include <vector>

bool
aws_bucket_conn::list_buckets(std::vector<std::string> &buckets) const
{
    Aws::S3Crt::Model::ListBucketsOutcome outcome = m_s3_crt_client.ListBuckets();
    if (outcome.IsSuccess()) {
        for (Aws::S3Crt::Model::Bucket bucket : outcome.GetResult().GetBuckets())
            buckets.push_back(bucket.GetName());
        return true;
    } else {
        std::cerr << "Error in list_buckets: " << outcome.GetError().GetMessage() << std::endl
                  << std::endl;
        return false;
    }
}

bool
aws_bucket_conn::list_objects(
  const Aws::String bucket_name, std::vector<Aws::S3Crt::Model::Object> &bucket_objects) const
{
    Aws::S3Crt::Model::ListObjectsRequest request;
    request.WithBucket(bucket_name);
    Aws::S3Crt::Model::ListObjectsOutcome outcomes = m_s3_crt_client.ListObjects(request);

    if (outcomes.IsSuccess()) {
        bucket_objects = outcomes.GetResult().GetContents();
        return true;
    } else {
        std::cerr << "Error in list_buckets: " << outcomes.GetError().GetMessage()
                  << std::endl;
        return false;
    }
}

bool
aws_bucket_conn::put_object(
  const Aws::String &bucket_name, const Aws::String &object_key, const Aws::String &file_name) const
{
    Aws::S3Crt::Model::PutObjectRequest request;
    request.SetBucket(bucket_name);
    request.SetKey(object_key);

    std::shared_ptr<Aws::IOStream> input_data = Aws::MakeShared<Aws::FStream>(
      "s3-source", file_name.c_str(), std::ios_base::in | std::ios_base::binary);

    if (!input_data->good()) {
        std::cerr << "Error in put_object: Failed to open file'" << file_name << "'." << std::endl;
        return false;
    }

    request.SetBody(input_data);

    Aws::S3Crt::Model::PutObjectOutcome outcome = m_s3_crt_client.PutObject(request);

    if (outcome.IsSuccess()) {
        return true;
    } else {
        std::cerr << "Error in put_object: " << outcome.GetError().GetMessage() << std::endl;
        return false;
    }
}

bool
aws_bucket_conn::delete_object(const Aws::String &bucket_name, const Aws::String &object_key) const
{
    Aws::S3Crt::Model::DeleteObjectRequest request;
    request.SetBucket(bucket_name);
    request.SetKey(object_key);

    Aws::S3Crt::Model::DeleteObjectOutcome outcome = m_s3_crt_client.DeleteObject(request);

    if (outcome.IsSuccess()) {
        return true;
    } else {
        std::cerr << "Error in delete_object: " << outcome.GetError().GetMessage() << std::endl;
        return false;
    }
}

aws_bucket_conn::aws_bucket_conn(const Aws::S3Crt::ClientConfiguration &config)
    : m_s3_crt_client(config){};

aws_bucket_conn::~aws_bucket_conn() {}