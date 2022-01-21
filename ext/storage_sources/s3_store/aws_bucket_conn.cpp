#include <aws/core/Aws.h>
#include <aws/s3-crt/S3CrtClient.h>
#include <iostream>
#include <fstream>

#include "aws_bucket_conn.h"

#include <aws/s3-crt/model/DeleteObjectRequest.h>
#include <aws/s3-crt/model/ListObjectsRequest.h>
#include <aws/s3-crt/model/PutObjectRequest.h>

#include <aws/s3-crt/model/Object.h>
#include <vector>
#include <string>
#include <sys/stat.h>

bool
aws_bucket_conn::list_buckets(Aws::Vector<Aws::S3Crt::Model::Bucket> *buckets) const
{
    Aws::S3Crt::Model::ListBucketsOutcome outcome = m_s3_crt_client.ListBuckets();

    if (outcome.IsSuccess()) {
        *buckets = outcome.GetResult().GetBuckets();
        return true;
    } else {
        std::cout << "Error in list_buckets: " << outcome.GetError().GetMessage() << std::endl
                  << std::endl;
        return false;
    }
}

bool
aws_bucket_conn::list_bucket_objects(
  const Aws::String bucket_name, Aws::Vector<Aws::S3Crt::Model::Object> *bucket_objects) const
{
    Aws::S3Crt::Model::ListObjectsRequest request;
    request.WithBucket(bucket_name);
    Aws::S3Crt::Model::ListObjectsOutcome outcomes = m_s3_crt_client.ListObjects(request);

    if (outcomes.IsSuccess()) {
        *bucket_objects = outcomes.GetResult().GetContents();
        return true;
    } else {
        std::cout << "Error in list_bucket_objects: " << outcomes.GetError().GetMessage()
                  << std::endl;
        return false;
    }
}

bool
aws_bucket_conn::put_object(
  const Aws::String &bucket_name, const Aws::String &object_key, const Aws::String &file_name) const
{
    std::cout << "Adding object '" << object_key << "' to bucket '" << bucket_name << "'..."
              << std::endl;

    struct stat buffer;
    if (stat(file_name.c_str(), &buffer) == -1) {
        std::cout << "Error in put_object: File '" << file_name << "' does not exist." << std::endl;
        return false;
    }

    Aws::S3Crt::Model::PutObjectRequest request;
    request.SetBucket(bucket_name);
    request.SetKey(object_key);

    std::shared_ptr<Aws::IOStream> input_data = Aws::MakeShared<Aws::FStream>(
      "s3-source", file_name.c_str(), std::ios_base::in | std::ios_base::binary);

    if (!input_data->good()) {
        std::cout << "Error in put_object: Failed to open file'" << file_name << "'." << std::endl;
        return false;
    }

    request.SetBody(input_data);

    Aws::S3Crt::Model::PutObjectOutcome outcome = m_s3_crt_client.PutObject(request);

    if (outcome.IsSuccess()) {
        std::cout << "Added object '" << object_key << "' to bucket '" << bucket_name << "'."
                  << std::endl;
        std::cout << std::endl;
        return true;
    } else {
        std::cout << "Error in put_object: " << outcome.GetError().GetMessage() << std::endl;
        return false;
    }
}

bool
aws_bucket_conn::delete_object(const Aws::String &bucket_name, const Aws::String &object_key) const
{
    std::cout << "Deleting object '" << object_key << "' from bucket '" << bucket_name << "'..."
              << std::endl;

    Aws::S3Crt::Model::DeleteObjectRequest request;
    request.SetBucket(bucket_name);
    request.SetKey(object_key);

    Aws::S3Crt::Model::DeleteObjectOutcome outcome = m_s3_crt_client.DeleteObject(request);

    if (outcome.IsSuccess()) {
        std::cout << "Deleted object '" << object_key << "' from bucket '" << bucket_name << "'."
                  << std::endl;
        std::cout << std::endl;
        return true;
    } else {
        std::cout << "Error in delete_object: " << outcome.GetError().GetMessage() << std::endl;
        return false;
    }
}

aws_bucket_conn::aws_bucket_conn(const Aws::S3Crt::ClientConfiguration &config)
    : m_s3_crt_client(config)
{
    std::cout << "Starting connection." << std::endl;
    std::cout << std::endl;
};

aws_bucket_conn::~aws_bucket_conn()
{
    std::cout << "Closing connection." << std::endl;
    std::cout << std::endl;
}