#include <aws/core/Aws.h>
#include <aws/s3-crt/S3CrtClient.h>
#include <iostream>
#include <fstream>

#include "AwsBucketConn.h"
#include <aws/s3-crt/model/ListObjectsRequest.h>
#include <aws/s3-crt/model/PutObjectRequest.h>
#include <aws/s3-crt/model/DeleteObjectRequest.h>

#include <aws/s3/model/Object.h>
#include <vector>
#include <string>
#include <sys/stat.h>




bool awsBucketConn::s3_list_buckets()
{
    Aws::S3Crt::Model::ListBucketsOutcome outcome = m_s3_crt_client.ListBuckets();

    if (outcome.IsSuccess()) {
        std::cout << "All buckets under my account:" << std::endl;
        for (auto const &bucket : outcome.GetResult().GetBuckets()) {
            std::cout << "  * " << bucket.GetName() << std::endl;
        }
        std::cout << std::endl;
        return true;
    } else {
        std::cout << "ListBuckets error:\n" << outcome.GetError() << std::endl << std::endl;
        return false;
    }
}

bool awsBucketConn::list_bucket_objects(const std::string bucket_name){

    Aws::S3Crt::Model::ListObjectsRequest request;
    request.WithBucket(bucket_name);
    auto outcomes = m_s3_crt_client.ListObjects(request);

    if (outcomes.IsSuccess())
        {
            std::cout << "Objects in bucket '" << bucket_name << "':" 
                << std::endl << std::endl;
            
            Aws::Vector<Aws::S3Crt::Model::Object> objects =
                outcomes.GetResult().GetContents();

            for (Aws::S3Crt::Model::Object& object : objects)
            {
                std::cout << object.GetKey() << std::endl;
            }

            return true;
        }
        else
        {
            std::cout << "Error: ListObjects: " <<
                outcomes.GetError().GetMessage() << std::endl;

            return false;
        }


}

bool awsBucketConn::put_object(const Aws::String& objectName, const std::string bucket_name, const std::string key_name){
    struct stat buffer;

    if (stat(objectName.c_str(), &buffer) == -1)
        {
            std::cout << "Error: PutObject: File '" <<
                key_name << "' does not exist." << std::endl;

            return false;
        }
    
    Aws::S3Crt::Model::PutObjectRequest request;
    request.SetBucket(bucket_name);
    
    request.SetKey(key_name);

    std::shared_ptr<Aws::IOStream> input_data = 
        Aws::MakeShared<Aws::FStream>("s3-source", objectName.c_str(),
            std::ios_base::in | std::ios_base::binary);

    if (!input_data->good()) {
        std::cout << "Failed to open file: \"" << objectName << "\"." << std::endl << std::endl;
        return false;
    }

    request.SetBody(input_data);

    Aws::S3Crt::Model::PutObjectOutcome outcome = 
        m_s3_crt_client.PutObject(request);

    if (outcome.IsSuccess()) {

        std::cout << "Added object '" << key_name << "' to bucket '"
            << bucket_name << "'.";
        return true;
    }
    else 
    {
        std::cout << "Error: PutObject: " << 
            outcome.GetError().GetMessage() << std::endl;
       
        return false;
    }
}

bool awsBucketConn::delete_object(const Aws::String& bucket_name, const Aws::String& object_key) {

    std::cout << "\n Deleting object: \"" << object_key << "\" from bucket: \"" << bucket_name << "\" ..." << std::endl;

    Aws::S3Crt::Model::DeleteObjectRequest request;
    request.SetBucket(bucket_name);
    request.SetKey(object_key);

    Aws::S3Crt::Model::DeleteObjectOutcome outcome = m_s3_crt_client.DeleteObject(request);

    if (outcome.IsSuccess()) {
        std::cout << "Object deleted." << std::endl << std::endl;

        return true;
    }
    else {
        std::cout << "DeleteObject error:\n" << outcome.GetError() << std::endl << std::endl;

        return false;
    }
}



awsBucketConn::awsBucketConn(const Aws::S3Crt::ClientConfiguration &config)
    : m_aws_config(config)
    , m_s3_crt_client(config)
    {
    std::cout << "\n Constructing obj" << std::endl;
    };

awsBucketConn::~awsBucketConn() {
    std::cout << "\n Destructing obj" << std::endl;
}