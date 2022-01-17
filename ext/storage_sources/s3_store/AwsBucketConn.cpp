#include <aws/core/Aws.h>
#include <aws/s3-crt/S3CrtClient.h>
#include <iostream>
#include "AwsBucketConn.h"

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


awsBucketConn::awsBucketConn(const Aws::S3Crt::ClientConfiguration &config)
    : m_aws_config(config)
    , m_s3_crt_client(config)
    {
    std::cout << "Constructing obj" << std::endl;
    };

awsBucketConn::~awsBucketConn() {
    std::cout << "Destructing obj" << std::endl;
}