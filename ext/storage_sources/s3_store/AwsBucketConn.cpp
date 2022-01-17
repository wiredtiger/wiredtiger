#include <aws/core/Aws.h>
#include <aws/s3-crt/S3CrtClient.h>
#include <iostream>
#include "AwsBucketConn.h"
 
AwsBucketConn::AwsBucketConn(Aws::S3Crt::ClientConfiguration config, Aws::SDKOptions options)
    : m_aws_config(config), m_options(options){}

void AwsBucketConn::initBucketConn() {
    Aws::InitAPI(m_options);
    Aws::S3Crt::S3CrtClient *s3_crt_client;
    s3_crt_client = new Aws::S3Crt::S3CrtClient(m_aws_config);
    m_s3_crt_client = *s3_crt_client;
}

bool AwsBucketConn::s3_list_buckets()
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