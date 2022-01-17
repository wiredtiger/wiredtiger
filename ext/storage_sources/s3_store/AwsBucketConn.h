#include <aws/core/Aws.h>
#include <aws/s3-crt/S3CrtClient.h>

class AwsBucketConn{
    private:
    Aws::S3Crt::ClientConfiguration m_aws_config;
    Aws::S3Crt::S3CrtClient m_s3_crt_client;
    Aws::SDKOptions m_options;

    public:
    AwsBucketConn(Aws::S3Crt::ClientConfiguration config, Aws::SDKOptions options);

    void initBucketConn();

    bool s3_list_buckets();
};