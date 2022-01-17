
#ifndef AWS_BUCKET_CONN
#define AWS_BUCKET_CONN

#include <aws/core/Aws.h>
#include <aws/s3-crt/S3CrtClient.h>

class awsBucketConn {

    public:

    explicit awsBucketConn(const Aws::S3Crt::ClientConfiguration &config);
   
    bool s3_list_buckets();

    ~awsBucketConn();
    
    private:
    /* data */
    Aws::S3Crt::ClientConfiguration m_aws_config;
    Aws::S3Crt::S3CrtClient m_s3_crt_client;
    // Aws::SDKOptions m_options;
};

#endif