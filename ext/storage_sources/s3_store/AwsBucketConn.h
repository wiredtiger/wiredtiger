
#ifndef AWS_BUCKET_CONN
#define AWS_BUCKET_CONN

#include <aws/core/Aws.h>
#include <aws/s3-crt/S3CrtClient.h>
#include <aws/s3-crt/model/ListObjectsV2Request.h>
#include <aws/s3-crt/model/ListObjectsV2Result.h>
#include <aws/s3/model/Object.h>
#include <aws/s3-crt/model/ListObjectsRequest.h>
#include <string>

class awsBucketConn {

    public:

    explicit awsBucketConn(const Aws::S3Crt::ClientConfiguration &config);
   
    bool s3_list_buckets();
    bool list_bucket_objects(const std::string bucket_name);
    bool put_object(const Aws::String& objectName, const std::string bucket_name, const std::string key_name);
    bool delete_object(const Aws::String& bucket_name, const Aws::String& object_key);

    ~awsBucketConn();
    
    private:
    /* data */
    Aws::S3Crt::ClientConfiguration m_aws_config;
    Aws::S3Crt::S3CrtClient m_s3_crt_client;
    // const Aws::String& bucket_name;
    // Aws::SDKOptions m_options;
};

#endif