
#ifndef AWS_BUCKET_CONN
#define AWS_BUCKET_CONN

#include <aws/core/Aws.h>
#include <aws/s3-crt/S3CrtClient.h>
#include <aws/s3-crt/model/Object.h>

#include <string>
#include <vector>

/*
 * Class to represent an active connection to the AWS S3 endpoint. Allows for interaction with s3
 * client.
 */
class aws_bucket_conn {
    public:
    explicit aws_bucket_conn(const Aws::S3Crt::ClientConfiguration &config);
    bool list_buckets(std::vector<std::string> &buckets) const;
    bool list_objects(
      const std::string &bucket_name, std::vector<Aws::S3Crt::Model::Object> &bucket_objects) const;
    bool put_object(const Aws::String &bucket_name, const Aws::String &object_key,
      const Aws::String &file_name) const;
    bool delete_object(const Aws::String &bucket_name, const Aws::String &object_key) const;

    private:
    const Aws::S3Crt::S3CrtClient m_s3_crt_client;
};
#endif
