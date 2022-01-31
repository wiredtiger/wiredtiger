
#ifndef AWS_BUCKET_CONN
#define AWS_BUCKET_CONN

#include <aws/core/Aws.h>
#include <aws/s3-crt/S3CrtClient.h>

#include <string>
#include <vector>

/*
 * Class to represent an active connection to the AWS S3 endpoint. Allows for interaction with S3
 * client.
 */
class aws_bucket_conn {
    public:
    explicit aws_bucket_conn(const Aws::S3Crt::ClientConfiguration &config);
    bool list_buckets(std::vector<std::string> &buckets) const;

    /*!
     * Return a vector of object names from an S3 bucket.
     *
     * @param bucket_name the name of the bucket.
     * @param prefix only objects with names matching the prefix are returned.
     * @param countp the number of entries returned.
     * @param max_objects the maximum number of objects to return. Defaults to 0, which returns all
     * objects.
     * @param n_per_iter the number of objects to access per iteration of the AWS request, from 1 to
     * 1000. Defaults to 1000.
     * @param[out] objects the vector of object names returned.
     */
    std::vector<std::string> list_objects(const std::string &bucket_name, const std::string &prefix,
      uint32_t &countp, uint32_t max_objects = 0, uint32_t n_per_iter = 1000) const;

    bool put_object(const std::string &bucket_name, const std::string &object_key,
      const std::string &file_name) const;
    bool delete_object(const std::string &bucket_name, const std::string &object_key) const;
    ~aws_bucket_conn() = default;

    private:
    const Aws::S3Crt::S3CrtClient m_s3_crt_client;
};
#endif
