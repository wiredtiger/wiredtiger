
#ifndef S3CONNECTION
#define S3CONNECTION

#include <aws/core/Aws.h>
#include <aws/s3-crt/S3CrtClient.h>

#include <string>
#include <vector>

/*
 * Class to represent an active connection to the AWS S3 endpoint. Allows for interaction with S3
 * client.
 */
class S3Connection {
    public:
    explicit S3Connection(const Aws::S3Crt::ClientConfiguration &config);
    int ListBuckets(std::vector<std::string> &buckets) const;

    /*!
     * Return a vector of object names from an S3 bucket.
     *
     * @param bucketName the name of the bucket.
     * @param prefix only objects with names matching the prefix are returned.
     * @param objects the vector of objects retrieved from S3 bucket.
     * @param countp the number of entries returned.
     * @param nPerIter the number of objects to access per iteration of the AWS request, from 1 to
     * 1000. Defaults to 1000.
     * @param maxObjects the maximum number of objects to return. Defaults to 0, which returns all
     * objects.
     * @param[out] ret returns 0 for success, 1 for failure.
     */
    int ListObjects(const std::string &bucketName, const std::string &prefix,
      std::vector<std::string> &objects, uint32_t &countp, uint32_t nPerIter = 1000,
      uint32_t maxObjects = 0) const;

    int PutObject(const std::string &bucketName, const std::string &objectKey,
      const std::string &fileName) const;
    int DeleteObject(const std::string &bucketName, const std::string &objectKey) const;
    ~S3Connection() = default;

    private:
    const Aws::S3Crt::S3CrtClient m_S3CrtClient;
};
#endif
