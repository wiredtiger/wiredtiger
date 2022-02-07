
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
    explicit S3Connection(const Aws::S3Crt::ClientConfiguration &config,
      const std::string &bucketName, const std::string &objPrefix = "");
    int ListObjects(std::vector<std::string> &objects) const;
    int PutObject(const std::string &objectKey, const std::string &fileName) const;
    int DeleteObject(const std::string &objectKey) const;
    int ObjectExists(const std::string &objectKey, bool &exists) const;
    ~S3Connection() = default;

    private:
    const Aws::S3Crt::S3CrtClient m_S3CrtClient;
    const std::string m_bucketName;
    const std::string m_objectPrefix;
};
#endif
