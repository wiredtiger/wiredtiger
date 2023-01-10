#ifndef AZURECONNECTION
#define AZURECONNECTION

// Includes necessary for connection
#include <azure/core.hpp>
#include <azure/storage/blobs.hpp>

#include <string>

class AzureConnection {
    public:
    AzureConnection(const std::string &bucketName, const std::string &objPrefix = "");
    int ListObjects(const std::string &prefix, std::vector<std::string> &objects,
      uint32_t batchSize, bool listSingle) const;
    int PutObject(const std::string &fileName) const;
    int DeleteObject() const;
    int GetObject(const std::string &path) const;
    ~AzureConnection() = default;

    private:
    const std::string _bucketName;
    const std::string _objectPrefix;
    const Azure::Storage::Blobs::BlobContainerClient _azureClient;
};
#endif
