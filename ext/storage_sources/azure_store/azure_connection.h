#ifndef AZURECONNECTION
#define AZURECONNECTION

// Includes necessary for connection
#include <azure/core.hpp>
#include <azure/storage/blobs.hpp>

#include <string>

class AzureConnection {
    public:
    ~AzureConnection() = default;

    private:
    const std::string _bucketName;
    const std::string _objectPrefix;
    const Azure::Storage::Blobs::BlobContainerClient _azureClient;
};
#endif
