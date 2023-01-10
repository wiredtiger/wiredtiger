#include "azure_connection.h"

// Includes necessary for connection
#include <azure/core.hpp>
#include <azure/storage/blobs.hpp>


// Constructor for Azure Connection
AzureConnection::AzureConnection(const std::string &bucketName, 
    const std::string &objPrefix)
    : _azureClient(
        Azure::Storage::Blobs::BlobContainerClient::CreateFromConnectionString(std::getenv("AZURE_STORAGE_CONNECTION_STRING"), bucketName)),
        _bucketName(bucketName), _objectPrefix(objPrefix) { 
}

int AzureConnection::ListObjects(const std::string &prefix, std::vector<std::string> &objects,
    uint32_t batchSize, bool listSingle) const 
{ 
    return 0;
}

int AzureConnection::PutObject(const std::string &fileName) const
{ 
    return 0;
}

int AzureConnection::DeleteObject() const
{ 
    return 0;
}

int AzureConnection::GetObject(const std::string &path) const
{
    return 0;
}
