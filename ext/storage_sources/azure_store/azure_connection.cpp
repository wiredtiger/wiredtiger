#include "azure_connection.h"

// Includes necessary for connection
#include <azure/core.hpp>
#include <azure/storage/blobs.hpp>

// Constructor for Azure Connection
azure_connection::azure_connection(const std::string &bucket_name, const std::string &obj_prefix)
    : _azure_client(Azure::Storage::Blobs::BlobContainerClient::CreateFromConnectionString(
        std::getenv("AZURE_STORAGE_CONNECTION_STRING"), bucket_name)),
      _bucket_name(bucket_name), _object_prefix(obj_prefix)
{
}

int
azure_connection::list_objects(std::vector<std::string> &objects) const
{
    return (0);
}

int
azure_connection::put_object(const std::string &file_name) const
{
    return (0);
}

int
azure_connection::delete_object() const
{
    return (0);
}

int
azure_connection::get_object(const std::string &path) const
{
    return (0);
}
