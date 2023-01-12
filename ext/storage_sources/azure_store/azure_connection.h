#ifndef AZURECONNECTION
#define AZURECONNECTION

// Includes necessary for connection
#include <azure/core.hpp>
#include <azure/storage/blobs.hpp>

#include <string>

class azure_connection {
    public:
    azure_connection(const std::string &bucket_name, const std::string &obj_prefix = "");
    int list_objects(std::vector<std::string> &objects) const;
    int put_object(const std::string &file_name) const;
    int delete_object() const;
    int get_object(const std::string &path) const;
    ~azure_connection() = default;

    private:
    const std::string _bucket_name;
    const std::string _object_prefix;
    const Azure::Storage::Blobs::BlobContainerClient _azure_client;
};
#endif
