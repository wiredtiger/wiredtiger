#include "gcp_connection.h"

#include <fstream>

gcp_connection::gcp_connection(const std::string &bucket_name)
    : _gcp_client(google::cloud::storage::Client()), _bucket_name(bucket_name)
{
}


// Builds a list of object names from the bucket.
int
gcp_connection::list_objects(std::vector<std::string> &objects) const
{
    return (0);
}

// Puts an object into a google cloud bucket.
int
gcp_connection::put_object(const std::string &object_key, const std::string &file_name) const
{
    return (0);
}

// Deletes an object from google cloud bucket.
int
gcp_connection::delete_object(const std::string &object_key) const
{
    return (0);
}

// Retrieves an object from the google cloud bucket.
int
gcp_connection::get_object(const std::string &object_key, const std::string &path) const
{
    return (0);
}

// Checks whether an object with the given key exists in the google cloud bucket and also retrieves
// size of the object.
int
gcp_connection::object_exists(const std::string &object_key, bool &exists, size_t &object_size) const
{
    return (0);
}

// Checks whether the bucket configured for the class is accessible to us or not.
int
gcp_connection::bucket_exists(bool &exists) const
{
    return (0);
}
