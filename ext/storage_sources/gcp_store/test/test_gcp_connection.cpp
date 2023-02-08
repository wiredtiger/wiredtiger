/*-
 * Public Domain 2014-present MongoDB, Inc.
 * Public Domain 2008-2014 WiredTiger, Inc.
 *
 * This is free and unencumbered software released into the public domain.
 *
 * Anyone is free to copy, modify, publish, use, compile, sell, or
 * distribute this software, either in source code form or as a compiled
 * binary, for any purpose, commercial or non-commercial, and by any
 * means.
 *
 * In jurisdictions that recognize copyright laws, the author or authors
 * of this software dedicate any and all copyright interest in the
 * software to the public domain. We make this dedication for the benefit
 * of the public at large and to the detriment of our heirs and
 * successors. We intend this dedication to be an overt act of
 * relinquishment in perpetuity of all present and future rights to this
 * software under copyright law.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#define CATCH_CONFIG_MAIN
#include <catch2/catch.hpp>

#include "gcp_connection.h"
#include <typeinfo>

namespace gcs = google::cloud::storage;
using namespace gcs;

static std::string
create_file(const std::string file_name, const std::string payload)
{
    std::ofstream file(file_name);
    file << payload;
    file.close();
    return file_name;
}

static auto
upload_file(gcs::Client client, std::string bucket_name, const std::string bucket_prefix,
  const std::string file_name, const std::string object_name)
{
    auto metadata = client.UploadFile("./" + file_name, bucket_name, bucket_prefix + object_name);

    return metadata;
}

static bool
check_exist(gcs::Client client, std::string bucket_name, const std::string bucket_prefix,
  const std::string object_name)
{
    auto metadata = client.GetObjectMetadata(bucket_name, bucket_prefix + object_name);

    // Metadata ok implies that the file is present.
    if (metadata.ok()) {
        return true;
    }

    return false;
}

static bool
check_object_num(
  gcs::Client client, std::string bucket_name, const std::string bucket_prefix, const int num)
{
    auto objects_iterator = client.ListObjects(bucket_name, gcs::Prefix(bucket_prefix));
    return std::distance(objects_iterator.begin(), objects_iterator.end()) == num;
}

// Concatenates a random suffix to the prefix being used for the test object keys. Example of
// generated test prefix: "gcptest/unit/2022-31-01-16-34-10/623843294--".
static std::string
randomize_test_prefix(std::string prefix)
{
    char time_str[100];
    std::time_t t = std::time(nullptr);

    REQUIRE(std::strftime(time_str, sizeof(time_str), "%F-%H-%M-%S", std::localtime(&t)) != 0);

    prefix += time_str;

    // Create a random device and use it to generate a random seed to initialize the generator.
    std::random_device my_random_device;
    unsigned seed = my_random_device();
    std::default_random_engine my_random_engine(seed);

    prefix += '/' + std::to_string(my_random_engine());
    prefix += "--";

    return prefix;
}

TEST_CASE("Testing class gcpConnection", "gcp-connection")
{
    std::string bucket_name = "unit_testing_gcp";
    std::string prefix = "gcptest-unit-";

    // Setup the test environment.
    std::string bucket_prefix = randomize_test_prefix(prefix);
    gcp_connection conn(bucket_name, bucket_prefix);

    const std::string object_name = "test_object";
    const std::string file_name = object_name + ".txt";
    const std::string non_existant_object_name = "test_non_exist";
    const std::string non_existant_file_name = non_existant_object_name + ".txt";
    const bool list_single = true;
    std::vector<std::string> objects;

    auto client = gcs::Client();

    std::string payload = "Test payload :)";
    create_file(file_name, payload);

    SECTION("Simple list test", "[gcp-connection]")
    {
        // No matching objects. Object size should be 0.
        REQUIRE(conn.list_objects(objects, false) == 0);
        REQUIRE(objects.empty());

        // No matching objects with list_single. Object size should be 0.
        REQUIRE(conn.list_objects(objects, list_single) == 0);
        REQUIRE(objects.empty());

        // Upload 1 file and test list.
        upload_file(client, bucket_name, bucket_prefix, file_name, object_name);
        REQUIRE(conn.list_objects(objects, false) == 0);
        REQUIRE(objects.size() == 1);
        objects.clear();

        // Delete that object and test list.
        client.DeleteObject(bucket_name, bucket_prefix + object_name);
        REQUIRE(conn.list_objects(objects, false) == 0);
        REQUIRE(objects.empty());
        objects.clear();

        // Upload multiple files and test list.
        const int32_t total_objects = 20;
        for (int i = 0; i < total_objects; i++) {
            std::string multi_file_name = object_name + std::to_string(i);
            REQUIRE(
              upload_file(client, bucket_name, bucket_prefix, file_name, multi_file_name).ok());
        }
        REQUIRE(conn.list_objects(objects, false) == 0);
        REQUIRE(objects.size() == total_objects);
        objects.clear();

        // Test if list single only returns one object.
        REQUIRE(conn.list_objects(objects, list_single) == 0);
        REQUIRE(objects.size() == 1);
        objects.clear();

        // Delete all object we uploaded.
        for (int i = 0; i < total_objects; i++) {
            client.DeleteObject(bucket_name, bucket_prefix + object_name + std::to_string(i));
        }

        // Bucket should be cleared.
        REQUIRE(conn.list_objects(objects, false) == 0);
        REQUIRE(objects.empty());
    }

    SECTION("Simple put test", "[gcp-connection]")
    {

        // Upload a file that does not exist locally - should fail.
        REQUIRE(conn.put_object(non_existant_object_name, non_existant_file_name) == ENOENT);

        // Check number of files with the given prefix currently in bucket.
        REQUIRE(check_object_num(client, bucket_name, bucket_prefix, 0));

        // Upload a test file.
        REQUIRE(conn.put_object(object_name, "./" + file_name) == 0);

        REQUIRE(check_object_num(client, bucket_name, bucket_prefix, 1));

        // Check the bucket contains the file.
        bool exists = check_exist(client, bucket_name, bucket_prefix, object_name);
        REQUIRE(exists);

        // Delete the uploaded file.
        client.DeleteObject(bucket_name, bucket_prefix + object_name);

        exists = check_exist(client, bucket_name, bucket_prefix, object_name);
        REQUIRE_FALSE(exists);
    }

    SECTION("Simple delete test", "[gcp-connection]")
    {

        // Delete a file that does not exist in the bucket - should fail.
        REQUIRE(conn.delete_object(non_existant_object_name) == ENOENT);

        // Upload a test file
        auto metadata = upload_file(client, bucket_name, bucket_prefix, file_name, object_name);
        REQUIRE(metadata.ok());

        REQUIRE(check_object_num(client, bucket_name, bucket_prefix, 1));

        // Delete the uploaded file
        REQUIRE(conn.delete_object(object_name) == 0);

        // Check that the file has been deleted
        REQUIRE(check_object_num(client, bucket_name, bucket_prefix, 0));

        bool exists = check_exist(client, bucket_name, bucket_prefix, object_name);
        REQUIRE_FALSE(exists);
        REQUIRE_FALSE(check_exist(client, bucket_name, bucket_prefix, object_name));
    }

    SECTION("Simple object exists test", "[gcp-connection]")
    {
        bool exists;
        size_t size;

        REQUIRE(conn.object_exists(object_name, exists, size) == 0);
        REQUIRE(exists == false);
        REQUIRE(size == 0);

        // Upload a test file
        auto metadata = upload_file(client, bucket_name, bucket_prefix, file_name, object_name);
        REQUIRE(metadata.status().ok());

        REQUIRE(check_object_num(client, bucket_name, bucket_prefix, 1));

        // Check the bucket contains the file
        REQUIRE(conn.object_exists(object_name, exists, size) == 0);
        REQUIRE(exists);
        REQUIRE(size != 0);

        // Delete the uploaded file
        auto dl_metadata = client.DeleteObject(bucket_name, bucket_prefix + object_name);
        REQUIRE(dl_metadata.ok());

        // Check that the file has been deleted
        REQUIRE(check_object_num(client, bucket_name, bucket_prefix, 0));

        REQUIRE(conn.object_exists(object_name, exists, size) == 0);
        REQUIRE(exists == false);
        REQUIRE(size == 0);
    }

    SECTION("Read tests", "[gcp-connection]")
    {
        // Upload a test file
        auto metadata = upload_file(client, bucket_name, bucket_prefix, file_name, object_name);
        REQUIRE(metadata.status().ok());

        REQUIRE(check_object_num(client, bucket_name, bucket_prefix, 1));

        // Check the bucket contains the file
        metadata = client.GetObjectMetadata(bucket_name, bucket_prefix + object_name);
        REQUIRE(metadata.ok());

        // Read GCP objects under the test bucket with no offset.
        char buf[1024];

        REQUIRE(conn.read_object(object_name, 0, 15, buf) == 0);
        REQUIRE(payload.compare(buf) == 0);
        memset(buf, 0, 1000);

        // Read GCP objects under the test bucket with offset.
        REQUIRE(conn.read_object(object_name, 4, 11, buf) == 0);
        REQUIRE(payload.substr(4, 16).compare(buf) == 0);
        memset(buf, 0, 1000);

        // Read GCP objects under the test bucket with len > file length.
        REQUIRE(conn.read_object(object_name, 0, 100000, buf) == EINVAL);
        memset(buf, 0, 1000);

        // Read GCP objects under the test bucket with offset < 0.
        REQUIRE(conn.read_object(object_name, -5, 15, buf) == EINVAL);
        memset(buf, 0, 1000);

        // Read GCP objects under the test bucket with offset > file length.
        REQUIRE(conn.read_object(object_name, 1000, 15, buf) == EINVAL);
    }

    // Cleanup
    // List and loop through objects with prefix.
    for (auto &&object_metadata : client.ListObjects(bucket_name, gcs::Prefix(bucket_prefix))) {
        // Delete the test file.
        if (object_metadata) {
            auto dl_metadata = client.DeleteObject(bucket_name, object_metadata.value().name());
            REQUIRE(dl_metadata.ok());
        }
    }
}
