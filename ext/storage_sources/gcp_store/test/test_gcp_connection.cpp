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
namespace test_defaults {
static std::string bucket_name("unit_testing_gcp");

/*
 * Objects with the prefex pattern "gcptest/*" are deleted after a certain period of time according
 * to the lifecycle rule on the gcp bucket. Should you wish to make any changes to the prefix
 * pattern or lifecycle of the object, please speak to the release manager.
 */
static std::string obj_prefix("gcptest"); // To be concatenated with a random string.
} // namespace test_defaults

// Concatenates a random suffix to the prefix being used for the test object keys. Example of
// generated test prefix: "gcptest/unit/2022-31-01-16-34-10/623843294--".
static int
randomize_test_prefix()
{
    char time_str[100];
    std::time_t t = std::time(nullptr);
    std::string new_prefix = "gcptest-unit-";

    REQUIRE(std::strftime(time_str, sizeof(time_str), "%F-%H-%M-%S", std::localtime(&t)) != 0);

    new_prefix += time_str;

    // Create a random device and use it to generate a random seed to initialize the generator.
    std::random_device my_random_device;
    unsigned seed = my_random_device();
    std::default_random_engine my_random_engine(seed);

    new_prefix += '/' + std::to_string(my_random_engine());
    new_prefix += "--";

    test_defaults::obj_prefix = new_prefix;

    return (0);
}

// Overrides the defaults with the ones specific for this test instance.
static int
setup_test_defaults()
{
    // Append the prefix to be used for object names by a unique string.
    REQUIRE(randomize_test_prefix() == 0);
    // std::cerr << "Generated prefix: " << test_defaults::obj_prefix << std::endl;

    return (0);
}

TEST_CASE("Testing class gcpConnection", "gcp-connection")
{
    // Setup the test environment.
    REQUIRE(setup_test_defaults() == 0);

    gcp_connection conn(test_defaults::bucket_name, test_defaults::obj_prefix);
    // bool exists = false;
    // size_t objectSize;

    const std::string object_key = "test_object";
    const std::string file_name = "test_object.txt";
    const std::string file_path = "./" + file_name;
    const std::string non_exi_object_key = "test_non_exist";
    const std::string non_exi_file_name = "test_non_exist.txt";
    const bool list_single = true;
    std::vector<std::string> objects;

    google::cloud::storage::Client client = conn.get_client();

    std::ofstream File(file_name);
    std::string payload = "Test payload :)";
    File << payload;
    File.close();

    SECTION("Simple list test", "[gcp-connection]")
    {
        const int32_t total_objects = 20;

        // No matching objects. Object size should be 0.
        REQUIRE(conn.list_objects(objects, false) == 0);
        REQUIRE(objects.size() == 0);

        // No matching objects with list_single. Object size should be 0.
        REQUIRE(conn.list_objects(objects, list_single) == 0);
        REQUIRE(objects.size() == 0);

        // Upload 1 file and test list.
        objects.clear();
        client.UploadFile(
          file_path, test_defaults::bucket_name, test_defaults::obj_prefix + object_key);
        REQUIRE(conn.list_objects(objects, false) == 0);
        REQUIRE(objects.size() == 1);

        // Delete that object and test list.
        objects.clear();
        client.DeleteObject(test_defaults::bucket_name, test_defaults::obj_prefix + object_key);
        REQUIRE(conn.list_objects(objects, false) == 0);
        REQUIRE(objects.size() == 0);

        // Upload multiple files and test list.
        objects.clear();
        for (int i = 0; i < total_objects; i++) {
            client.UploadFile(file_path, test_defaults::bucket_name,
              test_defaults::obj_prefix + std::to_string(i) + ".txt");
        }
        REQUIRE(conn.list_objects(objects, false) == 0);
        REQUIRE(objects.size() == total_objects);

        // Test if list single only returns one object.
        objects.clear();
        REQUIRE(conn.list_objects(objects, list_single) == 0);
        REQUIRE(objects.size() == 1);

        for (int i = 0; i < total_objects; i++) {
            client.DeleteObject(
              test_defaults::bucket_name, test_defaults::obj_prefix + std::to_string(i) + ".txt");
        }
    }

    SECTION("Simple put test", "[gcp-connection]")
    {

        // Upload a file that does not exist locally - should fail.
        REQUIRE(conn.put_object(non_exi_object_key, non_exi_file_name) == ENOENT);

        // Check number of files with the given prefix currently in bucket
        auto objects_iterator =
          client.ListObjects(test_defaults::bucket_name, gcs::Prefix(test_defaults::obj_prefix));
        int original_number_of_files =
          std::distance(objects_iterator.begin(), objects_iterator.end());
        std::cerr << "\n\n\n" << typeid(objects_iterator).name() << std::endl;
        // Upload a test file
        REQUIRE(conn.put_object(object_key, file_path) == 0);

        objects_iterator =
          client.ListObjects(test_defaults::bucket_name, gcs::Prefix(test_defaults::obj_prefix));

        REQUIRE(std::distance(objects_iterator.begin(), objects_iterator.end()) ==
          original_number_of_files + 1);

        // Check the bucket contains the file
        auto metadata = client.GetObjectMetadata(
          test_defaults::bucket_name, test_defaults::obj_prefix + object_key);
        REQUIRE(metadata.status().ok());

        // Delete the uploaded file
        auto dl_metadata =
          client.DeleteObject(test_defaults::bucket_name, test_defaults::obj_prefix + object_key);
        REQUIRE(dl_metadata.ok());

        // Check that the file has been deleted
        objects_iterator =
          client.ListObjects(test_defaults::bucket_name, gcs::Prefix(test_defaults::obj_prefix));

        REQUIRE(std::distance(objects_iterator.begin(), objects_iterator.end()) ==
          original_number_of_files);

        metadata = client.GetObjectMetadata(
          test_defaults::bucket_name, test_defaults::obj_prefix + file_name);
        REQUIRE_FALSE(metadata.ok());
    }

    SECTION("Simple delete test", "[gcp-connection]")
    {

        // Delete a file that does not exist in the bucket - should fail.
        REQUIRE(conn.delete_object(non_exi_object_key) == ENOENT);

        // Check number of files with the given prefix currently in bucket
        auto objects_iterator =
          client.ListObjects(test_defaults::bucket_name, gcs::Prefix(test_defaults::obj_prefix));
        int original_number_of_files =
          std::distance(objects_iterator.begin(), objects_iterator.end());

        // Upload a test file
        auto metadata = client.UploadFile(
          file_path, test_defaults::bucket_name, test_defaults::obj_prefix + file_name);
        REQUIRE(metadata.status().ok());

        objects_iterator =
          client.ListObjects(test_defaults::bucket_name, gcs::Prefix(test_defaults::obj_prefix));

        REQUIRE(std::distance(objects_iterator.begin(), objects_iterator.end()) ==
          original_number_of_files + 1);

        // Check the bucket contains the file
        metadata = client.GetObjectMetadata(
          test_defaults::bucket_name, test_defaults::obj_prefix + file_name);
        REQUIRE(metadata.ok());

        // Delete the uploaded file
        REQUIRE(conn.delete_object(file_name) == 0);

        // Check that the file has been deleted
        objects_iterator =
          client.ListObjects(test_defaults::bucket_name, gcs::Prefix(test_defaults::obj_prefix));

        REQUIRE(std::distance(objects_iterator.begin(), objects_iterator.end()) ==
          original_number_of_files);

        metadata = client.GetObjectMetadata(
          test_defaults::bucket_name, test_defaults::obj_prefix + file_name);
        REQUIRE_FALSE(metadata.ok());
    }

    SECTION("Simple object exists test", "[gcp-connection]")
    {
        // Check number of files with the given prefix currently in bucket
        auto objects_iterator =
          client.ListObjects(test_defaults::bucket_name, gcs::Prefix(test_defaults::obj_prefix));
        int original_number_of_files =
          std::distance(objects_iterator.begin(), objects_iterator.end());
        
        bool exists;
        size_t size;

        REQUIRE(conn.object_exists(object_key, exists, size) == 0);
        REQUIRE(exists == false);
        REQUIRE(size == 0);

        // Upload a test file
        auto metadata = client.UploadFile(
          file_path, test_defaults::bucket_name, test_defaults::obj_prefix + object_key);
        REQUIRE(metadata.status().ok());

        objects_iterator =
          client.ListObjects(test_defaults::bucket_name, gcs::Prefix(test_defaults::obj_prefix));

        REQUIRE(std::distance(objects_iterator.begin(), objects_iterator.end()) ==
          original_number_of_files + 1);

        // Check the bucket contains the file
        REQUIRE(conn.object_exists(object_key, exists, size) == 0);
        REQUIRE(exists);
        REQUIRE(size != 0);

        // Delete the uploaded file
        auto dl_metadata =
          client.DeleteObject(test_defaults::bucket_name, test_defaults::obj_prefix + object_key);
        REQUIRE(dl_metadata.ok());

        // Check that the file has been deleted
        objects_iterator =
          client.ListObjects(test_defaults::bucket_name, gcs::Prefix(test_defaults::obj_prefix));

        REQUIRE(std::distance(objects_iterator.begin(), objects_iterator.end()) ==
          original_number_of_files);

        REQUIRE(conn.object_exists(object_key, exists, size) == 0);
        REQUIRE(exists == false);
        REQUIRE(size == 0);
    }

    SECTION("Read tests", "[gcp-connection]")
    {
        // Check number of files with the given prefix currently in bucket
        auto objects_iterator =
          client.ListObjects(test_defaults::bucket_name, gcs::Prefix(test_defaults::obj_prefix));
        int original_number_of_files =
          std::distance(objects_iterator.begin(), objects_iterator.end());

        // Upload a test file
        auto metadata = client.UploadFile(
          file_path, test_defaults::bucket_name, test_defaults::obj_prefix + file_name);
        REQUIRE(metadata.status().ok());

        objects_iterator =
          client.ListObjects(test_defaults::bucket_name, gcs::Prefix(test_defaults::obj_prefix));

        REQUIRE(std::distance(objects_iterator.begin(), objects_iterator.end()) ==
          original_number_of_files + 1);

        // Check the bucket contains the file
        metadata = client.GetObjectMetadata(
          test_defaults::bucket_name, test_defaults::obj_prefix + file_name);
        REQUIRE(metadata.ok());

        int len;
        int offset;

        SECTION("Read GCP objects under the test bucket with no offset.", "[gcp-connection]")
        {

            len = 15;
            offset = 0;
            void *buf = calloc(len, sizeof(char));

            REQUIRE(conn.read_object(file_name, offset, len, buf) == 0);
            REQUIRE(static_cast<char *>(buf) == payload);
            free(buf);
        }

        SECTION("Read GCP objects under the test bucket with offset.", "[gcp-connection]")
        {

            len = 11;
            offset = 4;
            void *buf = calloc(len, sizeof(char));

            REQUIRE(conn.read_object(file_name, offset, len, buf) == 0);
            REQUIRE(static_cast<char *>(buf) == payload.substr(4, 16));
            free(buf);
        }

        SECTION(
          "Read GCP objects under the test bucket with len > file length.", "[gcp-connection]")
        {

            len = 1000;
            offset = 0;
            void *buf = calloc(len, sizeof(char));

            REQUIRE(conn.read_object(file_name, offset, len, buf) == EINVAL);

            free(buf);
        }

        SECTION("Read GCP objects under the test bucket with offset < 0.", "[gcp-connection]")
        {

            len = 15;
            offset = -5;
            void *buf = calloc(len, sizeof(char));

            REQUIRE(conn.read_object(file_name, offset, len, buf) == EINVAL);

            free(buf);
        }

        SECTION(
          "Read GCP objects under the test bucket with offset > file length.", "[gcp-connection]")
        {

            len = 15;
            offset = 1000;
            void *buf = calloc(len, sizeof(char));

            REQUIRE(conn.read_object(file_name, offset, len, buf) == EINVAL);
            free(buf);
        }
    }

    // Cleanup
    // List and loop through objects with prefix.
    for (auto &&object_metadata :
      client.ListObjects(test_defaults::bucket_name, gcs::Prefix(test_defaults::obj_prefix))) {
        // Delete the test file.
        if (object_metadata) {
            auto dl_metadata =
              client.DeleteObject(test_defaults::bucket_name, object_metadata.value().name());
            REQUIRE(dl_metadata.ok());
        }
    }
}
