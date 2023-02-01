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

#include "azure_connection.h"
#include <fstream>

TEST_CASE("Testing Azure Connection Class", "azure-connection")
{

    auto azure_client = Azure::Storage::Blobs::BlobContainerClient::CreateFromConnectionString(
      std::getenv("AZURE_STORAGE_CONNECTION_STRING"), "myblobcontainer1");
    bool exists = false;

    azure_connection conn = azure_connection("myblobcontainer1", "unit_testing_");
    azure_connection conn_bad = azure_connection("myblobcontainer1", "bad_prefix_");
    // Prefix for objects in this test.
    const std::string prefix = "unit_testing_";
    const std::string object_name = "test_object";
    const std::string file_name = "test_object.txt";
    const std::string path = "./" + file_name;
    const std::string object_name_2 = "test_object_2";
    const std::string file_name_2 = "test_object_2.txt";
    const std::string path_2 = "./" + file_name_2;
    const std::string non_exi_object_key = "test_non_exist";
    const std::string non_exi_file_name = "test_non_exist.txt";
    const std::string prefix_file_name = prefix + file_name;
    const std::string prefix_file_name_2 = prefix + file_name_2;

    std::ofstream file(file_name);
    const std::string payload = "Test payload";
    const std::string payload_substr = "payload";
    file << payload;
    file.close();

    std::ofstream file_2(file_name_2);
    const std::string payload_2 = "Testing offset and substring";
    const std::string payload_2_substr = "offset";
    file_2 << payload_2;
    file_2.close();

    SECTION("Checks azure connection constructor.", "[azure-connection]")
    {
        REQUIRE_THROWS_WITH(azure_connection("Bad_bucket", ""), "Bad_bucket : No such bucket.");
    }

    SECTION("Check if an object exists in Azure.", "[azure-connection]")
    {
        // Object does not exist yet so there should be 0 objects.
        REQUIRE(conn.object_exists(file_name, exists) == 0);
        REQUIRE(exists == false);

        auto blob_client = azure_client.GetBlockBlobClient(prefix_file_name);
        blob_client.UploadFrom(file_name);

        // Object exists now in the container.
        REQUIRE(conn.object_exists(file_name, exists) == 0);
        REQUIRE(exists == true);

        blob_client.Delete();
    }

    SECTION("Lists Azure objects under the text bucket.", "[azure-connection]")
    {
        std::vector<std::string> objects;

        // Total objects to insert in the test.
        const int32_t num_objects = 11;

        // No matching objects. Object size should be 0.
        REQUIRE(conn.list_objects(prefix_file_name, objects, false) == 0);
        REQUIRE(objects.size() == 0);

        // No matching objects with listSingle. Object size should be 0.
        REQUIRE(conn.list_objects(prefix_file_name, objects, true) == 0);
        REQUIRE(objects.size() == 0);

        // Put objects to prepare for test.
        for (int i = 0; i < num_objects; i++) {
            auto blob_client =
              azure_client.GetBlockBlobClient(prefix_file_name + std::to_string(i) + ".txt");
            blob_client.UploadFrom(file_name);
        }

        // List all objects. This is the size of num_objects.
        REQUIRE(conn.list_objects(prefix_file_name, objects, false) == 0);
        REQUIRE(objects.size() == num_objects);

        // List single. Object size should be 1.
        objects.clear();
        REQUIRE(conn.list_objects(prefix_file_name, objects, true) == 0);
        REQUIRE(objects.size() == 1);

        // There should be 2 matches with 'list_obj_1' prefix pattern.
        objects.clear();
        REQUIRE(conn.list_objects(prefix_file_name + "1", objects, false) == 0);
        REQUIRE(objects.size() == 2);

        // Clean up.
        for (int i = 0; i < num_objects; i++) {
            auto blob_client =
              azure_client.GetBlockBlobClient(prefix_file_name + std::to_string(i) + ".txt");
            blob_client.Delete();
        }
    }

    SECTION("Testing delete functionality for Azure.", "[azure-connection]")
    {
        auto blob_client = azure_client.GetBlockBlobClient(prefix_file_name);

        blob_client.UploadFrom(file_name);

        // Test that an object can be deleted.
        REQUIRE(conn.delete_object(file_name) == 0);

        // Test that removing an object that doesn't exist returns a ENOENT.
        REQUIRE(conn.delete_object(non_exi_object_key) == ENOENT);
    }

    SECTION("Checking put functionality in Azure.", "[azure-connection]")
    {
        REQUIRE(conn.put_object(file_name, path) == 0);

        auto blob_client = azure_client.GetBlockBlobClient(prefix_file_name);
        blob_client.Delete();

        // Test that putting an object that doesn't exist locally returns -1.
        REQUIRE(conn.put_object(non_exi_object_key, non_exi_file_name) == -1);

        // Test that deleting an object that exists but prefix is wrong returns a ENOENT.
        REQUIRE(conn_bad.delete_object(object_name + std::to_string(0) + ".txt") == ENOENT);
    }

    SECTION("Checking read functionality in Azure.", "[azure-connection]")
    {
        auto blob_client = azure_client.GetBlockBlobClient(prefix_file_name);

        blob_client.UploadFrom(file_name);
        void *buffer = calloc(1024, sizeof(char));

        // Test reading whole file.
        REQUIRE(conn.read_object(file_name, 0, payload.length(), buffer) == 0);
        REQUIRE(static_cast<char *>(buffer) == payload);
        memset(buffer, 0, 1024);

        // Check that read works from the middle to the end.
        REQUIRE(conn.read_object(file_name, payload.find(payload_substr),
                  payload.length() - payload.find(payload_substr), buffer) == 0);
        REQUIRE(static_cast<char *>(buffer) ==
          payload.substr(
            payload.find(payload_substr), payload.length() - payload.find(payload_substr)));
        memset(buffer, 0, 1024);

        // Test overflow on positive offset but past EOF.
        REQUIRE(conn.read_object(file_name, 1, 1000, buffer) == -1);
        memset(buffer, 0, 1024);

        // Test overflow on negative offset but past EOF.
        REQUIRE(conn.read_object(file_name, -1, 1000, buffer) == -1);
        memset(buffer, 0, 1024);

        // Test overflow with negative offset.
        REQUIRE(conn.read_object(file_name, -1, 12, buffer) == -1);
        free(buffer);

        blob_client.Delete();

        // Test that reading a non existent object returns a ENOENT.
        REQUIRE(conn.read_object(non_exi_object_key, 0, 1, buffer) == ENOENT);

        // Tests that reading a non existant object returns a ENOENT.
        REQUIRE(conn.read_object(non_exi_object_key, 0, 1, buffer) == ENOENT);
    }
}
