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

    Azure::Storage::Blobs::BlobContainerClient azure_client =
      Azure::Storage::Blobs::BlobContainerClient::CreateFromConnectionString(
        std::getenv("AZURE_STORAGE_CONNECTION_STRING"), "myblobcontainer1");
    bool exists = false;

    const std::string object_name = "test_object";
    const std::string file_name = "test_object.txt";
    const std::string path = "./" + file_name;
    const std::string non_exi_object_key = "test_non_exist";
    const std::string non_exi_file_name = "test_non_exist.txt";

    std::ofstream File(file_name);
    std::string payload = "Test payload";
    File << payload;
    File.close();

    SECTION(
      "(Checks if connection to a non-existing bucket fails gracefully).", "[azure-connection]")
    {
        REQUIRE_THROWS_WITH(azure_connection("Bad_bucket", ""), "Bad_bucket : No such bucket.");
    }

    SECTION("(Check if an object exists in Azure)", "[azure-connection]")
    {
        azure_connection conn = azure_connection("myblobcontainer1", "obj_exists_");

        // Object does not exist yet so there should be 0 objects.
        CHECK(conn.object_exists(file_name, exists) == 0);
        CHECK(exists == false);

        auto blob_client = azure_client.GetBlockBlobClient("obj_exists_" + file_name);
        blob_client.UploadFrom(file_name);

        // Object exists now in the container.
        CHECK(conn.object_exists(file_name, exists) == 0);
        CHECK(exists == true);

        blob_client.Delete();
    }

    SECTION("(Lists Azure objects under the text bucket).", "[azure-connection]")
    {
        azure_connection conn = azure_connection("myblobcontainer1", "list_obj_");
        std::vector<std::string> objects;

        // Total objects to insert in the test.
        const int32_t num_objects = 11;
        // Prefix for objects in this test.
        const std::string prefix = "list_obj_";
        const bool list_single = true;

        // No matching objects. Object size should be 0.
        CHECK(conn.list_objects(prefix + file_name, objects, !list_single) == 0);
        CHECK(objects.size() == 0);

        // No matching objects with listSingle. Object size should be 0.
        CHECK(conn.list_objects(prefix + file_name, objects, list_single) == 0);
        CHECK(objects.size() == 0);

        // Put objects to prepare for test.
        for (int i = 0; i < num_objects; i++) {
            auto blob_client =
              azure_client.GetBlockBlobClient(prefix + file_name + std::to_string(i) + ".txt");
            blob_client.UploadFrom(file_name);
        }

        // List all objects. This is the size of num_objects.
        CHECK(conn.list_objects(prefix + file_name, objects, !list_single) == 0);
        CHECK(objects.size() == num_objects);

        // List single. Object size should be 1.
        objects.clear();
        CHECK(conn.list_objects(prefix + file_name, objects, list_single) == 0);
        CHECK(objects.size() == 1);

        // There should be 2 matches with 'list_obj_1' prefix pattern.
        objects.clear();
        CHECK(conn.list_objects(prefix + file_name + "1", objects, !list_single) == 0);
        CHECK(objects.size() == 2);

        // Clean up.
        for (int i = 0; i < num_objects; i++) {
            auto blob_client =
              azure_client.GetBlockBlobClient(prefix + file_name + std::to_string(i) + ".txt");
            blob_client.Delete();
        }
    }

    SECTION("(Testing delete functionality for Azure).", "[azure-connection]")
    {
        azure_connection conn = azure_connection("myblobcontainer1", "delete_obj_");

        auto blob_client = azure_client.GetBlockBlobClient("delete_obj_" + file_name);

        blob_client.UploadFrom(file_name);

        // Test that an object can be deleted.
        CHECK(conn.delete_object(file_name) == 0);

        // Test that removing an object that doesn't exist throws an exception.
        CHECK(conn.delete_object(non_exi_object_key) == ENOENT);
    }

    SECTION("(Checking put functionality in Azure)", "[azure-connection]")
    {
        azure_connection conn = azure_connection("myblobcontainer1", "put_obj_");

        CHECK(conn.put_object(file_name, path) == 0);

        auto blob_client = azure_client.GetBlockBlobClient("put_obj_" + file_name);
        blob_client.Delete();

        // Test that putting an object that doesn't exist locally throws an exception.
        CHECK(conn.put_object(non_exi_object_key, non_exi_file_name) == -1);
    }

    SECTION("(Checking read functionality in Azure)", "[azure-connection]")
    {
        azure_connection conn = azure_connection("myblobcontainer1", "read_obj_");

        auto blob_client = azure_client.GetBlockBlobClient("read_obj_" + file_name);

        blob_client.UploadFrom(file_name);
        void *buffer = calloc(1024, sizeof(char));

        // Test reading whole file.
        CHECK(conn.read_object(file_name, 0, payload.length(), buffer) == 0);
        CHECK(static_cast<char *>(buffer) == payload);
        memset(buffer, 0, 1024);

        // Test overflow on positive offset but past EOF.
        CHECK(conn.read_object(file_name, 1, 1000, buffer) == -1);
        memset(buffer, 0, 1024);

        // Test overflow on negative offset but past EOF.
        CHECK(conn.read_object(file_name, -1, 1000, buffer) == -1);
        memset(buffer, 0, 1024);

        // Test overflow with negative offset.
        CHECK(conn.read_object(file_name, -1, 12, buffer) == -1);
        free(buffer);

        blob_client.Delete();

        // Tests that reading a non existant object returns an exception.
        CHECK(conn.read_object(non_exi_object_key, 0, 1, buffer) == ENOENT);
    }

    SECTION("(Test complex case).", "[azure-connection]")
    {
        azure_connection conn = azure_connection("myblobcontainer1", "complex_case_");

        SECTION("(Testing object exists in complex case).", "[azure-connection]")
        {
            // Check that the object does not exist in the container.
            CHECK(conn.object_exists(file_name, exists) == 0);
            CHECK(exists == false);

            // Put an object into the container.
            CHECK(conn.put_object(file_name, path) == 0);

            // Check that the object put in exists in the container.
            CHECK(conn.object_exists(file_name, exists) == 0);
            CHECK(exists == true);

            // Delete the object from the container.
            CHECK(conn.delete_object(file_name) == 0);

            // Check that the object no longer exists in the container.
            CHECK(conn.object_exists(file_name, exists) == 0);
            CHECK(exists == false);
        }

        SECTION("(Testing list objects in complex case).", "[azure-connection]")
        {
            std::vector<std::string> objects;

            // Total objects to insert in the test.
            const int32_t num_objects = 11;
            // Prefix for objects in this test.
            const std::string prefix = "complex_case_";
            const bool list_single = true;

            // No matching objects. Object size should be 0.
            CHECK(conn.list_objects(object_name, objects, !list_single) == 0);
            CHECK(objects.size() == 0);

            // Put objects to prepare for test.
            for (int i = 0; i < num_objects; i++) {
                CHECK(conn.put_object(object_name + std::to_string(i) + ".txt", path) == 0);
            }

            // There should be 2 matches with 'complex_case_test_object_1' prefix pattern.
            objects.clear();
            CHECK(conn.list_objects(prefix + object_name + '1', objects, !list_single) == 0);
            CHECK(objects.size() == 2);

            // Delete complex_case_10.txt from the container.
            CHECK(conn.delete_object(object_name + std::to_string(10) + ".txt") == 0);

            // There should be 1 match with 'complex_case_test_object_1' prefix pattern.
            objects.clear();
            CHECK(conn.list_objects(prefix + object_name + '1', objects, !list_single) == 0);
            CHECK(objects.size() == 1);

            // Clean up.
            for (int i = 0; i < num_objects - 1; i++) {
                CHECK(conn.delete_object(object_name + std::to_string(i) + ".txt") == 0);
            }
        }

        SECTION("(Testing delete objects in complex case).", "[azure-connection]")
        {
            // Put an object into the container.
            CHECK(conn.put_object(file_name, path) == 0);

            // Check that the object exists in the container.
            CHECK(conn.object_exists(file_name, exists) == 0);
            CHECK(exists == true);

            // Test the object can be deleted.
            CHECK(conn.delete_object(file_name) == 0);

            // Check that the object no longer exists in the container.
            CHECK(conn.object_exists(file_name, exists) == 0);
            CHECK(exists == false);

            // Test that removing an object that has already been deleted throws an exception.
            CHECK(conn.delete_object(file_name) == ENOENT);
        }

        SECTION("(Testing put objects in complex case).", "[azure-connection]")
        {
            // Put an object into the container.
            CHECK(conn.put_object(file_name, path) == 0);

            // Check that the object exists in the container.
            CHECK(conn.object_exists(file_name, exists) == 0);
            CHECK(exists == true);

            // Test that an object can be deleted.
            CHECK(conn.delete_object(file_name) == 0);

            // Check that the object does not exist in the container.
            CHECK(conn.object_exists(file_name, exists) == 0);
            CHECK(exists == false);
        }

        SECTION("(Testing read objects in complex case).", "[azure-connection]")
        {
            // Put an object into the container.
            CHECK(conn.put_object(file_name, path) == 0);

            // Check that the object exists in the container.
            CHECK(conn.object_exists(file_name, exists) == 0);
            CHECK(exists == true);

            void *buffer = calloc(1024, sizeof(char));
            // Test reading whole file.
            CHECK(conn.read_object(file_name, 0, payload.length(), buffer) == 0);
            CHECK(static_cast<char *>(buffer) == payload);
            memset(buffer, 0, 1024);

            // Test that an object can be deleted.
            CHECK(conn.delete_object(file_name) == 0);

            std::ofstream File2(file_name);
            std::string payload = "Testing offset and substring";
            std::string payload_substr = "offset";
            File2 << payload;
            File2.close();

            // Put an object into the container.
            CHECK(conn.put_object(file_name, path) == 0);

            // Check that the object exists in the container.
            CHECK(conn.object_exists(file_name, exists) == 0);
            CHECK(exists == true);

            // Check that the offset and length works by finding a substring in the payload.
            CHECK(conn.read_object(
                    file_name, payload.find(payload_substr), payload_substr.length(), buffer) == 0);
            CHECK(static_cast<char *>(buffer) == payload_substr);
            memset(buffer, 0, 1024);

            CHECK(conn.delete_object(file_name) == 0);
            free(buffer);

            // Check that the object does not exist in the container.
            CHECK(conn.object_exists(file_name, exists) == 0);
            CHECK(exists == false);
        }
    }
}
