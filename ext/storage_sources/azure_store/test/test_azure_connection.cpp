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

// TODO:
// ADD PREFIXING
// FIX UP CREATION TO REMOVE REDUNDANCY

static std::string
create_file(const std::string &object_name, const std::string &payload)
{
    std::ofstream file(object_name + ".txt");
    file << payload;
    file.close();
    return object_name + ".txt";
}

static std::string obj_prefix("azuretest/unit/"); // To be concatenated with a random string.

// Concatenates a random suffix to the prefix being used for the test object keys. Example of
// generated test prefix: "azuretest/unit/2023-31-01-16-34-10/623843294--".
static int
randomizeTestPrefix()
{
    char time_str[100];
    std::time_t t = std::time(nullptr);

    REQUIRE(std::strftime(time_str, sizeof(time_str), "%F-%H-%M-%S", std::localtime(&t)) != 0);

    obj_prefix += time_str;

    // Create a random device and use it to generate a random seed to initialize the generator.
    std::random_device my_random_device;
    unsigned seed = my_random_device();
    std::default_random_engine my_random_engine(seed);

    obj_prefix += '/' + std::to_string(my_random_engine());
    obj_prefix += "--";

    return 0;
}

TEST_CASE("Testing Azure Connection Class", "azure-connection")
{
    auto azure_client = Azure::Storage::Blobs::BlobContainerClient::CreateFromConnectionString(
      std::getenv("AZURE_STORAGE_CONNECTION_STRING"), "myblobcontainer1");
    bool exists = false;

    REQUIRE(randomizeTestPrefix() == 0);

    azure_connection conn = azure_connection("myblobcontainer1", obj_prefix);
    azure_connection conn_bad = azure_connection("myblobcontainer1", "bad_prefix_");

    // Payloads for blobs uploaded to container.
    const std::string payload = "payload";
    const std::string payload_2 = "Testing offset and substring";

    void *buffer = calloc(1024, sizeof(char));
    std::vector<std::string> objects;
    std::vector<std::pair<std::string, std::string>> blob_objects;

    const std::string object_name = "test_object";
    const std::string object_name_2 = "test_object_2";
    const std::string non_exist_object_key = "test_non_exist";
    blob_objects.push_back(std::make_pair(object_name, payload));
    blob_objects.push_back(std::make_pair(object_name_2, payload_2));

    for (auto pair : blob_objects) {
        auto blob_client = azure_client.GetBlockBlobClient(obj_prefix + pair.first);
        blob_client.UploadFrom(create_file(pair.first, pair.second));
    }

    SECTION("Check Azure connection constructor.", "[azure-connection]")
    {
        REQUIRE_THROWS_WITH(azure_connection("Bad_bucket", ""), "Bad_bucket : No such bucket.");
    }

    SECTION("Check object exists in Azure.", "[azure-connection]")
    {
        // Object exists so there should be 1 object.
        REQUIRE(conn.object_exists(object_name, exists) == 0);
        REQUIRE(exists == true);

        // Object does not exist so there should be 0 objects.
        REQUIRE(conn.object_exists(non_exist_object_key, exists) == 0);
        REQUIRE(exists == false);
    }

    SECTION("List Azure objects under the test bucket.", "[azure-connection]")
    {
        // No matching objects. Object size should be 0.
        objects.clear();
        REQUIRE(conn.list_objects(obj_prefix + non_exist_object_key, objects, false) == 0);
        REQUIRE(objects.size() == 0);

        // No matching objects with listSingle. Object size should be 0.
        REQUIRE(conn.list_objects(obj_prefix + non_exist_object_key, objects, true) == 0);
        REQUIRE(objects.size() == 0);

        // List all objects. This is the size of num_objects.
        REQUIRE(conn.list_objects(obj_prefix + object_name, objects, false) == 0);
        REQUIRE(objects.size() == blob_objects.size());

        // List single. Object size should be 1.
        objects.clear();
        REQUIRE(conn.list_objects(obj_prefix + object_name, objects, true) == 0);
        REQUIRE(objects.size() == 1);
    }

    SECTION("Test delete functionality for Azure.", "[azure-connection]")
    {
        auto blob_client = azure_client.GetBlockBlobClient(obj_prefix + object_name + "1");

        blob_client.UploadFrom(create_file(object_name + "1", payload));

        // Test that an object can be deleted.
        REQUIRE(conn.delete_object(object_name + "1") == 0);

        // Test that removing an object that doesn't exist returns a ENOENT.
        REQUIRE(conn.delete_object(non_exist_object_key) == ENOENT);
    }

    SECTION("Check put functionality in Azure.", "[azure-connection]")
    {
        const std::string path = "./" + create_file(object_name, payload);

        REQUIRE(conn.put_object(object_name + "1", path) == 0);

        auto blob_client = azure_client.GetBlockBlobClient(obj_prefix + object_name + "1");

        // Test that putting an object that doesn't exist locally returns -1.
        REQUIRE(conn.put_object(non_exist_object_key, non_exist_object_key + ".txt") == -1);

        blob_client.Delete();
    }

    SECTION("Check read functionality in Azure.", "[azure-connection]")
    {
        // Test reading whole file.
        REQUIRE(conn.read_object(object_name, 0, payload.length(), buffer) == 0);
        REQUIRE(static_cast<char *>(buffer) == payload);
        memset(buffer, 0, 1024);

        // Check that read works from the first ' ' to the end.
        const int str_len = payload_2.length() - payload_2.find(" ");
        REQUIRE(conn.read_object(object_name_2, payload_2.find(" "), str_len, buffer) == 0);
        REQUIRE(static_cast<char *>(buffer) == payload_2.substr(payload_2.find(" "), str_len));
        memset(buffer, 0, 1024);

        // Test overflow on positive offset but past EOF.
        REQUIRE(conn.read_object(object_name, 1, 1000, buffer) == -1);
        memset(buffer, 0, 1024);

        // Test overflow on negative offset but past EOF.
        REQUIRE(conn.read_object(object_name, -1, 1000, buffer) == -1);
        memset(buffer, 0, 1024);

        // Test overflow with negative offset.
        REQUIRE(conn.read_object(object_name, -1, 12, buffer) == -1);

        // Tests that reading a existing object but wrong prefix returns a ENOENT.
        REQUIRE(conn_bad.read_object(object_name, 0, 1, buffer) == ENOENT);

        // Test that reading a non existent object returns a ENOENT.
        REQUIRE(conn.read_object(non_exist_object_key, 0, 1, buffer) == ENOENT);
    }

    // Clean up.
    for (auto pair : blob_objects) {
        auto blob_client = azure_client.GetBlockBlobClient(obj_prefix + pair.first);
        blob_client.Delete();
    }

    // Sanity check that nothing exists.
    free(buffer);
    objects.clear();
    REQUIRE(conn.list_objects(obj_prefix, objects, false) == 0);
    REQUIRE(objects.size() == 0);
}
