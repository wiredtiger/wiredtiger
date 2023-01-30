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

TEST_CASE("Testing Azure Connection Class", "azure-connection") {

    Azure::Storage::Blobs::BlobContainerClient azure_client = Azure::Storage::Blobs::BlobContainerClient::CreateFromConnectionString(std::getenv("AZURE_STORAGE_CONNECTION_STRING"), "myblobcontainer1");
    bool exists = false; 

    const std::string object_name = "test_object";
    const std::string file_name = "test_object.txt";
    const std::string path = "./" + file_name;

    std::ofstream File(file_name);
    std::string payload = "Test payload";
    File << payload;
    File.close();

    SECTION("(Checks if connection to a non-existing bucket fails gracefully).", "[azure-connection]") { 
        REQUIRE_THROWS_WITH(azure_connection("Bad_bucket", ""), "Bad_bucket : No such bucket.");
    }

    SECTION("(Check if an object exists in Azure)", "[azure-connection]") { 
        azure_connection conn = azure_connection("myblobcontainer1", "obj_exists_");
        
        REQUIRE(conn.object_exists(file_name, exists) == 0);
        REQUIRE(exists == false);

        auto blob_client = azure_client.GetBlockBlobClient("obj_exists_" + file_name);

        // Create file to prepare for test.
        REQUIRE(std::ofstream(file_name).put('.').good());

        blob_client.UploadFrom(file_name);

        REQUIRE(conn.object_exists(file_name, exists) == 0);
        REQUIRE(exists == true);

        blob_client.Delete();
    }

    SECTION("(Lists Azure objects under the text bucket).", "[azure-connection]") { 
        azure_connection conn = azure_connection("myblobcontainer1", "list_obj_");
        std::vector<std::string> objects;

        // Total objects to insert in the test.
        const int32_t num_objects = 11; 
        // Prefix for objects in this test.
        const std::string prefix = "list_obj_";
        const bool list_single = true; 

        // No matching objects. Object size should be 0.
        REQUIRE(conn.list_objects(prefix, objects, !list_single) == 0); 
        REQUIRE(objects.size() == 0);

        // No matching objects with listSingle. Object size should be 0.
        REQUIRE(conn.list_objects(prefix, objects, list_single) == 0); 
        REQUIRE(objects.size() == 0);

        // Create file to prepare for test.
        REQUIRE(std::ofstream(file_name).put('.').good());

        // Put objects to prepare for test.
        for (int i = 0; i < num_objects; i++) {
            auto blob_client = azure_client.GetBlockBlobClient(prefix + std::to_string(i) + ".txt");
            blob_client.UploadFrom(file_name);
        }

        // List all objects. This is the size of num_objects.
        REQUIRE(conn.list_objects(prefix, objects, !list_single) == 0);
        REQUIRE(objects.size() == num_objects);

        // List single. Object size should be 1.
        objects.clear();
        REQUIRE(conn.list_objects(prefix, objects, list_single) == 0);
        REQUIRE(objects.size() == 1);

        // There should be 2 matches with 'list_obj_1' prefix pattern.
        objects.clear();
        REQUIRE(conn.list_objects(prefix + "1", objects, !list_single) == 0);
        REQUIRE(objects.size() == 2);

        // Clean up.
        for (int i = 0; i < num_objects; i++) {
            auto blob_client = azure_client.GetBlockBlobClient(prefix + std::to_string(i) + ".txt");
            blob_client.Delete();
        }
    }

    SECTION("(Testing delete functionality for Azure).", "[azure-connection]") { 
        azure_connection conn = azure_connection("myblobcontainer1", "delete_obj_");
        
        auto blob_client = azure_client.GetBlockBlobClient("delete_obj_" + file_name);

        // Create file to prepare for test.
        REQUIRE(std::ofstream(file_name).put('.').good());

        blob_client.UploadFrom(file_name);

        // Test that an object can be deleted.
        REQUIRE(conn.delete_object(file_name) == 0); 

        // Test that removing an object that doesn't exist throws an exception.
        REQUIRE_THROWS_WITH(conn.delete_object("object_not_exist"), "The specified blob does not exist."); 
    }

    SECTION("(Checking put functionality in Azure)", "[azure-connection]") { 
        azure_connection conn = azure_connection("myblobcontainer1", "put_obj_");

        REQUIRE(conn.put_object(file_name, path) == 0);
        
        auto blob_client = azure_client.GetBlockBlobClient("put_obj_" + file_name);
        blob_client.Delete(); 

        // Test that putting an object that doesn't exist locally throws an exception.
        REQUIRE_THROWS_WITH(conn.put_object("object_not_exist", "bad_file_path"), "Failed to open file for reading. File name: 'bad_file_path'"); 
    }

    SECTION("(Checking read functionality in Azure)", "[azure-connection]") { 
        azure_connection conn = azure_connection("myblobcontainer1", "read_obj_");
        
        auto blob_client = azure_client.GetBlockBlobClient("read_obj_" + file_name);

        // Create file to prepare for test.
        REQUIRE(std::ofstream(file_name).put('.').good());
        blob_client.UploadFrom(file_name);

        void *buffer = calloc(1024, sizeof(char));   

        // Test reading whole file.  
        REQUIRE(conn.read_object("read_obj_" + file_name, 0, payload.length(), buffer) == 0);

        memset(buffer, 0, 1024);

        // Test overflow on positive offset but past EOF.        
        REQUIRE_THROWS_WITH(conn.read_object("read_obj_" + file_name, 1, 1000, buffer), "Reading past end of file!");       
        memset(buffer, 0, 1024);

        // Test overflow on negative offset but past EOF.        
        REQUIRE_THROWS_WITH(conn.read_object("read_obj_" + file_name, -1, 1000, buffer), "Invalid argument: Offset!");       
        memset(buffer, 0, 1024);

        // Test overflow with negative offset.      
        REQUIRE_THROWS_WITH(conn.read_object("read_obj_" + file_name, -1, 12, buffer), "Invalid argument: Offset!");       
        free(buffer);

        blob_client.Delete(); 

        // Tests that reading a non existant object returns an exception. 
        REQUIRE_THROWS_WITH(conn.read_object("read_obj_bad_file_name", 0, 1, buffer), "The specified blob does not exist.");       
    }
}
