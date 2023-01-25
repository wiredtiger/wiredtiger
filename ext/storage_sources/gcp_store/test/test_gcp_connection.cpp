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

namespace test_defaults {
static std::string bucket_name("quickstart_test");

/*
 * Objects with the prefex pattern "gcptest/*" are deleted after a certain period of time according
 * to the lifecycle rule on the gcp bucket. Should you wish to make any changes to the prefix
 * pattern or lifecycle of the object, please speak to the release manager.
 */
static std::string obj_prefix("gcptest/unit/"); // To be concatenated with a random string.
} // namespace test_defaults

// Concatenates a random suffix to the prefix being used for the test object keys. Example of
// generated test prefix: "gcptest/unit/2022-31-01-16-34-10/623843294--".
static int
randomize_test_prefix()
{
    char time_str[100];
    std::time_t t = std::time(nullptr);

    REQUIRE(std::strftime(time_str, sizeof(time_str), "%F-%H-%M-%S", std::localtime(&t)) != 0);

    test_defaults::obj_prefix += time_str;

    // Create a random device and use it to generate a random seed to initialize the generator.
    std::random_device my_random_device;
    unsigned seed = my_random_device();
    std::default_random_engine my_random_engine(seed);

    test_defaults::obj_prefix += '/' + std::to_string(my_random_engine());
    test_defaults::obj_prefix += "--";

    return (0);
}

// Overrides the defaults with the ones specific for this test instance.
static int
setup_test_defaults()
{
    // Append the prefix to be used for object names by a unique string.
    REQUIRE(randomize_test_prefix() == 0);
    std::cerr << "Generated prefix: " << test_defaults::obj_prefix << std::endl;

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
    const std::string path = "./" + file_name;
    const std::string non_exi_object_key = "test_non_exist";
    const std::string non_exi_file_name = "test_non_exist.txt";
    const bool list_single = true;
    std::vector<std::string> objects;

    std::ofstream File(file_name);
    std::string payload = "Test payload";
    File << payload;
    File.close();

    SECTION("Simple check list on nothing", "[gcp-connection]")
    {
        // No matching objects. Object size should be 0.
        REQUIRE(conn.list_objects(objects, false) == 0);
        REQUIRE(objects.size() == 0);

        // No matching objects with list_single. Object size should be 0.
        REQUIRE(conn.list_objects(objects, list_single) == 0);
        REQUIRE(objects.size() == 0);
    }

    SECTION("Simple put, list, delete test case", "[gcp-connection]")
    {
        std::vector<std::string> objects;

        // No matching objects. Object size should be 0.
        REQUIRE(conn.list_objects(objects, false) == 0);
        REQUIRE(objects.size() == 0);

        REQUIRE(conn.put_object(object_key, file_name) == 0);
        REQUIRE(conn.list_objects(objects, false) == 0);
        REQUIRE(objects.size() == 1);
        objects.clear();

        REQUIRE(conn.delete_object(object_key) == 0);
        REQUIRE(conn.list_objects(objects, false) == 0);
        REQUIRE(objects.size() == 0);
    }

    SECTION("Simple check that put object returns -1 when file doesn't exist locally.", "[gcp-connection]")
    {
        // Upload a file that does not exist locally - should fail.
        REQUIRE(conn.put_object(non_exi_object_key, non_exi_file_name) == -1);
    }

    SECTION("Simple check that delete object returns -1 when doesn't exist.", "[gcp-connection]")
    {
        // Delete a file that does not exist in the bucket - should fail.
        REQUIRE(conn.delete_object(object_key) == -1);
    }

    SECTION("Simple check that list single works.", "[gcp-connection]")
    {
        // Total objects to insert in the test.
        const int32_t total_objects = 20;
        // Prefix for objects in this test.
        const std::string prefix = "test_list_objects_";
        const bool list_single = true;

        // No matching objects. Object size should be 0.
        REQUIRE(conn.list_objects(objects, false) == 0);
        REQUIRE(objects.size() == 0);

        // No matching objects with list_single. Object size should be 0.
        REQUIRE(conn.list_objects(objects, list_single) == 0);
        REQUIRE(objects.size() == 0);

        // Create file to prepare for test.
        REQUIRE(std::ofstream(file_name).put('.').good());

        // Put objects to prepare for test.
        for (int i = 0; i < total_objects; i++) {
            REQUIRE(conn.put_object(
                      test_defaults::obj_prefix + std::to_string(i) + ".txt", file_name) == 0);
        }

        // List all objects. This is the size of total_objects.
        REQUIRE(conn.list_objects(objects, false) == 0);
        REQUIRE(objects.size() == total_objects);

        // List single. Object size should be 1.
        objects.clear();
        REQUIRE(conn.list_objects(objects, list_single) == 0);
        REQUIRE(objects.size() == 1);
    }

    SECTION("Complex test: put, list, delete", "[gcp-connection]")
    {
        std::vector<std::string> objects;

        // Total objects to insert in the test.
        const int32_t total_objects = 30;
        const int32_t first_batch = 5;
        // Prefix for objects in this test.
        const std::string prefix = "test_list_objects_";
        const bool list_single = true;

        // No matching objects. Object size should be 0.
        REQUIRE(conn.list_objects(objects, false) == 0);
        REQUIRE(objects.size() == 0);

        // Create file to prepare for test.
        REQUIRE(std::ofstream(file_name).put('.').good());

        // Put objects to prepare for test.
        for (int i = 0; i < total_objects; i++) {
            REQUIRE(conn.put_object(
                      test_defaults::obj_prefix + std::to_string(i) + ".txt", file_name) == 0);
        }

        // List all objects. This is the size of total_objects.
        REQUIRE(conn.list_objects(objects, false) == 0);
        REQUIRE(objects.size() == total_objects);

        // List single. Object size should be 1.
        objects.clear();
        REQUIRE(conn.list_objects(objects, list_single) == 0);
        REQUIRE(objects.size() == 1);

        // Delete 5 files from the bucket. 
        for (int i = 0; i < first_batch; i++) {
            REQUIRE(conn.delete_object(test_defaults::obj_prefix + std::to_string(i) + ".txt") == 0);
        }

        // List all objects, this should be total objects - first batch.
        objects.clear();
        REQUIRE(conn.list_objects(objects, false) == 0);
        REQUIRE(objects.size() == total_objects - first_batch);

        // Delete all files from the bucket
        for (int i = first_batch; i < total_objects; i++) {
            REQUIRE(conn.delete_object(test_defaults::obj_prefix + std::to_string(i) + ".txt") == 0);
        }

        // List all objects, this should be empty.
        objects.clear();
        REQUIRE(conn.list_objects(objects, list_single) == 0);
        REQUIRE(objects.size() == 0);
    }
}
