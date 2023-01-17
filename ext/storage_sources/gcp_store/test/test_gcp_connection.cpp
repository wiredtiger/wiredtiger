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

TEST_CASE("Testing class gcpConnection", "gcp-connection")
{
    // Initialize the API.
    gcp_connection gcp_connection("quickstart_test");

    const std::string objectName = "test_object";
    const std::string fileName = "test_object.txt";
    const std::string path = "./" + fileName;

    std::ofstream File(fileName);
    std::string payload = "Test payload";
    File << payload;
    File.close();

    SECTION("Lists GCP objects under the test bucket.", "[gcp-connection]")
    {
        std::vector<std::string> objects;

        // Total objects to insert in the test.
        const int32_t totalObjects = 2;
        // Prefix for objects in this test.
        const std::string prefix = "";
        const bool listSingle = true;

        // No matching objects. Object size should be 0.
        REQUIRE(gcp_connection.list_objects(prefix, objects) == 0);
        REQUIRE(objects.size() == 2);

        // No matching objects with listSingle. Object size should be 0.
        objects.clear();
        REQUIRE(gcp_connection.list_objects(prefix, objects, listSingle) == 0);
        REQUIRE(objects.size() == 1);

        // Create file to prepare for test.

        // Put objects to prepare for test.

        // List all objects. This is the size of totalObjects.
        objects.clear();
        REQUIRE(gcp_connection.list_objects(prefix, objects) == 0);
        REQUIRE(objects.size() == totalObjects);

        // List single. Object size should be 1.
        objects.clear();
        REQUIRE(gcp_connection.list_objects(prefix, objects, listSingle) == 0);
        REQUIRE(objects.size() == 1);

        // List with 10 objects per AWS request. Object size should be size of totalObjects.
        // objects.clear();
        // batchSize = 10;
        objects.clear();
        REQUIRE(gcp_connection.list_objects("q", objects) == 0);
        REQUIRE(objects.size() == 1);

        objects.clear();
        REQUIRE(gcp_connection.list_objects("cl", objects) == 0);
        REQUIRE(objects.size() == 0);

        // ListSingle with 10 objects per AWS request. Object size should be 1.
        // objects.clear();
        // REQUIRE(conn.ListObjects(prefix, objects, batchSize, listSingle) == 0);
        // REQUIRE(objects.size() == 1);
    }
}
