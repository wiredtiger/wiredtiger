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

TEST_CASE("testing class azure_connection", "azure_connection")
{

    std::vector<std::string> objects;
    azure_connection pfx_test = azure_connection("myblobcontainer1", "pfx_test_");

    // There is nothing in the container so there should be 0 objects
    objects.clear();
    REQUIRE(pfx_test.list_objects("", objects, false) == 0);
    REQUIRE(objects.size() == 1);

    objects.clear();
    REQUIRE(pfx_test.put_object(
              "test.txt", "/home/ubuntu/wiredtiger/ext/storage_sources/azure_store/test.txt") == 0);
    REQUIRE(pfx_test.list_objects("", objects, false) == 0);
    REQUIRE(objects.size() == 1);

    void *buffer = calloc(1024, sizeof(char));
    REQUIRE(pfx_test.read_object("test.txt", 0, 29, buffer) == 0);
    memset(buffer, 0, 1024);

    REQUIRE(pfx_test.read_object("test.txt", 15, 14, buffer) == 0);
    memset(buffer, 0, 1024);

    // Test overflow on positive offset but past EOF
    REQUIRE(pfx_test.read_object("test.txt", 15, 1000, buffer) == -1);
    memset(buffer, 0, 1024);

    // Test overflow on negative offset but past EOF
    REQUIRE(pfx_test.read_object("test.txt", -1, 1000, buffer) == -1);
    memset(buffer, 0, 1024);

    // Test overflow with negative offset
    REQUIRE(pfx_test.read_object("test.txt", -1, 12, buffer) == -1);
    free(buffer);
}
