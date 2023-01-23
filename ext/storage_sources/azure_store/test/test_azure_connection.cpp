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

TEST_CASE("Testing Azure Connection Class", "azure_connection") {
    
    SECTION("Testing prefix functionality works for list, put, and delete.") 
    {
        std::vector<std::string> objects;
        azure_connection conn = azure_connection("myblobcontainer1", "");
        azure_connection pfx_test = azure_connection("myblobcontainer1", "pfx_test_");
        
        // There is nothing in the container so there should be 0 objects
        objects.clear();
        REQUIRE(pfx_test.list_objects("", objects, false) == 0);
        REQUIRE(objects.size() == 0);
        objects.clear();
        REQUIRE(conn.list_objects("", objects, true) == 0); 
        REQUIRE(objects.size() == 0);
        // The container is still empty so doing a list object with a prefix should 
        // result in 0 objects
        objects.clear(); 
        REQUIRE(conn.list_objects("test", objects, false) == 0); 
        REQUIRE(objects.size() == 0);
        
        objects.clear(); 
        REQUIRE(conn.list_objects("test", objects, true) == 0); 
        REQUIRE(objects.size() == 0);
        // Add an object into the empty container resulting in 1 object now
        objects.clear(); 
        REQUIRE(pfx_test.put_object("test.txt", "/home/ubuntu/wiredtiger/ext/storage_sources/azure_store/test.txt") == 0);
        REQUIRE(pfx_test.list_objects("", objects, false) == 0); 
        REQUIRE(objects.size() == 1);
        objects.clear(); 
        REQUIRE(conn.list_objects("", objects, true) == 0); 
        REQUIRE(objects.size() == 1);
        // Calling list objects with a prefix that doesn't exist should result in 0 objects 
        objects.clear(); 
        REQUIRE(pfx_test.list_objects("bad_pfx_", objects, false) == 0); 
        REQUIRE(objects.size() == 0);
        objects.clear(); 
        REQUIRE(pfx_test.list_objects("bad_pfx_", objects, true) == 0); 
        REQUIRE(objects.size() == 0);
        // Calling list objects with the prefix given should result in 1 object
        objects.clear(); 
        REQUIRE(pfx_test.list_objects("pfx_test_", objects, false) == 0); 
        REQUIRE(objects.size() == 1);
        
        objects.clear(); 
        REQUIRE(pfx_test.list_objects("pfx_test_", objects, true) == 0); 
        REQUIRE(objects.size() == 1);
        // Adding another object into the container to test the prefix functionality 
        // and the list single functionality
        objects.clear(); 
        REQUIRE(pfx_test.put_object("test1.txt", "/home/ubuntu/wiredtiger/ext/storage_sources/azure_store/test.txt") == 0);
        REQUIRE(pfx_test.list_objects("pfx_test_", objects, false) == 0); 
        REQUIRE(objects.size() == 2);
        objects.clear();
        REQUIRE(pfx_test.list_objects("pfx_test_", objects, true) == 0); 
        REQUIRE(objects.size() == 1);
        // Creation of another connection class to test prefix functionality works with 
        // multiple prefixes
        azure_connection pfx_check = azure_connection("myblobcontainer1", "pfx_check_");
        // There is are 2 objects in the container so there should be 2 objects
        objects.clear();
        REQUIRE(pfx_check.list_objects("", objects, false) == 0);
        REQUIRE(objects.size() == 2);
        // Now test that there should be no objects with the prefix pfx_check_ in the 
        // container 
        objects.clear();
        REQUIRE(pfx_check.list_objects("pfx_check_", objects, false) == 0);
        REQUIRE(objects.size() == 0);
        // Now let's add an object with the prefix pfx_check_ to the container
        objects.clear();
        REQUIRE(pfx_check.put_object("test.txt", "/home/ubuntu/wiredtiger/ext/storage_sources/azure_store/test.txt") == 0);
        REQUIRE(pfx_check.list_objects("pfx_check_", objects, false) == 0);
        REQUIRE(objects.size() == 1);
        // Testing 2 occurances of pfx_check_ in the same container results in 2 objects 
        objects.clear();
        REQUIRE(pfx_check.put_object("test1.txt", "/home/ubuntu/wiredtiger/ext/storage_sources/azure_store/test.txt") == 0);
        REQUIRE(pfx_check.list_objects("pfx_check_", objects, false) == 0);
        REQUIRE(objects.size() == 2);
        // There should be 4 objects in the container let's check that 
        objects.clear();
        REQUIRE(pfx_check.list_objects("", objects, false) == 0);
        REQUIRE(objects.size() == 4);
        // Test deletion of a pfx_check_ object results in only 1 left and 3 overall 
        objects.clear();
        REQUIRE(pfx_check.delete_object("test.txt") == 0);
        REQUIRE(pfx_check.list_objects("pfx_check_", objects, false) == 0);
        REQUIRE(objects.size() == 1);
        objects.clear();
        REQUIRE(pfx_check.list_objects("", objects, false) == 0);
        REQUIRE(objects.size() == 3);
        // Delete remaining objects in the container
        REQUIRE(pfx_check.delete_object("test1.txt") == 0);
        REQUIRE(pfx_test.delete_object("test.txt") == 0);
        REQUIRE(pfx_test.delete_object("test1.txt") == 0);
        // Check there are no objects in the container 
        objects.clear();
        REQUIRE(pfx_check.list_objects("", objects, false) == 0);
        REQUIRE(objects.size() == 0);
        // Check that deletion of an object that doesn't exist results in a -1 
        REQUIRE(pfx_check.delete_object("test.txt") == -1);
        REQUIRE(pfx_test.delete_object("test.txt") == -1);
    }
}
