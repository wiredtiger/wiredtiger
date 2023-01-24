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
    gcp_connection gcp_connection("quickstart_test","prefixadasd");
    SECTION("Read GCP objects under the test bucket.", "[gcp-connection]") {
        
        std::ofstream myfile;
        myfile.open ("/home/ubuntu/wiredtiger/ext/storage_sources/gcp_store/orangetest.txt");
        myfile << "1 2 3 4 5 6 7 8 9 10 \n";
        myfile.close();

        int len = 30;
        void* buf = calloc(len, sizeof(char));
        gcp_connection.put_object("orangetest.txt", "/home/ubuntu/wiredtiger/ext/storage_sources/gcp_store/orangetest.txt");
        gcp_connection.read_object("orangetest.txt", 0, len, buf);

        // std::string line;
        // std::ifstream file ("/home/ubuntu/wiredtiger/ext/storage_sources/gcp_store/orangetest.txt");
        // std::string lines;

        // if (file.is_open())
        // {
        //     while ( getline (file,line) )
        //     {
        //     std::cout << line << '\n';
        //     lines += line;
        //     }
        //     // file.close();
        // }
        // else std::cout << "Unable to open file"; 
        // std::cout << "The contents of lines is: " << lines << "\n";

        std::ifstream file2 ("/home/ubuntu/wiredtiger/ext/storage_sources/gcp_store/orangetest.txt");
        std::string file_str(std::istreambuf_iterator<char>{file2}, std::istreambuf_iterator<char>());
        std::cout << "The contents of file_str is: " << file_str << "\n";

        std::cout << "outside- buf is: " << buf << "\n";
        // std::cout << "contents buf is: " << *(static_cast<char*>(buf)) << "\n";

        char *contents_ptr = (char *)buf;
        std::cout << "The contents of contents is: " << contents_ptr << "\n";
        REQUIRE(contents_ptr == file_str);
        free(buf);
    }
}
