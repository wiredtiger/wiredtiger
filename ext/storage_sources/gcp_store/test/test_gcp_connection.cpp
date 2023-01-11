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

#define CATCH_CONFIG_RUNNER
#include <catch2/catch.hpp>

#include "gcp_connection.h"

// Default config settings for the Test environment.
namespace TestDefaults {
// const Aws::String region = Aws::Region::AP_SOUTHEAST_2;
const double throughputTargetGbps = 5;
const uint64_t partSize = 8 * 1024 * 1024;  // 8 MB.
static std::string bucketName("gcptestext"); // Can be overridden with environment variables.

/*
 * Objects with the prefex pattern "gcptest/*" are deleted after a certain period of time according
 * to the lifecycle rule on the gcp bucket. Should you wish to make any changes to the prefix pattern
 * or lifecycle of the object, please speak to the release manager.
 */
static std::string objPrefix("gcptest/unit/"); // To be concatenated with a random string.
} // namespace TestDefaults

// Concatenates a random suffix to the prefix being used for the test object keys. Example of
// generated test prefix: "gcptest/unit/2023-11-01-16-34-10/623843294--".
static int
randomizeTestPrefix()
{
    return (0);
}

// Overrides the defaults with the ones specific for this test instance.
static int
setupTestDefaults()
{
    return (0);
}

TEST_CASE("Testing class gcpConnection", "gcp-connection")
{
    
}

int
main(int argc, char **argv)
{
    return (0);
}
