/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#define CATCH_CONFIG_RUNNER
#include <catch2/catch.hpp>

#include <s3_connection.h>
#include <fstream>
#include <random>

// Default config settings for the Test environment.
namespace TestDefaults {
const Aws::String region = Aws::Region::AP_SOUTHEAST_2;
const double throughputTargetGbps = 5;
const uint64_t partSize = 8 * 1024 * 1024;  // 8 MB.
static std::string bucketName("s3testext"); // Can be overridden with environment variables.

// Objects with the prefex pattern "s3test/*" are deleted after a certain period of time according
// to the lifecycle rule on the S3 bucket. Should you wish to make any changes to the prefix pattern
// or lifecycle of the object, please speak to the release manager.
static std::string objPrefix("s3test/unit/"); // To be concatenated with a random string.
} // namespace TestDefaults

// Concatenates a random suffix to the prefix being used for the test object keys. Example of
// generated test prefix: "s3test/unit/2022-31-01-16-34-10/623843294--".
static int
randomizeTestPrefix()
{
    char timeStr[100];
    std::time_t t = std::time(nullptr);

    if (std::strftime(timeStr, sizeof(timeStr), "%F-%H-%M-%S", std::localtime(&t)) == 0)
        return (1);

    TestDefaults::objPrefix += timeStr;

    // Create a random device and use it to generate a random seed to initialize the generator.
    std::random_device myRandomDevice;
    unsigned seed = myRandomDevice();
    std::default_random_engine myRandomEngine(seed);

    TestDefaults::objPrefix += '/' + std::to_string(myRandomEngine());
    TestDefaults::objPrefix += "--";

    return (0);
}

// Overrides the defaults with the ones specific for this test instance.
static int
setupTestDefaults()
{
    // Prefer to use the bucket provided through the environment variable.
    const char *envBucket = std::getenv("WT_S3_EXT_BUCKET");
    if (envBucket != nullptr)
        TestDefaults::bucketName = envBucket;
    std::cerr << "Bucket to be used for testing: " << TestDefaults::bucketName << std::endl;

    // Append the prefix to be used for object names by a unique string.
    if (randomizeTestPrefix() != 0)
        return (1);
    std::cerr << "Generated prefix: " << TestDefaults::objPrefix << std::endl;

    return (0);
}

TEST_CASE("something", "something"){

    // Setup the test environment.
    REQUIRE(setupTestDefaults() == 0);
    
    // Set up the config to use the defaults specified.
    Aws::S3Crt::ClientConfiguration awsConfig;
    awsConfig.region = TestDefaults::region;
    awsConfig.throughputTargetGbps = TestDefaults::throughputTargetGbps;
    awsConfig.partSize = TestDefaults::partSize;

    // Set the SDK options and initialize the API.
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    S3Connection conn(awsConfig, TestDefaults::bucketName, TestDefaults::objPrefix);
    bool exists = false;
    size_t objectSize;

    const std::string objectName = "test_object";
    const std::string fileName = "test_object.txt";

    std::ofstream File(fileName);
    std::string payload = "Test payload";
    File << payload;
    File.close();

    SECTION( "Check if object exists in S3, and if object size is correct)", "[single-file]" ) {
        CHECK((conn.ObjectExists(objectName, exists, objectSize)) == 0);
        CHECK(objectSize == 0);
        CHECK((conn.PutObject(objectName, fileName)) == 0);
        CHECK((conn.ObjectExists(objectName, exists, objectSize)) == 0);
    }

    SECTION( "Factorials of 1 and higher are computed (pass)", "[single-file]" ) {
        CHECK(0==0);
    }
}

int
main(int argc, char **argv)
{   
    return Catch::Session().run(argc, argv);
}