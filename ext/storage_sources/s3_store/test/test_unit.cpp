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

    REQUIRE(std::strftime(timeStr, sizeof(timeStr), "%F-%H-%M-%S", std::localtime(&t)) != 0);

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
    REQUIRE(randomizeTestPrefix() == 0);
    std::cerr << "Generated prefix: " << TestDefaults::objPrefix << std::endl;

    return (0);
}

static int
CleanupTestListObjects(S3Connection &conn, const int totalObjects, const std::string &prefix,
  const std::string &fileName)
{
    // Delete objects and file at end of test.
    for (int i = 0; i < totalObjects; i++) {
        REQUIRE(conn.DeleteObject(prefix + std::to_string(i) + ".txt") == 0);
    }
    std::remove(fileName.c_str());

    return (0);
}


TEST_CASE("Testing S3 Connection", "something"){

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
    const std::string path = "./" + fileName;

    std::ofstream File(fileName);
    std::string payload = "Test payload";
    File << payload;
    File.close();

    SECTION( "Check if object exists in S3, and if object size is correct)", "[single-file]" ) {
        CHECK((conn.ObjectExists(objectName, exists, objectSize)) == 0);
        CHECK(objectSize == 0);
        CHECK((conn.PutObject(objectName, fileName)) == 0);
        CHECK((conn.ObjectExists(objectName, exists, objectSize)) == 0);
        CHECK(exists);
        CHECK(objectSize == payload.length());
        CHECK(conn.DeleteObject(objectName) == 0);
    }

    SECTION( "Gets an object from an S3 Bucket", "[single-file]" ) {
        REQUIRE(conn.PutObject(objectName, fileName) == 0);
        REQUIRE(std::remove(path.c_str()) == 0); // Delete the local copy of the file.
        REQUIRE(conn.GetObject(objectName, path) == 0); // Download the file from S3
        
        // The file should now be in the current directory.
        std::ifstream f(path);
        CHECK(f.good());

        // Clean up test artifacts.
        CHECK(std::remove(path.c_str()) == 0);
        CHECK(conn.DeleteObject(objectName) == 0);
    }

    SECTION( "Lists S3 objects under the test bucket.", "[single-file]" ) {
        std::vector<std::string> objects;
        
        // Total objects to insert in the test.
        const int32_t totalObjects = 20;
        // Prefix for objects in this test.
        const std::string prefix = "test_list_objects_";
        // Parameter for getting single object.
        const bool listSingle = true;
        // Number of objects to access per iteration of AWS.
        int32_t batchSize = 1;
        // Expected number of matches.
        int32_t expectedResult = 0;

        // No matching objects.
        CHECK(conn.ListObjects(prefix, objects) == 0);
        CHECK(objects.size() == expectedResult);

        // No matching objects with listSingle.
        CHECK(conn.ListObjects(prefix, objects, batchSize, listSingle) == 0);
        CHECK(objects.size() == expectedResult);

        // Create file to prepare for test.
        REQUIRE(static_cast<bool>(std::ofstream(fileName).put('.')));

        // Put objects to prepare for test.
        // ASK ABOUT HOW TO TURN THIS INTO CHECK / REQUIRE if we need to delete when fails. 
        for (int i = 0; i < totalObjects; i++) 
            REQUIRE(conn.PutObject(prefix + std::to_string(i) + ".txt", fileName) == 0);
            // else --> CleanupTestListObjects(conn, totalObjects, prefix, fileName);

        // List all objects.
        expectedResult = totalObjects;
        CHECK(conn.ListObjects(prefix, objects) == 0);
        // else --> CleanupTestListObjects(conn, totalObjects, prefix, fileName);
        REQUIRE(objects.size() == expectedResult);
        // CleanupTestListObjects(conn, totalObjects, prefix, fileName);
  
        // List single.
        objects.clear();
        expectedResult = 1;
        CHECK(conn.ListObjects(prefix, objects, batchSize, listSingle) == 0);
            // CleanupTestListObjects(conn, totalObjects, prefix, fileName);
        REQUIRE (objects.size() == expectedResult);
            // CleanupTestListObjects(conn, totalObjects, prefix, fileName);
    

    

    }

}

int
main(int argc, char **argv)
{   
    return Catch::Session().run(argc, argv);
}