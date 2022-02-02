#include <s3_connection.h>

#include <fstream>

/* Default config settings for the S3CrtClient. */
namespace TestDefaults {
const Aws::String region = Aws::Region::AP_SOUTHEAST_2;
const double throughputTargetGbps = 5;
const uint64_t partSize = 8 * 1024 * 1024; /* 8 MB. */
} // namespace TestDefaults

int TestListBuckets(const Aws::S3Crt::ClientConfiguration &config);
int TestListObjects(const Aws::S3Crt::ClientConfiguration &config);

int CleanupTestListObjects(
  const Aws::S3Crt::ClientConfiguration &config, std::string bucketName, int totalObjects);

/* Wrapper for unit test functions. */
#define TEST(func, config, expectedOutput)              \
    do {                                                \
        int __ret;                                      \
        if ((__ret = (func(config))) != expectedOutput) \
            return (__ret);                             \
    } while (0)

/*
 * TestListBuckets --
 *     Example of a unit test to list S3 buckets under the associated AWS account.
 */
int
TestListBuckets(const Aws::S3Crt::ClientConfiguration &config)
{
    int ret = 0;
    S3Connection conn(config);
    std::vector<std::string> buckets;
    if (ret = conn.ListBuckets(buckets) != 0)
        return ret;

    std::cout << "All buckets under my account:" << std::endl;
    for (const auto &bucket : buckets)
        std::cout << "  * " << bucket << std::endl;

    return ret;
}

/*
 * TestListObjects --
 *     Unit test for listing S3 objects under the first bucket in the associated AWS account. This
 *     test assumes there are initially no objects with the prefix of "test_list_objects_" in the
 *     bucket.
 */
int
TestListObjects(const Aws::S3Crt::ClientConfiguration &config)
{
    S3Connection conn(config);

    /* Temporary workaround to get a bucket to use. */
    std::vector<std::string> buckets;
    int ret;
    if (ret = conn.ListBuckets(buckets) != 0)
        return (0);
    if (buckets.empty()) {
        std::cout << "No buckets found in AWS account." << std::endl;
        return (1);
    }

    std::string firstBucket = buckets.at(0);
    std::vector<std::string> objects;

    /* Total objects to insert in the test. */
    const int32_t totalObjects = 20;
    /* Parameter for getting single object. */
    const bool listSingle = true;
    /* Number of objects to access per iteration of AWS. */
    int32_t batchSize = 1;
    /* Expected number of matches. */
    int32_t expectedResult = 0;

    /* No matching objects. */
    if (ret = conn.ListObjects(firstBucket, "test_list_objects_", objects) != 0)
        return (ret);
    if (objects.size() != expectedResult) {
        return (1);
    }

    /* No matching objects with listSingle. */
    if (ret =
          conn.ListObjects(firstBucket, "test_list_objects_", objects, batchSize, listSingle) != 0)
        return (ret);
    if (objects.size() != expectedResult) {
        return (1);
    }

    /* Create file to prepare for test. */
    if (!static_cast<bool>(std::ofstream("test_list_objects.txt").put('.'))) {
        std::cerr << "Error creating file." << std::endl;
        return (1);
    }

    /* Put objects to prepare for test. */
    for (int i = 0; i < totalObjects; i++) {
        if (ret = conn.PutObject(firstBucket, "test_list_objects_" + std::to_string(i) + ".txt",
                    "test_list_objects.txt") != 0)
            return (ret);
    }

    /* List all objects. */
    expectedResult = totalObjects;
    if (ret = conn.ListObjects(firstBucket, "test_list_objects_", objects) != 0)
        return (ret);
    if (objects.size() != expectedResult) {
        CleanupTestListObjects(config, firstBucket, totalObjects);
        return (1);
    }

    /* List single. */
    objects.clear();
    expectedResult = 1;
    if (ret =
          conn.ListObjects(firstBucket, "test_list_objects_", objects, batchSize, listSingle) != 0)
        return (ret);
    if (objects.size() != expectedResult) {
        CleanupTestListObjects(config, firstBucket, totalObjects);
        return (1);
    }

    /* Expected number of matches with test_list_objects_1 prefix. */
    objects.clear();
    expectedResult = 11;
    if (ret = conn.ListObjects(firstBucket, "test_list_objects_1", objects) != 0)
        return (ret);
    if (objects.size() != expectedResult) {
        CleanupTestListObjects(config, firstBucket, totalObjects);
        return (1);
    }

    /* List with 5 objects per AWS request. */
    objects.clear();
    batchSize = 5;
    expectedResult = totalObjects;
    if (ret = conn.ListObjects(firstBucket, "test_list_objects_", objects, batchSize) != 0)
        return (ret);
    if (objects.size() != expectedResult) {
        CleanupTestListObjects(config, firstBucket, totalObjects);
        return (1);
    }

    /* ListSingle with 8 objects per AWS request. */
    objects.clear();
    expectedResult = 1;
    if (ret =
          conn.ListObjects(firstBucket, "test_list_objects_", objects, batchSize, listSingle) != 0)
        return (ret);
    if (objects.size() != expectedResult) {
        CleanupTestListObjects(config, firstBucket, totalObjects);
        return (1);
    }

    /* List with 8 objects per AWS request. */
    objects.clear();
    batchSize = 8;
    expectedResult = totalObjects;
    if (ret = conn.ListObjects(firstBucket, "test_list_objects_", objects, batchSize) != 0)
        return (ret);
    if (objects.size() != expectedResult) {
        CleanupTestListObjects(config, firstBucket, totalObjects);
        return (1);
    }

    /* ListSingle with 8 objects per AWS request. */
    objects.clear();
    expectedResult = 1;
    if (ret =
          conn.ListObjects(firstBucket, "test_list_objects_", objects, batchSize, listSingle) != 0)
        return (ret);
    if (objects.size() != expectedResult) {
        CleanupTestListObjects(config, firstBucket, totalObjects);
        return (1);
    }

    CleanupTestListObjects(config, firstBucket, totalObjects);
    return (0);
}

int
CleanupTestListObjects(
  const Aws::S3Crt::ClientConfiguration &config, std::string bucketName, int totalObjects)
{
    /* Delete objects and file at end of test. */
    S3Connection conn(config);
    int ret = 0;
    for (int i = 0; i < totalObjects; i++) {
        if (ret =
              conn.DeleteObject(bucketName, "test_list_objects_" + std::to_string(i) + ".txt") != 0)
            std::cerr << "TestListObjects cleanup failed." << std::endl;
    }
    std::remove("test_list_objects.txt");

    return ret;
}

/*
 * main --
 *     Set up configs and call unit tests.
 */
int
main()
{
    /* Set up the config to use the defaults specified. */
    Aws::S3Crt::ClientConfiguration awsConfig;
    awsConfig.region = TestDefaults::region;
    awsConfig.throughputTargetGbps = TestDefaults::throughputTargetGbps;
    awsConfig.partSize = TestDefaults::partSize;

    /* Set the SDK options and initialize the API. */
    Aws::SDKOptions options;
    Aws::InitAPI(options);

    int expectedOutput = 0;
    TEST(TestListBuckets, awsConfig, expectedOutput);
    TEST(TestListObjects, awsConfig, expectedOutput);

    /* Shutdown the API at end of tests. */
    Aws::ShutdownAPI(options);
    return 0;
}
