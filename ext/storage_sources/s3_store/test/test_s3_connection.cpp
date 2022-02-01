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
    S3Connection conn(config);
    std::vector<std::string> buckets;
    if (!conn.ListBuckets(buckets))
        return 1;

    std::cout << "All buckets under my account:" << std::endl;
    for (const auto &bucket : buckets)
        std::cout << "  * " << bucket << std::endl;
    return 0;
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
    std::string firstBucket;
    std::vector<std::string> buckets;
    if (conn.ListBuckets(buckets)) {
        if (!buckets.empty()) {
            firstBucket = buckets.at(0);

            std::vector<std::string> objects;
            uint32_t countp;

            /* No matching objects. */
            objects = conn.ListObjects(firstBucket, "test_list_objects_", countp);
            if (objects.size() != countp || countp != 0)
                return (1);

            /* Create file to prepare for test. */
            if (!static_cast<bool>(std::ofstream("test_list_objects.txt").put('.'))) {
                std::cerr << "Error creating file." << std::endl;
                return (1);
            }

            /* Put objects to prepare for test. */
            for (int i = 0; i < 20; i++)
                conn.PutObject(firstBucket, "test_list_objects_" + std::to_string(i) + ".txt",
                  "test_list_objects.txt");

            /* List all objects with prefix. */
            objects = conn.ListObjects(firstBucket, "test_list_objects_", countp);
            if (objects.size() != countp || countp != 20) {
                /* Delete objects and file at end of test. */
                for (int i = 0; i < 20; i++)
                    conn.DeleteObject(
                      firstBucket, "test_list_objects_" + std::to_string(i) + ".txt");
                std::remove("test_list_objects.txt");
                return (1);
            }
            std::cout << "Objects with test_list_objects_ prefix:" << std::endl;
            for (const auto &object : objects)
                std::cout << "  * " << object << std::endl;

            objects = conn.ListObjects(firstBucket, "test_list_objects_1", countp);
            if (objects.size() != countp || countp != 11) {
                /* Delete objects and file at end of test. */
                for (int i = 0; i < 20; i++)
                    conn.DeleteObject(
                      firstBucket, "test_list_objects_" + std::to_string(i) + ".txt");
                std::remove("test_list_objects.txt");
                return (1);
            }
            std::cout << "Objects with test_list_objects_1 prefix:" << std::endl;
            for (const auto &object : objects)
                std::cout << "  * " << object << std::endl;

            /* List with max objects of 1. */
            objects = conn.ListObjects(firstBucket, "test_list_objects_", countp, 1, 1);
            if (objects.size() != countp || countp != 1) {
                /* Delete objects and file at end of test. */
                for (int i = 0; i < 20; i++)
                    conn.DeleteObject(
                      firstBucket, "test_list_objects_" + std::to_string(i) + ".txt");
                std::remove("test_list_objects.txt");
                return (1);
            }
            std::cout << "List 1 object:" << std::endl;
            for (const auto &object : objects)
                std::cout << "  * " << object << std::endl;

            /* List with 5 objects per AWS request. */
            const int32_t maxPerIter = 5;
            objects = conn.ListObjects(firstBucket, "test_list_objects_", countp, maxPerIter);
            if (objects.size() != countp || countp != 20) {
                /* Delete objects and file at end of test. */
                for (int i = 0; i < 20; i++)
                    conn.DeleteObject(
                      firstBucket, "test_list_objects_" + std::to_string(i) + ".txt");
                std::remove("test_list_objects.txt");
                return (1);
            }
            std::cout << "List all:" << std::endl;
            for (const auto &object : objects)
                std::cout << "  * " << object << std::endl;

            /* List with total_objects < max_per_iter. */
            objects =
              conn.ListObjects(firstBucket, "test_list_objects_", countp, maxPerIter, 4);
            if (objects.size() != countp || countp != 4) {
                /* Delete objects and file at end of test. */
                for (int i = 0; i < 20; i++)
                    conn.DeleteObject(
                      firstBucket, "test_list_objects_" + std::to_string(i) + ".txt");
                std::remove("test_list_objects.txt");
                return (1);
            }
            std::cout << "List with total objects < max per iter:" << std::endl;
            for (const auto &object : objects)
                std::cout << "  * " << object << std::endl;

            /* List with total objects non-divisable by max per iter. */
            objects =
              conn.ListObjects(firstBucket, "test_list_objects_", countp, maxPerIter, 8);
            if (objects.size() != countp || countp != 8) {
                /* Delete objects and file at end of test. */
                for (int i = 0; i < 20; i++)
                    conn.DeleteObject(
                      firstBucket, "test_list_objects_" + std::to_string(i) + ".txt");
                std::remove("test_list_objects.txt");
                return (1);
            }
            std::cout << "List with total objects non-divisable by max per iter:" << std::endl;
            for (const auto &object : objects)
                std::cout << "  * " << object << std::endl;

            /* List with max objects greater than total objects. */
            objects = conn.ListObjects(firstBucket, "test_list_objects_", countp, 5, 30);
            if (objects.size() != countp || countp != 20) {
                /* Delete objects and file at end of test. */
                for (int i = 0; i < 20; i++)
                    conn.DeleteObject(
                      firstBucket, "test_list_objects_" + std::to_string(i) + ".txt");
                std::remove("test_list_objects.txt");
                return (1);
            }
            std::cout << "List all:" << std::endl;
            for (const auto &object : objects)
                std::cout << "  * " << object << std::endl;

            /* Delete objects and file at end of test. */
            for (int i = 0; i < 20; i++)
                conn.DeleteObject(firstBucket, "test_list_objects_" + std::to_string(i) + ".txt");
            std::remove("test_list_objects.txt");
            return (0);
        } else {
            std::cout << "No buckets in AWS account." << std::endl;
            return (0);
        }
    } else {
        std::cerr << "Error listing buckets." << std::endl;
        return (1);
    }
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
