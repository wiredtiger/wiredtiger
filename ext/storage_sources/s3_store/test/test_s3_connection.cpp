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
    int ret = 0;
    S3Connection conn(config);
    std::vector<std::string> buckets;
    if (ret = conn.ListBuckets(buckets) != 0)
        goto err;

    std::cout << "All buckets under my account:" << std::endl;
    for (const auto &bucket : buckets)
        std::cout << "  * " << bucket << std::endl;

err:
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
    int ret = 0;
    S3Connection conn(config);

    /* Temporary workaround to get a bucket to use. */
    std::string firstBucket;
    std::vector<std::string> buckets;

    if (ret = conn.ListBuckets(buckets) != 0)
        goto err;
    if (!buckets.empty()) {
        firstBucket = buckets.at(0);

        std::vector<std::string> objects;
        uint32_t countp;

        /* No matching objects. */
        if (ret = conn.ListObjects(firstBucket, "test_list_objects_", objects, countp) != 0)
            goto err;
        if (objects.size() != countp || countp != 0) {
            ret = 1;
            goto err;
        }

        /* Create file to prepare for test. */
        if (!static_cast<bool>(std::ofstream("test_list_objects.txt").put('.'))) {
            std::cerr << "Error creating file." << std::endl;
            ret = 1;
            goto err;
        }

        /* Put objects to prepare for test. */
        for (int i = 0; i < 20; i++) {
            if (ret = conn.PutObject(firstBucket, "test_list_objects_" + std::to_string(i) + ".txt",
                        "test_list_objects.txt") != 0)
                goto err;
        }

        /* List all objects with prefix. */
        objects.clear();
        if (ret = conn.ListObjects(firstBucket, "test_list_objects_", objects, countp) != 0)
            goto err;
        if (objects.size() != countp || countp != 20) {
            /* Delete objects and file at end of test. */
            for (int i = 0; i < 20; i++) {
                if (ret = conn.DeleteObject(
                            firstBucket, "test_list_objects_" + std::to_string(i) + ".txt") != 0)
                    goto err;
            }
            std::remove("test_list_objects.txt");
            ret = 1;
            goto err;
        }
        std::cout << "Objects with test_list_objects_ prefix:" << std::endl;
        for (const auto &object : objects)
            std::cout << "  * " << object << std::endl;

        objects.clear();
        if (ret = conn.ListObjects(firstBucket, "test_list_objects_1", objects, countp) != 0)
            goto err;
        if (objects.size() != countp || countp != 11) {
            /* Delete objects and file at end of test. */
            for (int i = 0; i < 20; i++) {
                if (ret = conn.DeleteObject(
                            firstBucket, "test_list_objects_" + std::to_string(i) + ".txt") != 0)
                    goto err;
            }
            std::remove("test_list_objects.txt");
            ret = 1;
            goto err;
        }
        std::cout << "Objects with test_list_objects_1 prefix:" << std::endl;
        for (const auto &object : objects)
            std::cout << "  * " << object << std::endl;

        objects.clear();
        /* List with max objects of 1. */
        if (ret = conn.ListObjects(firstBucket, "test_list_objects_", objects, countp, 1, 1) != 0)
            goto err;
        if (objects.size() != countp || countp != 1) {
            /* Delete objects and file at end of test. */
            for (int i = 0; i < 20; i++) {
                if (ret = conn.DeleteObject(
                            firstBucket, "test_list_objects_" + std::to_string(i) + ".txt") != 0)
                    goto err;
            }
            std::remove("test_list_objects.txt");
            ret = 1;
            goto err;
        }
        std::cout << "List 1 object:" << std::endl;
        for (const auto &object : objects)
            std::cout << "  * " << object << std::endl;

        /* List with 5 objects per AWS request. */
        const int32_t maxPerIter = 5;
        objects.clear();
        if (ret =
              conn.ListObjects(firstBucket, "test_list_objects_", objects, countp, maxPerIter) != 0)
            goto err;
        if (objects.size() != countp || countp != 20) {
            /* Delete objects and file at end of test. */
            for (int i = 0; i < 20; i++) {
                if (ret = conn.DeleteObject(
                            firstBucket, "test_list_objects_" + std::to_string(i) + ".txt") != 0)
                    goto err;
            }
            std::remove("test_list_objects.txt");
            ret = 1;
            goto err;
        }
        std::cout << "List all:" << std::endl;
        for (const auto &object : objects)
            std::cout << "  * " << object << std::endl;

        /* List with total_objects < max_per_iter. */
        objects.clear();
        if (ret = conn.ListObjects(
                    firstBucket, "test_list_objects_", objects, countp, maxPerIter, 4) != 0)
            goto err;
        if (objects.size() != countp || countp != 4) {
            /* Delete objects and file at end of test. */
            for (int i = 0; i < 20; i++) {
                if (ret = conn.DeleteObject(
                            firstBucket, "test_list_objects_" + std::to_string(i) + ".txt") != 0)
                    goto err;
            }
            std::remove("test_list_objects.txt");
            ret = 1;
            goto err;
        }
        std::cout << "List with total objects < max per iter:" << std::endl;
        for (const auto &object : objects)
            std::cout << "  * " << object << std::endl;

        /* List with total objects non-divisable by max per iter. */
        objects.clear();
        if (ret = conn.ListObjects(
                    firstBucket, "test_list_objects_", objects, countp, maxPerIter, 8) != 0)
            goto err;
        if (objects.size() != countp || countp != 8) {
            /* Delete objects and file at end of test. */
            for (int i = 0; i < 20; i++) {
                if (ret = conn.DeleteObject(
                            firstBucket, "test_list_objects_" + std::to_string(i) + ".txt") != 0)
                    goto err;
            }
            std::remove("test_list_objects.txt");
            ret = 1;
            goto err;
        }
        std::cout << "List with total objects non-divisable by max per iter:" << std::endl;
        for (const auto &object : objects)
            std::cout << "  * " << object << std::endl;

        /* List with max objects greater than total objects. */
        objects.clear();
        if (ret = conn.ListObjects(firstBucket, "test_list_objects_", objects, countp, 5, 30) != 0)
            goto err;
        if (objects.size() != countp || countp != 20) {
            /* Delete objects and file at end of test. */
            for (int i = 0; i < 20; i++) {
                if (ret = conn.DeleteObject(
                            firstBucket, "test_list_objects_" + std::to_string(i) + ".txt") != 0)
                    goto err;
            }
            std::remove("test_list_objects.txt");
            ret = 1;
            goto err;
        }
        std::cout << "List all:" << std::endl;
        for (const auto &object : objects)
            std::cout << "  * " << object << std::endl;

        /* Delete objects and file at end of test. */
        for (int i = 0; i < 20; i++) {
            if (ret = conn.DeleteObject(
                        firstBucket, "test_list_objects_" + std::to_string(i) + ".txt") != 0)
                goto err;
        }
        std::remove("test_list_objects.txt");
    } else {
        std::cout << "No buckets in AWS account." << std::endl;
    }
err:
    return (ret);
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
