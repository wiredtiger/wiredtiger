#include <aws_bucket_conn.h>

#include <fstream>

/* Default config settings for the S3CrtClient. */
namespace test_defaults {
const Aws::String region = Aws::Region::AP_SOUTHEAST_2;
const double throughput_target_gbps = 5;
const uint64_t part_size = 8 * 1024 * 1024; /* 8 MB. */
} // namespace test_defaults

int test_list_buckets(const Aws::S3Crt::ClientConfiguration &config);
int test_list_objects(const Aws::S3Crt::ClientConfiguration &config);

/* Wrapper for unit test functions. */
#define TEST(func, config, expected_output)              \
    do {                                                 \
        int __ret;                                       \
        if ((__ret = (func(config))) != expected_output) \
            return (__ret);                              \
    } while (0)

/*
 * test_list_buckets --
 *     Example of a unit test to list S3 buckets under the associated AWS account.
 */
int
test_list_buckets(const Aws::S3Crt::ClientConfiguration &config)
{
    aws_bucket_conn conn(config);
    std::vector<std::string> buckets;
    if (!conn.list_buckets(buckets))
        return (1);

    std::cout << "All buckets under my account:" << std::endl;
    for (const auto &bucket : buckets)
        std::cout << "  * " << bucket << std::endl;
    return (0);
}

/*
 * test_list_objects --
 *     Unit test for listing S3 objects under the first bucket in the associated AWS account. This
 *     test assumes there are initially no objects with the prefix of "test_list_objects_" in the
 *     bucket.
 */
int
test_list_objects(const Aws::S3Crt::ClientConfiguration &config)
{
    aws_bucket_conn conn(config);

    /* Temporary workaround to get a bucket to use. */
    std::string first_bucket;
    std::vector<std::string> buckets;
    if (conn.list_buckets(buckets)) {
        if (!buckets.empty()) {
            first_bucket = buckets.at(0);

            std::vector<std::string> objects;
            uint32_t countp;

            /* No matching objects. */
            objects = conn.list_objects(first_bucket, "test_list_objects_", countp);
            if (objects.size() != countp || countp != 0)
                return (1);

            /* Create file to prepare for test. */
            if (!static_cast<bool>(std::ofstream("test_list_objects.txt").put('.'))) {
                std::cerr << "Error creating file." << std::endl;
                return (1);
            }

            /* Put objects to prepare for test. */
            for (int i = 0; i < 20; i++)
                conn.put_object(first_bucket, "test_list_objects_" + std::to_string(i) + ".txt",
                  "test_list_objects.txt");

            /* List all objects with prefix. */
            objects = conn.list_objects(first_bucket, "test_list_objects_", countp);
            if (objects.size() != countp || countp != 20) {
                /* Delete objects and file at end of test. */
                for (int i = 0; i < 20; i++)
                    conn.delete_object(
                      first_bucket, "test_list_objects_" + std::to_string(i) + ".txt");
                std::remove("test_list_objects.txt");
                return (1);
            }
            std::cout << "Objects with test_list_objects_ prefix:" << std::endl;
            for (const auto &object : objects)
                std::cout << "  * " << object << std::endl;

            objects = conn.list_objects(first_bucket, "test_list_objects_1", countp);
            if (objects.size() != countp || countp != 11) {
                /* Delete objects and file at end of test. */
                for (int i = 0; i < 20; i++)
                    conn.delete_object(
                      first_bucket, "test_list_objects_" + std::to_string(i) + ".txt");
                std::remove("test_list_objects.txt");
                return (1);
            }
            std::cout << "Objects with test_list_objects_1 prefix:" << std::endl;
            for (const auto &object : objects)
                std::cout << "  * " << object << std::endl;

            /* List with max objects of 5. */
            objects = conn.list_objects(first_bucket, "test_list_objects_", countp, 5);
            if (objects.size() != countp || countp != 5) {
                /* Delete objects and file at end of test. */
                for (int i = 0; i < 20; i++)
                    conn.delete_object(
                      first_bucket, "test_list_objects_" + std::to_string(i) + ".txt");
                std::remove("test_list_objects.txt");
                return (1);
            }
            std::cout << "List 5 objects:" << std::endl;
            for (const auto &object : objects)
                std::cout << "  * " << object << std::endl;

            /* List with max objects greater than total objects. */
            objects = conn.list_objects(first_bucket, "test_list_objects_", countp, 30);
            if (objects.size() != countp || countp != 20) {
                /* Delete objects and file at end of test. */
                for (int i = 0; i < 20; i++)
                    conn.delete_object(
                      first_bucket, "test_list_objects_" + std::to_string(i) + ".txt");
                std::remove("test_list_objects.txt");
                return (1);
            }
            std::cout << "List all:" << std::endl;
            for (const auto &object : objects)
                std::cout << "  * " << object << std::endl;

            /* Delete objects and file at end of test. */
            for (int i = 0; i < 20; i++)
                conn.delete_object(first_bucket, "test_list_objects_" + std::to_string(i) + ".txt");
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
    Aws::S3Crt::ClientConfiguration aws_config;
    aws_config.region = test_defaults::region;
    aws_config.throughputTargetGbps = test_defaults::throughput_target_gbps;
    aws_config.partSize = test_defaults::part_size;

    /* Set the SDK options and initialize the API. */
    Aws::SDKOptions options;
    Aws::InitAPI(options);

    int expected_output = 0;
    TEST(test_list_buckets, aws_config, expected_output);
    TEST(test_list_objects, aws_config, expected_output);

    /* Shutdown the API at end of tests. */
    Aws::ShutdownAPI(options);
    return (0);
}
