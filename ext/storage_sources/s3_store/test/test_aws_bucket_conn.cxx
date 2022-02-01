#include <aws_bucket_conn.h>
#include <fstream>

/* Default config settings for the S3CrtClient. */
namespace test_defaults {
const Aws::String region = Aws::Region::AP_SOUTHEAST_2;
const double throughput_target_gbps = 5;
const uint64_t part_size = 8 * 1024 * 1024; /* 8 MB. */
} // namespace test_defaults

int test_list_buckets(const Aws::S3Crt::ClientConfiguration &config);
int test_object_exists(const Aws::S3Crt::ClientConfiguration &config);

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
        return 1;

    std::cout << "All buckets under my account:" << std::endl;
    for (const auto &bucket : buckets)
        std::cout << "  * " << bucket << std::endl;
    return 0;
}

/*
 * test_object_exists --
 *     Unit test to check if an object exists in an AWS bucket.
 */
int
test_object_exists(const Aws::S3Crt::ClientConfiguration &config)
{
    aws_bucket_conn conn(config);
    std::vector<std::string> buckets;
    if (!conn.list_buckets(buckets))
        return 1;
    const std::string bucket_name = buckets.at(0);
    const std::string object_name = "test_object";
    const std::string file_name = "test_object.txt";

    /* Create a file to upload to the bucket.*/
    std::ofstream File(file_name);
    File << "Test payload";
    File.close();

    bool exists = false;

    int result = conn.object_exists(bucket_name, object_name, exists);
    if (result == 0) {
        std::cout << "test_object_exists(): FAILURE - test_object already exists in the bucket"
                  << std::endl;
        return 1;
    }

    conn.put_object(bucket_name, object_name, file_name);
    result = conn.object_exists(bucket_name, object_name, exists);
    if (result != 0) {
        std::cout << "test_object_exists(): FAILURE - test object does not exist after put_object."
                  << std::endl;
        return 1;
    }

    conn.delete_object(bucket_name, object_name);
    std::cout << "test_object_exists(): succeeded.\n" << std::endl;
    return 0;
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

    int object_exists_expected_output = 0;
    TEST(test_object_exists, aws_config, object_exists_expected_output);

    /* Shutdown the API at end of tests. */
    Aws::ShutdownAPI(options);
    return 0;
}
