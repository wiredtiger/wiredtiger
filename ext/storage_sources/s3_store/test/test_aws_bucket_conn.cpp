#include <aws_bucket_conn.h>
#include <aws/core/Aws.h>

#include <iostream>

/* Default config settings for the S3CrtClient. */
namespace test_defaults 
{
    const Aws::String region = Aws::Region::AP_SOUTHEAST_2;
    const double throughput_target_gbps = 5;
    const uint64_t part_size = 8 * 1024 * 1024; /* 8 MB. */
}

int test_list_buckets(const Aws::S3Crt::ClientConfiguration &config);

/* Wrapper for unit test functions. */
#define TEST(func, config, expected_output)                   \
    do {                                                      \
        int __ret;                                            \
        if ((__ret = (func(config))) != expected_output)      \
            return (__ret);                                   \
    } while (0)

/*
 * test_list_buckets --
 *      Example unit test for listing S3 buckets under the associated AWS account.
 *      aws_bucket_conn does not require the list_buckets API functionality and it 
 *      along with this test eventually will be removed.
 */
int 
test_list_buckets(const Aws::S3Crt::ClientConfiguration &config) {
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
 * main --
 *     Set up configs and call unit tests.
 */
int 
main () {
    /* Set up the config to use the defaults specified. */
    Aws::S3Crt::ClientConfiguration aws_config;
    aws_config.region = test_defaults::region;
    aws_config.throughputTargetGbps = test_defaults::throughput_target_gbps;
    aws_config.partSize = test_defaults::part_size;

    /* Set the SDK options and initialize the API. */
    Aws::SDKOptions options;
    Aws::InitAPI(options);

    TEST(test_list_buckets, aws_config, 0);

    /* Shutdown the API at end of tests. */
    Aws::ShutdownAPI(options);
    return 0;
}
