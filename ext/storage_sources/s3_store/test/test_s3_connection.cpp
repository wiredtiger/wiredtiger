#include <s3_connection.h>
#include <random>

/* Default config settings for the S3CrtClient. */
namespace TestDefaults {
const Aws::String region = Aws::Region::AP_SOUTHEAST_2;
const double throughputTargetGbps = 5;
const uint64_t partSize = 8 * 1024 * 1024; /* 8 MB. */
} // namespace TestDefaults

int TestListBuckets(const Aws::S3Crt::ClientConfiguration &config);

#define TEST_BUCKET "s3testext"

/* Wrapper for unit test functions. */
#define TEST(func, config, expectedOutput)              \
    do {                                                \
        int __ret;                                      \
        if ((__ret = (func(config))) != expectedOutput) \
            return (__ret);                             \
    } while (0)

/*
 * generate_unique_prefix --
 *     Generates a unique prefix to be used with the object keys, eg:
 *     "s3test_artefacts/unit_2022-31-01-16-34-10_623843294/"
 */
static int
generate_unique_prefix(std::string &prefix)
{
    char time_str[100];
    std::time_t t = std::time(nullptr);

    if (std::strftime(time_str, sizeof(time_str), "%F-%H-%M-%S", std::localtime(&t)) == 0)
        return 1;

    prefix = "s3test_artefacts/unit_";
    prefix += time_str;

    // Create a random device and use it to generate a random seed to initialize the generator.
    std::random_device myRandomDevice;
    unsigned seed = myRandomDevice();
    std::default_random_engine myRandomEngine(seed);

    prefix += '_' + std::to_string(myRandomEngine());
    prefix += '/';

    return 0;
}

/*
 * TestListBuckets --
 *     Example of a unit test to list S3 buckets under the associated AWS account.
 */
int
TestListBuckets(const Aws::S3Crt::ClientConfiguration &config)
{
    S3Connection conn(config, TEST_BUCKET);
    std::vector<std::string> buckets;
    if (!conn.ListBuckets(buckets))
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

    /* Silence the compiler for now. */
    std::string prefix;
    if (generate_unique_prefix(prefix) != 0)
        return 1;
    std::cout << "Generated prefix: " << prefix << std::endl;

    /* Shutdown the API at end of tests. */
    Aws::ShutdownAPI(options);
    return 0;
}
