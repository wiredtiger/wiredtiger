#include <s3_connection.h>
#include <fstream>

/* Default config settings for the S3CrtClient. */
namespace TestDefaults {
const Aws::String region = Aws::Region::AP_SOUTHEAST_2;
const double throughputTargetGbps = 5;
const uint64_t partSize = 8 * 1024 * 1024; /* 8 MB. */
} // namespace TestDefaults

int TestListBuckets(const Aws::S3Crt::ClientConfiguration &config);
int TestGetObject(const Aws::S3Crt::ClientConfiguration &config);

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
 * TestGetObject --
 *     Unit test to get an object from an S3 Bucket.
 */
int
TestGetObject(const Aws::S3Crt::ClientConfiguration &config) {
    S3Connection conn(config);
    std::vector<std::string> buckets;

    if (!conn.ListBuckets(buckets))
        return (1);
    const std::string bucketName = buckets.at(0);
    const std::string objectName = "permanent_object.txt";
    const std::string path = "./" + objectName;

    int ret = conn.GetObject(bucketName, objectName, path);

    if (ret != 0){
        std::cout << "TestGetObject: call to S3Connection:GetObject has failed." << std::endl;
        return (1);
    }

    /* The file should now be in the current directory. */
    std::ifstream f(path);
    if (!f.good()) {
        std::cout << "TestGetObject: target " << objectName << " has not been succesfully downloaded." << std::endl;
        return (1);
    }
    /* Clean up test artefacts. */
    if (std::remove(path.c_str()) != 0)
        return (1);

    std::cout << "TestGetObject succeded." << std::endl;
    return (0);
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

    int getObjectExpectedOutput = 0;
    TEST(TestGetObject, awsConfig, getObjectExpectedOutput);
    /* Shutdown the API at end of tests. */
    Aws::ShutdownAPI(options);
    return 0;
}
