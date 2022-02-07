#include <s3_connection.h>
#include <fstream>
#include <random>

/* Default config settings for the Test environment. */
namespace TestDefaults {
const Aws::String region = Aws::Region::AP_SOUTHEAST_2;
const double throughputTargetGbps = 5;
const uint64_t partSize = 8 * 1024 * 1024; /* 8 MB. */
static std::string testBucket("s3testext"); // Can be overridden with environment variables.
static std::string testPrefix("s3test_artefacts/unit_"); // To be concatenated with a random string.
} // namespace TestDefaults

#define TEST_SUCCESS 0
#define TEST_FAILURE 1

int TestListBuckets(const Aws::S3Crt::ClientConfiguration &config);
int TestObjectExists(const Aws::S3Crt::ClientConfiguration &config);

/* Wrapper for unit test functions. */
#define TEST(func, config, expectedOutput)              \
    do {                                                \
        int __ret;                                      \
        if ((__ret = (func(config))) != expectedOutput) \
            return (__ret);                             \
    } while (0)

/*
 * randomizeTestPrefix --
 *     Concatenates a random suffix to the prefix being used for the test object keys.
 *     Example of generated test prefix:
 *     "s3test_artefacts/unit_" 2022-31-01-16-34-10_623843294/"
 */
static int
randomizeTestPrefix()
{
    char timeStr[100];
    std::time_t t = std::time(nullptr);

    if (std::strftime(timeStr, sizeof(timeStr), "%F-%H-%M-%S", std::localtime(&t)) == 0)
        return (TEST_FAILURE);

    TestDefaults::testPrefix += timeStr;

    /* Create a random device and use it to generate a random seed to initialize the generator. */
    std::random_device myRandomDevice;
    unsigned seed = myRandomDevice();
    std::default_random_engine myRandomEngine(seed);

    TestDefaults::testPrefix += '_' + std::to_string(myRandomEngine());
    TestDefaults::testPrefix += '/';

    return (TEST_SUCCESS);
}

/*
 * setupTestDefaults --
 *     Override the defaults with the ones specific for this test instance.
 */
static int
setupTestDefaults()
{
    /* Prefer to use the bucket provided through the environment variable. */
    const char* envBucket = std::getenv("WT_S3_EXT_BUCKET");
    if (envBucket != NULL)
        TestDefaults::testBucket = envBucket;
    std::cout << "Bucket to be used for testing: " << TestDefaults::testBucket << std::endl;

    /* Append the prefix to be used for object names by a unique string. */
    if (randomizeTestPrefix() != 0)
        return (TEST_FAILURE);
    std::cout << "Generated prefix: " << TestDefaults::testPrefix << std::endl;

    return (TEST_SUCCESS);
}

/*
 * TestObjectExists --
 *     Unit test to check if an object exists in an AWS bucket.
 */
int
TestObjectExists(const Aws::S3Crt::ClientConfiguration &config)
{
    S3Connection conn(config, TestDefaults::testBucket, TestDefaults::testPrefix);
    bool exists = false;
    int ret = TEST_FAILURE;

    const std::string objectName = "test_object";
    const std::string fileName = "test_object.txt";

    /* Create a file to upload to the bucket.*/
    std::ofstream File(fileName);
    File << "Test payload";
    File.close();

    if ((ret = conn.ObjectExists(objectName, exists)) != 0 || exists)
        return (ret);

    if ((ret = conn.PutObject(objectName, fileName)) != 0)
        return (ret);

    if ((ret = conn.ObjectExists(objectName, exists)) != 0 || !exists)
        return (ret);

    if ((ret = conn.DeleteObject(objectName)) != 0)
        return (ret);

    std::cout << "TestObjectExists(): succeeded.\n" << std::endl;

    return (ret);
}

/*
 * main --
 *     Set up configs and call unit tests.
 */
int
main()
{
    /* Setup the test environment. */
    if (setupTestDefaults() != 0)
        return (TEST_FAILURE);

    /* Set up the config to use the defaults specified. */
    Aws::S3Crt::ClientConfiguration awsConfig;
    awsConfig.region = TestDefaults::region;
    awsConfig.throughputTargetGbps = TestDefaults::throughputTargetGbps;
    awsConfig.partSize = TestDefaults::partSize;

    /* Set the SDK options and initialize the API. */
    Aws::SDKOptions options;
    Aws::InitAPI(options);

    int objectExistsExpectedOutput = TEST_SUCCESS;
    TEST(TestObjectExists, awsConfig, objectExistsExpectedOutput);

    /* Shutdown the API at end of tests. */
    Aws::ShutdownAPI(options);
    return (TEST_SUCCESS);
}
