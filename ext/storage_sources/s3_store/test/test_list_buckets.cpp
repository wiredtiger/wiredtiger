#include <aws_bucket_conn.h>
#include <aws/core/Aws.h>
#include <iostream>

/* Default config settings for the S3CrtClient. */
namespace defaults 
{
    const Aws::String region = Aws::Region::AP_SOUTHEAST_2;
    const double throughput_target_gbps = 5;
    const uint64_t part_size = 8 * 1024 * 1024; // 8 MB.    
}

/* Unit test for listing S3 buckets under the associated AWS account. */
int test_list_buckets(const Aws::S3Crt::ClientConfiguration &config) {
    aws_bucket_conn conn(config);
    std::vector<std::string> buckets;

    if (conn.list_buckets(buckets)) {
        std::cout << "All buckets under my account:" << std::endl;
        for (const std::string &bucket : buckets) {
            std::cout << "  * " << bucket << std::endl;
        }
        std::cout << std::endl;
        return true;
    } else {
        return false;
    }
}

int main () {
    /* Set up the config to use the defaults specified. */
    Aws::S3Crt::ClientConfiguration aws_config;
    aws_config.region = defaults::region;
    aws_config.throughputTargetGbps = defaults::throughput_target_gbps;
    aws_config.partSize = defaults::part_size;

    /* Set the SDK options and initialize the API. */
    Aws::SDKOptions options;
    Aws::InitAPI(options);

    int ret = 0;

    ret = !test_list_buckets(aws_config);

    return (ret);
}
