#include <AwsBucketConn.h>
#include <aws/core/Aws.h>
#include <iostream>

/* Default config settings for the S3CrtClient. */
namespace defaults 
{
    const Aws::String region = Aws::Region::US_EAST_1;
    const double throughput_target_gbps = 5;
    const uint64_t part_size = 8 * 1024 * 1024; // 8 MB.    
}

/* Unit test for listing S3 buckets under the associated AWS account. */
int test_list_buckets(const Aws::S3Crt::ClientConfiguration &config) {
    awsBucketConn bucket_conn(config);
    std::cout << "Listing all S3 buckets under the AWS account:" << std::endl;
    bool outcome = bucket_conn.s3_list_buckets();
    std::cout << "Finished listing all S3 buckets under the AWS account." << std::endl;
    return outcome;
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
