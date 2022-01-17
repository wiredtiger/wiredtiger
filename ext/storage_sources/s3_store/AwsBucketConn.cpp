#include <aws/core/Aws.h>
#include <aws/s3-crt/S3CrtClient.h>
#include <iostream>


class AwsBucketConn{

    private:
    Aws::S3Crt::ClientConfiguration aws_config;
    Aws::S3Crt::S3CrtClient s3_crt_client;
    Aws::String region = Aws::Region::US_EAST_1;
    const double throughput_target_gbps = 5;
    const uint64_t part_size = 8 * 1024 * 1024; // 8 MB.
    



    public: 
    void ListBuckets(){

    }
};


int main(){
    AwsBucketConn conn1;
}