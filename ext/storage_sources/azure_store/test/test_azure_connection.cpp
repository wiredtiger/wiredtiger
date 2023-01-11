
#include <catch2/catch.hpp>

#include "azure_connection.h"


// Default config settings for the test environment
namespace TestDefaults {
const double throughputTargetGbps = 5;
const uint64_t partSize = 8 * 1024 * 1024;  // 8 MB.
static std::string bucketName("azuretestext"); // Can be overridden with environment variables.;


static std::string objPrefix("azuretest/unit/"); // To be concatenated with a random string.
} // namespace TestDefaults



static int
setupTestDefaults() {
    return (0);
}

TEST_CASE("Testubg class ")
{}




int
main (int argc, char **argb) {
    return (0);
}