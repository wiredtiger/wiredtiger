#define CATCH_CONFIG_RUNNER
#include <catch2/catch.hpp>

#include "utils.h"

int
main(int argc, char **argv)
{
    // clean up after any previous failed/crashed test runs
    utils::wiredtigerCleanup();

    return Catch::Session().run(argc, argv);
}
