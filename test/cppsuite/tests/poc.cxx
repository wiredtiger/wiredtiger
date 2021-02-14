#include <iostream>
#include <cstdlib>
#include "test_harness/test_harness.h"
#include "test_harness/test_workload.h"

class poc_test : public test_harness::test {

    public:

    int
    run()
    {
        test_workload::workload wl(_configuration);
        if (wl.load() != 0) {
        }
        else if (wl.run() != 0) {
        }

    }

    poc_test(const char *config) : test(config) {}
};

const char *poc_test::test::_name = "poc_test";
const char *poc_test::test::_default_config = "collection_count=2,key_count=5";

int
main(int argc, char *argv[])
{
    // Configuration
    std::string cfg = "";

    // Parse args
    for (int i = 1; i < argc; ++i) {
        std::cout << "Arg is " << argv[i] << std::endl;
        if (strcmp(argv[i], "-F") == 0) {
            if (i + 1 < argc) {
                std::cout << "There is a value for " << argv[i++] << std::endl;
                std::cout << "The value is " << argv[i] << std::endl;
                cfg = argv[i];
            } else {
                std::cout << "No value given for option " << argv[i] << std::endl;
            }
        }
    }

    // Check if default configuration should be used
    if (cfg.compare("") == 0) {
        std::cout << "Setting configuration to default" << std::endl;
        cfg = poc_test::test::_default_config;
    }

    return poc_test(cfg.c_str()).run();

}
