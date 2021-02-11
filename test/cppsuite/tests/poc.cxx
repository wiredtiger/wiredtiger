#include <iostream>
#include <cstdlib>
#include "test_harness/test_harness.h"
#include "test_harness/test_workload.h"

class poc_test : public test_harness::test {
    public:
    int
    run()
    {
        WT_CONNECTION *conn;
        int ret = 0;
        /* Setup basic test directory. */
        const char *default_dir = "WT_TEST";

        /*
         * Csuite tests utilise a test_util.h command to make their directory, currently that
         * doesn't compile under c++ and some extra work will be needed to make it work. Its unclear
         * if the test framework will use test_util.h yet.
         */
        const char *mkdir_cmd = "mkdir WT_TEST";
        ret = system(mkdir_cmd);
        if (ret != 0)
            return (ret);

        ret = wiredtiger_open(default_dir, NULL, "create,cache_size=1G", &conn);
        return (ret);
    }

    poc_test(const char *config) : test(config) {}
};

const char *poc_test::test::_name = "poc_test";

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
        std::cout << "Setting configuration to default one" << std::endl;
        // Set the default config
        cfg = "collection_count=1,key_size=5";
    }
    std::cout << "Configuration is: " << cfg << std::endl;

    // Test name
    const char *name = "wl_test";

    test_workload::workload wl(name, cfg.c_str());

    return 0;
}
