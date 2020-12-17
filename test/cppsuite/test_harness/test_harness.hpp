#include <iostream>
#include <cstdlib>

extern "C" {
    #include "wiredtiger.h"
}

namespace TestHarness {
    class Test {
        public:
        /*
         * All tests will implement this initially, the return value from it will indicate whether
         * the test was successful or not.
         */
        virtual int Run();
    };
}
