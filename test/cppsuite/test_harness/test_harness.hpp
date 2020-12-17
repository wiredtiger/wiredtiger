#include <iostream>
#include <cstdlib>

extern "C" {
    #include "wiredtiger.h"
}

namespace TestHarness {
    class Test {
        public:
        virtual int Run();
    };
}
