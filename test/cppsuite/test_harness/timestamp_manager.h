#ifndef TIMESTAMP_MANAGER_H
#define TIMESTAMP_MANAGER_H

#include "component.h"

namespace test_harness {
/*
 * The timestamp monitor class managers global timestamp state for all components in the test
 * harness. It also manages the global timestamps within WiredTiger.
 */
class timestamp_manager : public component  {
    public:
    void
    run(){
        while (_running) {
            /* Do something. */
        }
    }
};
}

#endif
